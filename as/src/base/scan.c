/*
 * scan.c
 *
 * Copyright (C) 2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see http://www.gnu.org/licenses/
 */

//==============================================================================
// Includes.
//

#include "base/scan.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

#include "aerospike/as_module.h"
#include "aerospike/as_stream.h"
#include "aerospike/as_string.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_ll.h"
#include "citrusleaf/cf_vector.h"

#include "dynbuf.h"
#include "fault.h"

#include "base/aggr.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/job_manager.h"
#include "base/monitor.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "base/udf_memtracker.h"



//==============================================================================
// Typedefs and forward declarations.
//

//----------------------------------------------------------
// Scan types.
//

typedef enum {
	SCAN_TYPE_BASIC		= 0,
	SCAN_TYPE_AGGR		= 1,
	SCAN_TYPE_UDF_BG	= 2,

	SCAN_TYPE_UNKNOWN	= -1
} scan_type;

static inline const char*
scan_type_str(scan_type type)
{
	switch (type) {
	case SCAN_TYPE_BASIC:
		return "basic";
	case SCAN_TYPE_AGGR:
		return "aggregation";
	case SCAN_TYPE_UDF_BG:
		return "background-udf";
	default:
		return "?";
	}
}

//----------------------------------------------------------
// scan_job - derived classes' public methods.
//

int basic_scan_job_start(as_transaction* tr, as_namespace* ns, uint16_t set_id);
int aggr_scan_job_start(as_transaction* tr, as_namespace* ns, uint16_t set_id);
int udf_bg_scan_job_start(as_transaction* tr, as_namespace* ns, uint16_t set_id);

//----------------------------------------------------------
// Non-class-specific utilities.
//

typedef struct scan_options_s {
	int			priority;
	bool		fail_on_cluster_change;
	uint32_t	sample_pct;
} scan_options;

int get_scan_ns(as_msg* m, as_namespace** p_ns);
int get_scan_set_id(as_msg* m, as_namespace* ns, uint16_t* p_set_id);
scan_type get_scan_type(as_transaction* tr);
bool get_scan_options(as_msg* m, scan_options* options);
void set_blocking(int fd, bool block);
size_t send_blocking_response_chunk(int fd, uint8_t* buf, size_t size);
size_t send_blocking_response_fin(int fd, int result_code);
static inline bool excluded_set(as_index* r, uint16_t set_id);



//==============================================================================
// Constants.
//

const size_t INIT_BUF_BUILDER_SIZE = 1024 * 1024 * 2;
const size_t SCAN_CHUNK_LIMIT = 1024 * 1024;



//==============================================================================
// Globals.
//

static as_job_manager g_scan_manager;



//==============================================================================
// Public API.
//

void
as_scan_init()
{
	as_job_manager_init(&g_scan_manager, g_config.scan_max_active,
			g_config.scan_max_done, g_config.scan_threads);
}

int
as_scan(as_transaction *tr)
{
	int result;
	as_namespace* ns = NULL;
	uint16_t set_id = INVALID_SET_ID;

	if ((result = get_scan_ns(&tr->msgp->msg, &ns)) != 0 ||
		(result = get_scan_set_id(&tr->msgp->msg, ns, &set_id)) != 0) {
		cf_free(tr->msgp);
		return result;
	}

	switch (get_scan_type(tr)) {
	case SCAN_TYPE_BASIC:
		result = basic_scan_job_start(tr, ns, set_id);
		break;
	case SCAN_TYPE_AGGR:
		result = aggr_scan_job_start(tr, ns, set_id);
		break;
	case SCAN_TYPE_UDF_BG:
		result = udf_bg_scan_job_start(tr, ns, set_id);
		break;
	default:
		cf_warning(AS_SCAN, "can't identify scan type");
		result = AS_PROTO_RESULT_FAIL_PARAMETER;
		break;
	}

	if (result != AS_PROTO_RESULT_OK) {
		cf_free(tr->msgp);
	}

	return result;
}

void
as_scan_limit_active_jobs(uint32_t max_active)
{
	as_job_manager_limit_active_jobs(&g_scan_manager, max_active);
}

void
as_scan_limit_finished_jobs(uint32_t max_done)
{
	as_job_manager_limit_finished_jobs(&g_scan_manager, max_done);
}

void
as_scan_resize_thread_pool(uint32_t n_threads)
{
	as_job_manager_resize_thread_pool(&g_scan_manager, n_threads);
}

int
as_scan_get_active_job_count()
{
	return as_job_manager_get_active_job_count(&g_scan_manager);
}

int
as_scan_list(char* name, cf_dyn_buf* db)
{
	as_mon_info_cmd(AS_MON_MODULES[SCAN_MOD], NULL, 0, 0, db);
	return 0;
}

as_mon_jobstat*
as_scan_get_jobstat(uint64_t trid)
{
	return as_job_manager_get_job_info(&g_scan_manager, trid);
}

as_mon_jobstat*
as_scan_get_jobstat_all(int* size)
{
	return as_job_manager_get_info(&g_scan_manager, size);
}

int
as_scan_abort(uint64_t trid)
{
	return as_job_manager_abort_job(&g_scan_manager, trid) ? 0 : -1;
}

int
as_scan_abort_all()
{
	return as_job_manager_abort_all_jobs(&g_scan_manager);
}

int
as_scan_change_job_priority(uint64_t trid, uint32_t priority)
{
	return as_job_manager_change_job_priority(&g_scan_manager, trid,
			(int)priority) ? 0 : -1;
}


//==============================================================================
// Non-class-specific utilities.
//

int
get_scan_ns(as_msg* m, as_namespace** p_ns)
{
	as_msg_field *f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_NAMESPACE);

	if (! f) {
		cf_warning(AS_SCAN, "scan msg has no namespace");
		return AS_PROTO_RESULT_FAIL_NAMESPACE;
	}

	as_namespace* ns = as_namespace_get_bymsgfield(f);

	if (! ns) {
		cf_warning(AS_SCAN, "scan msg has unrecognized namespace");
		return AS_PROTO_RESULT_FAIL_NAMESPACE;
	}

	*p_ns = ns;

	return AS_PROTO_RESULT_OK;
}

int
get_scan_set_id(as_msg* m, as_namespace* ns, uint16_t* p_set_id)
{
	uint16_t set_id = INVALID_SET_ID;
	as_msg_field *f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET);

	if (f && as_msg_field_get_value_sz(f) != 0) {
		uint32_t set_name_len = as_msg_field_get_value_sz(f);
		char set_name[set_name_len + 1];

		memcpy(set_name, f->data, set_name_len);
		set_name[set_name_len] = '\0';
		set_id = as_namespace_get_set_id(ns, set_name);

		if (set_id == INVALID_SET_ID) {
			cf_warning(AS_SCAN, "scan msg has unrecognized set %s", set_name);
			return AS_PROTO_RESULT_FAIL_NOTFOUND;
		}
	}

	*p_set_id = set_id;

	return AS_PROTO_RESULT_OK;
}

scan_type
get_scan_type(as_transaction* tr)
{
	as_msg_field *filename_f = as_msg_field_get(&tr->msgp->msg,
			AS_MSG_FIELD_TYPE_UDF_FILENAME);

	if (! filename_f && ! tr->udata.req_udata) {
		return SCAN_TYPE_BASIC;
	}

	as_msg_field *udf_op_f = as_msg_field_get(&tr->msgp->msg,
			AS_MSG_FIELD_TYPE_UDF_OP);

	if (udf_op_f && *udf_op_f->data == (uint8_t)AS_SCAN_UDF_OP_AGGREGATE) {
		return SCAN_TYPE_AGGR;
	}

	if (tr->udata.req_udata || (udf_op_f &&
			*udf_op_f->data == (uint8_t)AS_SCAN_UDF_OP_BACKGROUND)) {
		return SCAN_TYPE_UDF_BG;
	}

	return SCAN_TYPE_UNKNOWN;
}

bool
get_scan_options(as_msg* m, scan_options* options)
{
	as_msg_field *f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_SCAN_OPTIONS);

	if (f) {
		if (as_msg_field_get_value_sz(f) != 2) {
			cf_warning(AS_SCAN, "scan msg options field size not 2");
			return false;
		}

		options->priority = AS_MSG_FIELD_SCAN_PRIORITY(f->data[0]);
		options->fail_on_cluster_change =
				(AS_MSG_FIELD_SCAN_FAIL_ON_CLUSTER_CHANGE & f->data[0]) != 0;
		options->sample_pct = f->data[1];
	}

	return true;
}

void
set_blocking(int fd, bool block)
{
	int flags = fcntl(fd, F_GETFL, 0);

	if (flags < 0) {
		cf_crash(AS_SCAN, "error getting flags on fd %d (%d: %s)", fd, errno,
				cf_strerror(errno));
	}

	flags = block ? flags & ~O_NONBLOCK : flags | O_NONBLOCK;

	if (fcntl(fd, F_SETFL, flags) < 0) {
		cf_crash(AS_SCAN, "error setting flags on fd %d (%d: %s)", fd, errno,
				cf_strerror(errno));
	}
}

size_t
send_blocking_response_chunk(int fd, uint8_t* buf, size_t size)
{
	as_proto proto;

	proto.version = PROTO_VERSION;
	proto.type = PROTO_TYPE_AS_MSG;
	proto.sz = size;
	as_proto_swap(&proto);

	int rv = send(fd, (uint8_t*)&proto, sizeof(as_proto),
			MSG_NOSIGNAL | MSG_MORE);

	if (rv != sizeof(as_proto)) {
		cf_warning(AS_SCAN, "send error - fd %d rv %d %s", fd, rv,
				rv < 0 ? cf_strerror(errno) : "");
		return 0;
	}

	if ((rv = send(fd, buf, size, MSG_NOSIGNAL)) != size) {
		cf_warning(AS_SCAN, "send error - fd %d sz %lu rv %d %s", fd, size, rv,
				rv < 0 ? cf_strerror(errno) : "");
		return 0;
	}

	return sizeof(as_proto) + size;
}

size_t
send_blocking_response_fin(int fd, int result_code)
{
	cl_msg m;

	m.proto.version = PROTO_VERSION;
	m.proto.type = PROTO_TYPE_AS_MSG;
	m.proto.sz = sizeof(as_msg);
	as_proto_swap(&m.proto);

	m.msg.header_sz = sizeof(as_msg);
	m.msg.info1 = 0;
	m.msg.info2 = 0;
	m.msg.info3 = AS_MSG_INFO3_LAST;
	m.msg.unused = 0;
	m.msg.result_code = result_code;
	m.msg.generation = 0;
	m.msg.record_ttl = 0;
	m.msg.transaction_ttl = 0;
	m.msg.n_fields = 0;
	m.msg.n_ops = 0;
	as_msg_swap_header(&m.msg);

	int rv = send(fd, (uint8_t*)&m, sizeof(cl_msg), MSG_NOSIGNAL);

	if (rv != sizeof(cl_msg)) {
		cf_warning(AS_SCAN, "send error - fd %d rv %d %s", fd, rv,
				rv < 0 ? cf_strerror(errno) : "");
		return 0;
	}

	return sizeof(cl_msg);
}

static inline bool
excluded_set(as_index* r, uint16_t set_id)
{
	return set_id != INVALID_SET_ID && set_id != as_index_get_set_id(r);
}



//==============================================================================
// conn_scan_job derived class implementation - not final class.
//

//----------------------------------------------------------
// conn_scan_job typedefs and forward declarations.
//

typedef struct conn_scan_job_s {
	// Base object must be first:
	as_job			_base;

	// Derived class data:
	pthread_mutex_t	fd_lock;
	as_file_handle*	fd_h;

	uint64_t		net_io_bytes;
} conn_scan_job;

void conn_scan_job_own_fd(conn_scan_job* job, as_file_handle* fd_h);
void conn_scan_job_disown_fd(conn_scan_job* job);
void conn_scan_job_finish(conn_scan_job* job);
bool conn_scan_job_send_response(conn_scan_job* job, uint8_t* buf, size_t size);
void conn_scan_job_release_fd(conn_scan_job* job);
void conn_scan_job_info(conn_scan_job* job, as_mon_jobstat* stat);

//----------------------------------------------------------
// conn_scan_job API.
//

void
conn_scan_job_own_fd(conn_scan_job* job, as_file_handle* fd_h)
{
	pthread_mutex_init(&job->fd_lock, NULL);

	job->fd_h = fd_h;
	job->fd_h->fh_info |= FH_INFO_DONOT_REAP;
	set_blocking(job->fd_h->fd, true);

	job->net_io_bytes = 0;
}

void
conn_scan_job_disown_fd(conn_scan_job* job)
{
	// Just undo conn_scan_job_own_fd(), nothing more.

	set_blocking(job->fd_h->fd, false);
	job->fd_h->fh_info &= ~FH_INFO_DONOT_REAP;

	pthread_mutex_destroy(&job->fd_lock);
}

void
conn_scan_job_finish(conn_scan_job* job)
{
	as_job* _job = (as_job*)job;

	if (job->fd_h) {
		job->net_io_bytes += send_blocking_response_fin(job->fd_h->fd,
				_job->abandoned);
		conn_scan_job_release_fd(job);
	}

	pthread_mutex_destroy(&job->fd_lock);
}

bool
conn_scan_job_send_response(conn_scan_job* job, uint8_t* buf, size_t size)
{
	as_job* _job = (as_job*)job;

	pthread_mutex_lock(&job->fd_lock);

	if (! job->fd_h) {
		pthread_mutex_unlock(&job->fd_lock);
		// Job already abandoned.
		return false;
	}

	size_t size_sent = send_blocking_response_chunk(job->fd_h->fd, buf, size);

	if (size_sent == 0) {
		conn_scan_job_release_fd(job);
		pthread_mutex_unlock(&job->fd_lock);
		as_job_manager_abandon_job(_job->mgr, _job,
				AS_PROTO_RESULT_FAIL_UNKNOWN);
		return false;
	}

	job->net_io_bytes += size_sent;

	pthread_mutex_unlock(&job->fd_lock);
	return true;
}

void
conn_scan_job_release_fd(conn_scan_job* job)
{
	set_blocking(job->fd_h->fd, false);
	job->fd_h->fh_info &= ~FH_INFO_DONOT_REAP;
	job->fd_h->last_used = cf_getms();
	AS_RELEASE_FILE_HANDLE(job->fd_h);
	job->fd_h = NULL;
}

void
conn_scan_job_info(conn_scan_job* job, as_mon_jobstat* stat)
{
	stat->net_io_bytes = job->net_io_bytes;
}



//==============================================================================
// basic_scan_job derived class implementation.
//

//----------------------------------------------------------
// basic_scan_job typedefs and forward declarations.
//

typedef struct basic_scan_job_s {
	// Base object must be first:
	conn_scan_job	_base;

	// Derived class data:
	uint64_t		cluster_key;
	bool			fail_on_cluster_change;
	bool			no_bin_data;
	uint32_t		sample_pct;
	cf_vector*		bin_names;
} basic_scan_job;

void basic_scan_job_slice(as_job* _job, as_partition_reservation* rsv);
void basic_scan_job_finish(as_job* _job);
void basic_scan_job_destroy(as_job* _job);
void basic_scan_job_info(as_job* _job, as_mon_jobstat* stat);

const as_job_vtable basic_scan_job_vtable = {
		basic_scan_job_slice,
		basic_scan_job_finish,
		basic_scan_job_destroy,
		basic_scan_job_info
};

typedef struct basic_scan_slice_s {
	basic_scan_job*		job;
	cf_buf_builder**	bb_r;
} basic_scan_slice;

void basic_scan_job_reduce_cb(as_index_ref* r_ref, void* udata);
cf_vector* bin_names_from_op(as_msg* m, int* result);

//----------------------------------------------------------
// basic_scan_job public API.
//

int
basic_scan_job_start(as_transaction* tr, as_namespace* ns, uint16_t set_id)
{
	basic_scan_job* job = cf_malloc(sizeof(basic_scan_job));
	as_job* _job = (as_job*)job;

	if (! job) {
		cf_warning(AS_SCAN, "basic scan job failed alloc");
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	scan_options options = { 0, false, 100 };

	if (! get_scan_options(&tr->msgp->msg, &options)) {
		cf_free(job);
		return AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	as_job_init(_job, &basic_scan_job_vtable, &g_scan_manager, RSV_WRITE,
			tr->trid, ns, set_id, options.priority);

	int result;

	job->cluster_key = as_paxos_get_cluster_key();
	job->fail_on_cluster_change = options.fail_on_cluster_change;
	job->no_bin_data = (tr->msgp->msg.info1 & AS_MSG_INFO1_GET_NOBINDATA) != 0;
	job->sample_pct = options.sample_pct;
	job->bin_names = bin_names_from_op(&tr->msgp->msg, &result);

	if (! job->bin_names && result != AS_PROTO_RESULT_OK) {
		as_job_destroy(_job);
		return result;
	}

	// TODO - detect migrations more rigorously!
	if (job->fail_on_cluster_change &&
			cf_atomic_int_get(g_config.migrate_progress_send) != 0) {
		// TODO - was AS_PROTO_RESULT_FAIL_UNAVAILABLE - ok?
		cf_warning(AS_SCAN, "basic scan job not started - migration");
		as_job_destroy(_job);
		return AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH;
	}

	// Take ownership of socket from transaction.
	conn_scan_job_own_fd((conn_scan_job*)job, tr->proto_fd_h);

	cf_info(AS_SCAN, "starting basic scan job %lu {%s:%s} priority %u, sample-pct %u%s%s",
			_job->trid, ns->name, as_namespace_get_set_name(ns, set_id),
			_job->priority, job->sample_pct,
			job->no_bin_data ? ", metadata-only" : "",
			job->fail_on_cluster_change ? ", fail-on-cluster-change" : "");

	if ((result = as_job_manager_start_job(_job->mgr, _job)) != 0) {
		cf_warning(AS_SCAN, "basic scan job %lu failed to start (%d)",
				_job->trid, result);
		conn_scan_job_disown_fd((conn_scan_job*)job);
		as_job_destroy(_job);
		return result;
	}

	// Normal scans don't need anything in the message beyond here.
	cf_free(tr->msgp);

	return AS_PROTO_RESULT_OK;
}

//----------------------------------------------------------
// basic_scan_job mandatory scan_job interface.
//

void
basic_scan_job_slice(as_job* _job, as_partition_reservation* rsv)
{
	basic_scan_job* job = (basic_scan_job*)_job;
	as_index_tree* tree = rsv->p->vp;
	cf_buf_builder* bb = cf_buf_builder_create_size(INIT_BUF_BUILDER_SIZE);

	if (! bb) {
		as_job_manager_abandon_job(_job->mgr, _job,
				AS_PROTO_RESULT_FAIL_UNKNOWN);
		return;
	}

	basic_scan_slice slice = { job, &bb };

	if (job->sample_pct == 100) {
		as_index_reduce(tree, basic_scan_job_reduce_cb, (void*)&slice);
	}
	else {
		uint32_t sample_count = (uint32_t)
				(((uint64_t)tree->elements * (uint64_t)job->sample_pct) / 100);

		as_index_reduce_partial(tree, sample_count, basic_scan_job_reduce_cb,
				(void*)&slice);
	}

	if (bb->used_sz != 0) {
		conn_scan_job_send_response((conn_scan_job*)job, bb->buf, bb->used_sz);
	}

	// TODO - guts don't check buf_builder realloc failures rigorously.
	cf_buf_builder_free(bb);
}

void
basic_scan_job_finish(as_job* _job)
{
	conn_scan_job_finish((conn_scan_job*)_job);

	cf_atomic_int_incr(_job->abandoned == 0 ?
			&g_config.basic_scans_succeeded : &g_config.basic_scans_failed);

	cf_info(AS_SCAN, "finished basic scan job %lu (%d)", _job->trid,
			_job->abandoned);
}

void
basic_scan_job_destroy(as_job* _job)
{
	basic_scan_job* job = (basic_scan_job*)_job;

	if (job->bin_names) {
		cf_vector_destroy(job->bin_names);
	}
}

void
basic_scan_job_info(as_job* _job, as_mon_jobstat* stat)
{
	strcpy(stat->job_type, scan_type_str(SCAN_TYPE_BASIC));
	conn_scan_job_info((conn_scan_job*)_job, stat);
}

//----------------------------------------------------------
// basic_scan_job utilities.
//

void
basic_scan_job_reduce_cb(as_index_ref* r_ref, void* udata)
{
	basic_scan_slice* slice = (basic_scan_slice*)udata;
	basic_scan_job* job = slice->job;
	as_job* _job = (as_job*)job;
	as_namespace* ns = _job->ns;

	if (_job->abandoned != 0) {
		as_record_done(r_ref, ns);
		return;
	}

	if (job->fail_on_cluster_change &&
			job->cluster_key != as_paxos_get_cluster_key()) {
		as_record_done(r_ref, ns);
		as_job_manager_abandon_job(_job->mgr, _job,
				AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH);
		return;
	}

	as_index *r = r_ref->r;

	if (excluded_set(r, _job->set_id) || as_record_is_expired(r)) {
		as_record_done(r_ref, ns);
		return;
	}

	if (job->no_bin_data) {
		if (as_index_is_flag_set(r, AS_INDEX_FLAG_KEY_STORED)) {
			as_storage_rd rd;

			as_storage_record_open(ns, r, &rd, &r->key);
			as_msg_make_response_bufbuilder(r, &rd, slice->bb_r, true, NULL,
					true, true, true, NULL);
			as_storage_record_close(r, &rd);
		}
		else {
			as_msg_make_response_bufbuilder(r, NULL, slice->bb_r, true,
					ns->name, true, false, true, NULL);
		}
	}
	else {
		as_storage_rd rd;

		as_storage_record_open(ns, r, &rd, &r->key);
		rd.n_bins = as_bin_get_n_bins(r, &rd);

		as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];

		rd.bins = as_bin_get_all(r, &rd, stack_bins);
		as_msg_make_response_bufbuilder(r, &rd, slice->bb_r, false, NULL, true,
				true, true, job->bin_names);
		as_storage_record_close(r, &rd);
	}

	as_record_done(r_ref, ns);

	cf_atomic64_incr(&_job->n_records_read);

	cf_buf_builder* bb = *slice->bb_r;

	// If we exceed the proto size limit, send accumulated data back to client
	// and reset the buf-builder to start a new proto.
	if (bb->used_sz > SCAN_CHUNK_LIMIT) {
		if (! conn_scan_job_send_response((conn_scan_job*)job, bb->buf,
				bb->used_sz)) {
			return;
		}

		cf_buf_builder_reset(bb);
	}
}

cf_vector*
bin_names_from_op(as_msg* m, int* result)
{
	if (m->n_ops == 0) {
		*result = AS_PROTO_RESULT_OK;
		return NULL;
	}

	cf_vector *v  = cf_vector_create(AS_ID_BIN_SZ, m->n_ops, 0);

	as_msg_op *op = NULL;
	int n = 0;

	while ((op = as_msg_op_iterate(m, op, &n)) != NULL) {
		if (op->name_sz >= AS_ID_BIN_SZ) {
			cf_warning(AS_SCAN, "basic scan job bin name too long");
			*result = AS_PROTO_RESULT_FAIL_BIN_NAME;
			cf_vector_destroy(v);
			return NULL;
		}

		char bin_name[AS_ID_BIN_SZ];

		memcpy(bin_name, op->name, op->name_sz);
		bin_name[op->name_sz] = 0;
		cf_vector_append_unique(v, (void *)bin_name);
	}

	return v;
}



//==============================================================================
// aggr_scan_job derived class implementation.
//

//----------------------------------------------------------
// aggr_scan_job typedefs and forward declarations.
//

typedef struct aggr_scan_job_s {
	// Base object must be first:
	conn_scan_job	_base;

	// Derived class data:
	cl_msg*			msgp;
	as_aggr_call	aggr_call;
} aggr_scan_job;

void aggr_scan_job_slice(as_job* _job, as_partition_reservation* rsv);
void aggr_scan_job_finish(as_job* _job);
void aggr_scan_job_destroy(as_job* _job);
void aggr_scan_job_info(as_job* _job, as_mon_jobstat* stat);

const as_job_vtable aggr_scan_job_vtable = {
		aggr_scan_job_slice,
		aggr_scan_job_finish,
		aggr_scan_job_destroy,
		aggr_scan_job_info
};

typedef struct aggr_scan_slice_s {
	aggr_scan_job*		job;
	cf_ll*				ll;
	cf_buf_builder**	bb_r;
} aggr_scan_slice;

void aggr_scan_job_reduce_cb(as_index_ref* r_ref, void* udata);
bool aggr_scan_add_digest(cf_ll* ll, cf_digest* keyd);
void aggr_scan_set_error(void* caller);
bool aggr_scan_mem_op(mem_tracker* mt, uint32_t num_bytes, memtracker_op op);
as_aggr_caller_type aggr_scan_get_type();
as_stream_status aggr_scan_ostream_write(const as_stream* s, as_val* v);

const as_aggr_caller_intf aggr_scan_caller_intf = {
		.set_error	= aggr_scan_set_error,
		.mem_op		= aggr_scan_mem_op,
		.get_type	= aggr_scan_get_type
};

const as_stream_hooks aggr_scan_istream_hooks = {
		.destroy	= NULL,
		.read		= as_aggr_istream_read,
		.write		= NULL
};

const as_stream_hooks aggr_scan_ostream_hooks = {
		.destroy	= NULL,
		.read		= NULL,
		.write		= aggr_scan_ostream_write
};

void aggr_scan_add_val_response(aggr_scan_slice* slice, const as_val* val, bool success);

//----------------------------------------------------------
// aggr_scan_job public API.
//

int
aggr_scan_job_start(as_transaction* tr, as_namespace* ns, uint16_t set_id)
{
	aggr_scan_job* job = cf_malloc(sizeof(aggr_scan_job));
	as_job* _job = (as_job*)job;

	if (! job) {
		cf_warning(AS_SCAN, "aggregation scan job failed alloc");
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	scan_options options = { 0, false, 100 };

	if (! get_scan_options(&tr->msgp->msg, &options)) {
		cf_free(job);
		return AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	as_job_init(_job, &aggr_scan_job_vtable, &g_scan_manager, RSV_WRITE,
			tr->trid, ns, set_id, options.priority);

	job->msgp = tr->msgp;

	if (as_aggr_call_init(&job->aggr_call, tr, job, &aggr_scan_caller_intf,
			&aggr_scan_istream_hooks, &aggr_scan_ostream_hooks, ns,
			true) != 0) {
		cf_warning(AS_SCAN, "aggregation scan job failed call init");
		job->msgp = NULL;
		as_job_destroy(_job);
		return AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	// Take ownership of socket from transaction.
	conn_scan_job_own_fd((conn_scan_job*)job, tr->proto_fd_h);

	cf_info(AS_SCAN, "starting aggregation scan job %lu {%s:%s} priority %u",
			_job->trid, ns->name, as_namespace_get_set_name(ns, set_id),
			_job->priority);

	int result = as_job_manager_start_job(_job->mgr, _job);

	if (result != 0) {
		cf_warning(AS_SCAN, "aggregation scan job %lu failed to start (%d)",
				_job->trid, result);
		conn_scan_job_disown_fd((conn_scan_job*)job);
		job->msgp = NULL;
		as_job_destroy(_job);
		return result;
	}

	return AS_PROTO_RESULT_OK;
}

//----------------------------------------------------------
// aggr_scan_job mandatory scan_job interface.
//

void
aggr_scan_job_slice(as_job* _job, as_partition_reservation* rsv)
{
	aggr_scan_job* job = (aggr_scan_job*)_job;
	as_index_tree* tree = rsv->p->vp;
	cf_ll ll;
	cf_buf_builder* bb = cf_buf_builder_create_size(INIT_BUF_BUILDER_SIZE);

	if (! bb) {
		as_job_manager_abandon_job(_job->mgr, _job,
				AS_PROTO_RESULT_FAIL_UNKNOWN);
		return;
	}

	cf_ll_init(&ll, as_index_keys_ll_destroy_fn, false);

	aggr_scan_slice slice = { job, &ll, &bb };

	as_index_reduce(tree, aggr_scan_job_reduce_cb, (void*)&slice);

	if (cf_ll_size(&ll) != 0) {
		as_result* res = as_result_new();
		int ret = as_aggr_process(&job->aggr_call, &ll, (void*)&slice, res);

		if (ret != 0) {
			char* rs = as_module_err_string(ret);

			if (res->value) {
				as_string* lua_s = as_string_fromval(res->value);
				char* lua_err = (char*)as_string_tostring(lua_s);

				if (lua_err) {
					int l_rs_len = strlen(rs);

					rs = cf_realloc(rs, l_rs_len + strlen(lua_err) + 4);
					sprintf(&rs[l_rs_len], " : %s", lua_err);
				}
			}

			const as_val* v = (as_val*)as_string_new(rs, false);

			aggr_scan_add_val_response(&slice, v, false);
			as_val_destroy(v);
			cf_free(rs);
			as_job_manager_abandon_job(_job->mgr, _job,
					AS_PROTO_RESULT_FAIL_UNKNOWN);
		}
	}

	cf_ll_reduce(&ll, true, as_index_keys_ll_reduce_fn, NULL);

	if (bb->used_sz != 0) {
		conn_scan_job_send_response((conn_scan_job*)job, bb->buf, bb->used_sz);
	}

	// TODO - guts don't check buf_builder realloc failures rigorously.
	cf_buf_builder_free(bb);
}

void
aggr_scan_job_finish(as_job* _job)
{
	aggr_scan_job* job = (aggr_scan_job*)_job;

	conn_scan_job_finish((conn_scan_job*)job);

	cf_free(job->msgp);
	job->msgp = NULL;

	cf_atomic_int_incr(_job->abandoned == 0 ?
			&g_config.aggr_scans_succeeded : &g_config.aggr_scans_failed);

	cf_info(AS_SCAN, "finished aggregation scan job %lu (%d)", _job->trid,
			_job->abandoned);
}

void
aggr_scan_job_destroy(as_job* _job)
{
	aggr_scan_job* job = (aggr_scan_job*)_job;

	if (job->msgp) {
		cf_free(job->msgp);
	}
}

void
aggr_scan_job_info(as_job* _job, as_mon_jobstat* stat)
{
	strcpy(stat->job_type, scan_type_str(SCAN_TYPE_AGGR));
	conn_scan_job_info((conn_scan_job*)_job, stat);
}

//----------------------------------------------------------
// aggr_scan_job utilities.
//

void
aggr_scan_job_reduce_cb(as_index_ref* r_ref, void* udata)
{
	aggr_scan_slice* slice = (aggr_scan_slice*)udata;
	aggr_scan_job* job = slice->job;
	as_job* _job = (as_job*)job;
	as_namespace* ns = _job->ns;

	if (_job->abandoned != 0) {
		as_record_done(r_ref, ns);
		return;
	}

	as_index* r = r_ref->r;

	if (excluded_set(r, _job->set_id) || as_record_is_expired(r)) {
		as_record_done(r_ref, ns);
		return;
	}

	if (! aggr_scan_add_digest(slice->ll, &r->key)) {
		as_record_done(r_ref, ns);
		as_job_manager_abandon_job(_job->mgr, _job,
				AS_PROTO_RESULT_FAIL_UNKNOWN);
		return;
	}

	cf_atomic64_incr(&_job->n_records_read);
	as_record_done(r_ref, ns);
}

bool
aggr_scan_add_digest(cf_ll* ll, cf_digest* keyd)
{
	as_index_keys_ll_element* tail_e = (as_index_keys_ll_element*)ll->tail;
	as_index_keys_arr* keys_arr;

	if (tail_e) {
		keys_arr = tail_e->keys_arr;

		if (keys_arr->num == AS_INDEX_KEYS_PER_ARR) {
			tail_e = NULL;
		}
	}

	if (! tail_e) {
		if (! (keys_arr = as_index_get_keys_arr())) {
			return false;
		}

		if (! (tail_e = cf_malloc(sizeof(as_index_keys_ll_element)))) {
			return false;
		}

		tail_e->keys_arr = keys_arr;
		cf_ll_append(ll, (cf_ll_element *)tail_e);
	}

	keys_arr->pindex_digs[keys_arr->num] = *keyd;
	keys_arr->num++;

	return true;
}

void
aggr_scan_set_error(void* caller)
{
}

bool
aggr_scan_mem_op(mem_tracker* mt, uint32_t num_bytes, memtracker_op op)
{
	return mt && mt->udata;
}

as_aggr_caller_type
aggr_scan_get_type()
{
	return AS_AGGR_SCAN;
}

as_stream_status
aggr_scan_ostream_write(const as_stream* s, as_val* v)
{
	aggr_scan_slice* slice = (aggr_scan_slice*)as_stream_source(s);

	if (v) {
		aggr_scan_add_val_response(slice, v, true);
		as_val_destroy(v);
	}

	return AS_STREAM_OK;
}

void
aggr_scan_add_val_response(aggr_scan_slice* slice, const as_val* val,
		bool success)
{
	uint32_t size = 0;

	as_val_tobuf(val, NULL, &size); // sizing only
	as_msg_make_val_response_bufbuilder(val, slice->bb_r, size, success);

	cf_buf_builder* bb = *slice->bb_r;
	conn_scan_job* conn_job = (conn_scan_job*)slice->job;

	// If we exceed the proto size limit, send accumulated data back to client
	// and reset the buf-builder to start a new proto.
	if (bb->used_sz > SCAN_CHUNK_LIMIT) {
		if (! conn_scan_job_send_response(conn_job, bb->buf, bb->used_sz)) {
			return;
		}

		cf_buf_builder_reset(bb);
	}
}



//==============================================================================
// udf_bg_scan_job derived class implementation.
//

//----------------------------------------------------------
// udf_bg_scan_job typedefs and forward declarations.
//

typedef struct udf_bg_scan_job_s {
	// Base object must be first:
	as_job			_base;

	// Derived class data:
	cl_msg*			msgp;
	udf_call		call;
	cf_atomic32		n_active_tr;

	cf_atomic64		n_successful_tr;
	cf_atomic64		n_failed_tr;
} udf_bg_scan_job;

void udf_bg_scan_job_slice(as_job* _job, as_partition_reservation* rsv);
void udf_bg_scan_job_finish(as_job* _job);
void udf_bg_scan_job_destroy(as_job* _job);
void udf_bg_scan_job_info(as_job* _job, as_mon_jobstat* stat);

const as_job_vtable udf_bg_scan_job_vtable = {
		udf_bg_scan_job_slice,
		udf_bg_scan_job_finish,
		udf_bg_scan_job_destroy,
		udf_bg_scan_job_info
};

void udf_bg_scan_job_reduce_cb(as_index_ref* r_ref, void* udata);
int udf_bg_scan_tr_complete(as_transaction *tr, int retcode);

//----------------------------------------------------------
// as_scan public API for udf_bg_scan_job.
//

udf_call*
as_scan_get_udf_call(void* req_udata)
{
	udf_bg_scan_job* job = (udf_bg_scan_job*)req_udata;

	return &job->call;
}

//----------------------------------------------------------
// udf_bg_scan_job public API.
//

int
udf_bg_scan_job_start(as_transaction* tr, as_namespace* ns, uint16_t set_id)
{
	udf_bg_scan_job* job = cf_malloc(sizeof(udf_bg_scan_job));
	as_job* _job = (as_job*)job;

	if (! job) {
		cf_warning(AS_SCAN, "udf-bg scan job failed alloc");
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	scan_options options = { 0, false, 100 };

	if (! get_scan_options(&tr->msgp->msg, &options)) {
		cf_free(job);
		return AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	as_job_init(_job, &udf_bg_scan_job_vtable, &g_scan_manager, RSV_WRITE,
			tr->trid, ns, set_id, options.priority);

	job->msgp = tr->msgp;
	job->n_active_tr = 0;
	job->n_successful_tr = 0;
	job->n_failed_tr = 0;

	if (udf_call_init(&job->call, tr) != 0) {
		cf_warning(AS_SCAN, "udf-bg scan job failed call init");
		job->msgp = NULL;
		as_job_destroy(_job);
		return AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	cf_info(AS_SCAN, "starting udf-bg scan job %lu {%s:%s} priority %u",
			_job->trid, ns->name, as_namespace_get_set_name(ns, set_id),
			_job->priority);

	int result = as_job_manager_start_job(_job->mgr, _job);

	if (result != 0) {
		cf_warning(AS_SCAN, "udf-bg scan job %lu failed to start (%d)",
				_job->trid, result);
		job->msgp = NULL;
		as_job_destroy(_job);
		return result;
	}

	if (as_msg_send_fin(tr->proto_fd_h->fd, AS_PROTO_RESULT_OK) != 0) {
		cf_warning(AS_SCAN, "udf-bg scan job error sending fin");
		// Nothing more we can do here.
	}

	tr->proto_fd_h->last_used = cf_getms();
	AS_RELEASE_FILE_HANDLE(tr->proto_fd_h);
	tr->proto_fd_h = NULL;

	return AS_PROTO_RESULT_OK;
}

//----------------------------------------------------------
// udf_bg_scan_job mandatory scan_job interface.
//

void
udf_bg_scan_job_slice(as_job* _job, as_partition_reservation* rsv)
{
	as_index_reduce(rsv->p->vp, udf_bg_scan_job_reduce_cb, (void*)_job);
}

void
udf_bg_scan_job_finish(as_job* _job)
{
	udf_bg_scan_job* job = (udf_bg_scan_job*)_job;

	while (cf_atomic32_get(job->n_active_tr) != 0) {
		usleep(100);
	}

	cf_free(job->msgp);
	job->msgp = NULL;

	cf_atomic_int_incr(_job->abandoned == 0 ?
			&g_config.udf_bg_scans_succeeded : &g_config.udf_bg_scans_failed);

	cf_info(AS_SCAN, "finished udf-bg scan job %lu (%d)", _job->trid,
			_job->abandoned);
}

void
udf_bg_scan_job_destroy(as_job* _job)
{
	udf_bg_scan_job* job = (udf_bg_scan_job*)_job;

	if (job->msgp) {
		cf_free(job->msgp);
	}
}

void
udf_bg_scan_job_info(as_job* _job, as_mon_jobstat* stat)
{
	strcpy(stat->job_type, scan_type_str(SCAN_TYPE_UDF_BG));
	stat->net_io_bytes = sizeof(cl_msg); // size of original synchronous fin

	udf_bg_scan_job* job = (udf_bg_scan_job*)_job;
	char *extra = stat->jdata + strlen(stat->jdata);

	sprintf(extra, ":udf-filename=%s:udf-function=%s:udf-active=%u:udf-success=%lu:udf-failed=%lu",
			job->call.filename, job->call.function,
			cf_atomic32_get(job->n_active_tr),
			cf_atomic64_get(job->n_successful_tr),
			cf_atomic64_get(job->n_failed_tr));
}

//----------------------------------------------------------
// udf_bg_scan_job utilities.
//

void
udf_bg_scan_job_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_job* _job = (as_job*)udata;
	udf_bg_scan_job* job = (udf_bg_scan_job*)_job;
	as_namespace* ns = _job->ns;

	if (_job->abandoned != 0) {
		as_record_done(r_ref, ns);
		return;
	}

	as_index* r = r_ref->r;

	if (excluded_set(r, _job->set_id) || as_record_is_expired(r)) {
		as_record_done(r_ref, ns);
		return;
	}

	tr_create_data d;

	d.digest	= r->key;
	d.ns		= ns;
	d.set[0]	= 0;				// TODO - transfer set name ???
	d.call		= &job->call;
	d.msg_type	= AS_MSG_INFO2_WRITE;
	d.fd_h		= NULL;
	d.trid		= 0;				// TODO - transfer trid ???
	d.udata		= NULL;				// TODO - unused - why is it there ???

	// Release record lock before enqueuing transaction.
	as_record_done(r_ref, ns);

	// TODO - replace this mechanism with signal-based counter?
	while (cf_atomic32_get(job->n_active_tr) >
			g_config.scan_max_udf_transactions) {
		usleep(50);
	}

	as_transaction tr;

	if (as_transaction_create(&tr, &d) != 0) {
		as_job_manager_abandon_job(_job->mgr, _job, AS_JOB_FAIL_UNKNOWN);
		return;
	}

	tr.udata.req_cb		= udf_bg_scan_tr_complete;
	tr.udata.req_udata	= (void*)job;
	tr.udata.req_type	= UDF_SCAN_REQUEST;

	// TODO - should these be done in as_transaction_create() ???
	tr.flag |= AS_TRANSACTION_FLAG_INTERNAL;
	tr.microbenchmark_is_resolve = false;

	cf_atomic64_incr(&_job->n_records_read);
	cf_atomic32_incr(&job->n_active_tr);

	thr_tsvc_enqueue(&tr);
}

int
udf_bg_scan_tr_complete(as_transaction *tr, int retcode)
{
	udf_bg_scan_job* job = (udf_bg_scan_job*)tr->udata.req_udata;

	cf_atomic32_decr(&job->n_active_tr);
	cf_atomic64_incr(retcode == 0 ? &job->n_successful_tr : &job->n_failed_tr);

	return 0;
}
