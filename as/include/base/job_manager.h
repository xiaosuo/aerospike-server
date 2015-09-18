/*
 * job_manager.h
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

#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_queue_priority.h"

#include "base/datamodel.h"
#include "base/monitor.h"

//----------------------------------------------------------
// as_priority_thread_pool - class header.
// TODO - move to common.
//

typedef struct as_priority_thread_pool_s {
	pthread_mutex_t		lock;
	cf_queue_priority*	dispatch_queue;
	cf_queue*			complete_queue;
	uint32_t			n_threads;
} as_priority_thread_pool;

typedef void (*as_priority_thread_pool_task_fn)(void* task);

// Same as cf_queue_priority scheme, so no internal conversion needed:
#define THREAD_POOL_PRIORITY_LOW	CF_QUEUE_PRIORITY_LOW
#define THREAD_POOL_PRIORITY_MEDIUM	CF_QUEUE_PRIORITY_MEDIUM
#define THREAD_POOL_PRIORITY_HIGH	CF_QUEUE_PRIORITY_HIGH

bool as_priority_thread_pool_init(as_priority_thread_pool* pool, uint32_t n_threads);
void as_priority_thread_pool_shutdown(as_priority_thread_pool* pool);
bool as_priority_thread_pool_resize(as_priority_thread_pool* pool, uint32_t n_threads);
bool as_priority_thread_pool_queue_task(as_priority_thread_pool* pool, as_priority_thread_pool_task_fn task_fn, void* task, int priority);
bool as_priority_thread_pool_remove_task(as_priority_thread_pool* pool, void* task);
void as_priority_thread_pool_change_task_priority(as_priority_thread_pool* pool, void* task, int new_priority);

//----------------------------------------------------------
// as_job - base class header.
//

struct as_job_s;
typedef void (*as_job_slice_fn)(struct as_job_s* _job, as_partition_reservation* rsv);
typedef void (*as_job_finish_fn)(struct as_job_s* _job);
typedef void (*as_job_destroy_fn)(struct as_job_s* _job);
typedef void (*as_job_info_fn)(struct as_job_s* _job, as_mon_jobstat* stat);

typedef struct as_job_vtable_s {
	as_job_slice_fn		slice_fn;
	as_job_finish_fn	finish_fn;
	as_job_destroy_fn	destroy_fn;
	as_job_info_fn		info_mon_fn;
} as_job_vtable;

struct as_job_manager_s;

typedef enum {
	RSV_WRITE	= 0,
	RSV_MIGRATE	= 1
} as_job_rsv_type;

// Same as cf_queue_priority scheme, so no internal conversion needed:
#define AS_JOB_PRIORITY_LOW		THREAD_POOL_PRIORITY_LOW
#define AS_JOB_PRIORITY_MEDIUM	THREAD_POOL_PRIORITY_MEDIUM
#define AS_JOB_PRIORITY_HIGH	THREAD_POOL_PRIORITY_HIGH

// Same as proto result codes so connected scans don't have to convert:
#define AS_JOB_FAIL_UNKNOWN		AS_PROTO_RESULT_FAIL_UNKNOWN
#define AS_JOB_FAIL_PARAMETER	AS_PROTO_RESULT_FAIL_PARAMETER
#define AS_JOB_FAIL_CLUSTER_KEY	AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH
#define AS_JOB_FAIL_USER_ABORT	AS_PROTO_RESULT_FAIL_SCAN_ABORT
#define AS_JOB_FAIL_FORBIDDEN	AS_PROTO_RESULT_FAIL_FORBIDDEN

typedef struct as_job_s {
	// Mandatory interface for derived classes:
	as_job_vtable				vtable;

	// Parent:
	struct as_job_manager_s*	mgr;

	// Which partitions to reduce:
	as_job_rsv_type				rsv_type;

	// Unique identifier:
	uint64_t					trid;

	// Job scope:
	as_namespace*				ns;
	uint16_t					set_id;

	// Handle active phase:
	pthread_mutex_t				requeue_lock;
	int							priority;
	cf_atomic32					active_rc;
	volatile int				next_pid;
	volatile int				abandoned;

	// For tracking:
	uint64_t					start_ms;
	uint64_t					finish_ms;
	cf_atomic64					n_records_read;
} as_job;

void as_job_init(as_job* _job, const as_job_vtable* vtable,
		struct as_job_manager_s* manager, as_job_rsv_type rsv_type,
		uint64_t trid, as_namespace* ns, uint16_t set_id, int priority);
void as_job_slice(void* task);
void as_job_finish(as_job* _job);
void as_job_destroy(as_job* _job);
void as_job_info(as_job* _job, as_mon_jobstat* stat);
void as_job_active_reserve(as_job* _job);
void as_job_active_release(as_job* _job);

//----------------------------------------------------------
// as_job_manager - class header.
//

typedef struct as_job_manager_s {
	pthread_mutex_t			lock;
	cf_queue*				active_jobs;
	cf_queue*				finished_jobs;
	as_priority_thread_pool	thread_pool;

	// Manager configuration:
	uint32_t				max_active;
	uint32_t				max_done;
} as_job_manager;

void as_job_manager_init(as_job_manager* mgr, uint32_t max_active, uint32_t max_done, uint32_t n_threads);
int as_job_manager_start_job(as_job_manager* mgr, as_job* _job);
void as_job_manager_requeue_job(as_job_manager* mgr, as_job* _job);
void as_job_manager_finish_job(as_job_manager* mgr, as_job* _job);
void as_job_manager_abandon_job(as_job_manager* mgr, as_job* _job, int reason);
bool as_job_manager_abort_job(as_job_manager* mgr, uint64_t trid);
int as_job_manager_abort_all_jobs(as_job_manager* mgr);
bool as_job_manager_change_job_priority(as_job_manager* mgr, uint64_t trid, int priority);
void as_job_manager_limit_active_jobs(as_job_manager* mgr, uint32_t max_active);
void as_job_manager_limit_finished_jobs(as_job_manager* mgr, uint32_t max_done);
void as_job_manager_resize_thread_pool(as_job_manager* mgr, uint32_t n_threads);
as_mon_jobstat* as_job_manager_get_job_info(as_job_manager* mgr, uint64_t trid);
as_mon_jobstat* as_job_manager_get_info(as_job_manager* mgr, int* size);
int as_job_manager_get_active_job_count(as_job_manager* mgr);
