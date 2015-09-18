/*
 * bin.c
 *
 * Copyright (C) 2008-2014 Aerospike, Inc.
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
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */

/*
 *  bin operations
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"

#include "fault.h"
#include "vmapx.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "storage/storage.h"


bool
as_bin_get_id_from_name_buf(as_namespace *ns, byte *buf, size_t len, uint32_t *p_id)
{
	if (ns->single_bin) {
		cf_crash(AS_BIN, "single-bin call of as_bin_get_id_from_name_buf()");
	}

	char name[len + 1];
	memcpy(name, buf, len);
	name[len] = 0;

	return cf_vmapx_get_index(ns->p_bin_name_vmap, name, p_id) == CF_VMAPX_OK;
}

// caller-beware, name cannot be null
int16_t
as_bin_get_id(as_namespace *ns, const char *name)
{
	if (ns->single_bin) {
		cf_crash(AS_BIN, "single-bin call of as_bin_get_id()");
	}
	uint32_t idx;
	if (cf_vmapx_get_index(ns->p_bin_name_vmap, name, &idx) == CF_VMAPX_OK) {
		return (uint16_t)idx;
	}
	return -1;
}

uint16_t
as_bin_get_or_assign_id(as_namespace *ns, const char *name)
{
	if (ns->single_bin) {
		cf_crash(AS_BIN, "single-bin call of as_bin_get_or_assign_id()");
	}

	uint32_t idx;

	if (cf_vmapx_get_index(ns->p_bin_name_vmap, name, &idx) == CF_VMAPX_OK) {
		return (uint16_t)idx;
	}

	// This should always have been checked by this point.
	if (strlen(name) >= AS_ID_BIN_SZ) {
		cf_crash(AS_BIN, "bin name %s too long", name);
	}

	cf_vmapx_err result = cf_vmapx_put_unique(ns->p_bin_name_vmap, name, &idx);

	if (! (result == CF_VMAPX_OK || result == CF_VMAPX_ERR_NAME_EXISTS)) {
		// Tedious to handle safely for all usage paths, so for now...
		cf_crash(AS_BIN, "could not add bin name %s, vmap err %d", name, result);
	}

	return (uint16_t)idx;
}

const char *
as_bin_get_name_from_id(as_namespace *ns, uint16_t id)
{
	if (ns->single_bin) {
		cf_crash(AS_BIN, "single-bin call of as_bin_get_name_from_id()");
	}

	const char* name = NULL;

	if (cf_vmapx_get_by_index(ns->p_bin_name_vmap, id, (void**)&name) != CF_VMAPX_OK) {
		// TODO - Fail softly by returning forbidden bin name? (Empty string?)
		cf_crash(AS_BIN, "no bin name for id %u", id);
	}

	return name;
}

bool
as_bin_name_within_quota(as_namespace *ns, const char *name)
{
	// Won't exceed quota if single-bin or currently below quota.
	if (ns->single_bin || cf_vmapx_count(ns->p_bin_name_vmap) < BIN_NAMES_QUOTA) {
		return true;
	}

	// Won't exceed quota if name is found (and so would NOT be added to vmap).
	if (cf_vmapx_get_index(ns->p_bin_name_vmap, name, NULL) == CF_VMAPX_OK) {
		return true;
	}

	cf_warning(AS_BIN, "{%s} bin-name quota full - can't add new bin-name %s", ns->name, name);

	return false;
}

void
as_bin_all_dump(as_storage_rd *rd, char *msg)
{
	cf_info(AS_BIN, "bin dump: %s: new nbins %d", msg, rd->n_bins);
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];
		cf_info(AS_BIN, "bin %s: %d: bin %p inuse %d particle %p", msg, i, b, as_bin_inuse(b), b->particle);
	}
}

static inline void
as_bin_init_nameless(as_namespace *ns, as_bin *b)
{
	as_bin_state_set(b, AS_BIN_STATE_UNUSED);
	as_bin_set_version(b, 0, ns->single_bin);

	b->particle = NULL;
}

void
as_bin_init(as_namespace *ns, as_bin *b, const char *name)
{
	as_bin_state_set(b, AS_BIN_STATE_UNUSED);
	as_bin_set_version(b, 0, ns->single_bin);

	b->particle = NULL;

	as_bin_set_id_from_name(ns, b, name);
	// Don't touch b->unused - like b->id, it's past the end of its enclosing
	// as_index if single-bin, data-in-memory.
}

static inline
as_bin_space* safe_bin_space(const as_record *r) {
	return r->dim ? as_index_get_bin_space(r) : NULL;
}

static inline
uint16_t safe_n_bins(const as_record *r) {
	as_bin_space* bin_space = safe_bin_space(r);
	return bin_space ? bin_space->n_bins : 0;
}

static inline
as_bin* safe_bins(const as_record *r) {
	as_bin_space* bin_space = safe_bin_space(r);
	return bin_space ? bin_space->bins : NULL;
}

uint16_t
as_bin_get_n_bins(as_record *r, as_storage_rd *rd)
{
	if (rd->ns->single_bin) {
		return (1);
	}

	if (rd->ns->storage_data_in_memory) {
		return safe_n_bins(r);
	}

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		if (! rd->have_device_block) {
			as_storage_record_read(rd);
		}

		return (as_storage_record_get_n_bins(rd));
	}

	return (0);
}

as_bin *
as_bin_get_all(as_record *r, as_storage_rd *rd, as_bin *stack_bins)
{
	if (rd->ns->storage_data_in_memory) {
		return rd->ns->single_bin ? as_index_get_single_bin(r) : safe_bins(r);
	}

	rd->bins = stack_bins;
	as_bin_set_all_empty(rd);

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		as_storage_particle_read_all(rd);
	}

	return (stack_bins);
}

// - Seems like an as_storage_record method, but leaving it here for now.
// - sets rd->bins!
int
as_storage_rd_load_bins(as_storage_rd *rd, as_bin *stack_bins)
{
	if (rd->ns->storage_data_in_memory) {
		rd->bins = rd->ns->single_bin ? as_index_get_single_bin(rd->r) :
				safe_bins(rd->r);
		return 0;
	}

	// Data NOT in-memory.

	rd->bins = stack_bins;
	as_bin_set_all_empty(rd);

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		int result = as_storage_particle_read_all_ssd(rd);

		if (result < 0) {
			return result;
		}
	}

	return 0;
}

// utility function to convert a pointer to the bin space to an array of pointers to each (used) bin

void
as_bin_get_all_p(as_storage_rd *rd, as_bin **bin_ptrs)
{
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		bin_ptrs[i] = &rd->bins[i];
	}
}

// Does not check bin name length.
as_bin *
as_bin_create(as_storage_rd *rd, const char *name)
{
	if (rd->ns->single_bin) {
		if (as_bin_inuse(rd->bins)) {
			cf_crash(AS_BIN, "single bin create found bin in use");
		}

		as_bin_init_nameless(rd->ns, rd->bins);

		return rd->bins;
	}

	as_bin *b = NULL;

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		if (! as_bin_inuse(&rd->bins[i])) {
			b = &rd->bins[i];
			break;
		}
	}

	if (b) {
		as_bin_init(rd->ns, b, name);
	}

	return b;
}

as_bin *
as_bin_create_from_buf(as_storage_rd *rd, byte *name, size_t namesz)
{
	if (rd->ns->single_bin) {
		if (as_bin_inuse(rd->bins)) {
			cf_crash(AS_BIN, "single bin create found bin in use");
		}

		as_bin_init_nameless(rd->ns, rd->bins);

		return rd->bins;
	}

	if (namesz >= AS_ID_BIN_SZ) {
		cf_warning(AS_BIN, "bin name too long (%lu)", namesz);
		return NULL;
	}

	as_bin *b = NULL;

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		if (! as_bin_inuse(&rd->bins[i])) {
			b = &rd->bins[i];
			break;
		}
	}

	if (b) {
		char zname[namesz + 1];

		memcpy(zname, name, namesz);
		zname[namesz] = 0;

		as_bin_init(rd->ns, b, zname);
	}

	return b;
}

as_bin *
as_bin_get_by_id(as_storage_rd *rd, uint32_t id)
{
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];

		if (! as_bin_inuse(b)) {
			break;
		}

		if ((uint32_t)b->id == id) {
			return b;
		}
	}

	return NULL;
}

as_bin *
as_bin_get(as_storage_rd *rd, const char *name)
{
	if (rd->ns->single_bin) {
		return as_bin_inuse_has(rd) ? rd->bins : NULL;
	}

	uint32_t id;

	if (cf_vmapx_get_index(rd->ns->p_bin_name_vmap, name, &id) != CF_VMAPX_OK) {
		return NULL;
	}

	return as_bin_get_by_id(rd, id);
}

as_bin *
as_bin_get_from_buf(as_storage_rd *rd, byte *name, size_t namesz)
{
	if (rd->ns->single_bin) {
		return as_bin_inuse_has(rd) ? rd->bins : NULL;
	}

	uint32_t id;

	if (! as_bin_get_id_from_name_buf(rd->ns, name, namesz, &id)) {
		return NULL;
	}

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];

		if (! as_bin_inuse(b)) {
			break;
		}

		if ((uint32_t)b->id == id) {
			return b;
		}
	}

	return NULL;
}

// Does not check bin name length.
// Checks bin name quota - use appropriately.
as_bin *
as_bin_get_or_create(as_storage_rd *rd, const char *name)
{
	if (rd->ns->single_bin) {
		if (! as_bin_inuse_has(rd)) {
			as_bin_init_nameless(rd->ns, rd->bins);
		}

		return rd->bins;
	}

	uint32_t id = (uint32_t)-1;
	uint16_t i;
	as_bin *b;

	if (cf_vmapx_get_index(rd->ns->p_bin_name_vmap, name, &id) == CF_VMAPX_OK) {
		for (i = 0; i < rd->n_bins; i++) {
			b = &rd->bins[i];

			if (! as_bin_inuse(b)) {
				break;
			}

			if ((uint32_t)b->id == id) {
				return b;
			}
		}
	}
	else {
		if (cf_vmapx_count(rd->ns->p_bin_name_vmap) >= BIN_NAMES_QUOTA) {
			cf_warning(AS_BIN, "{%s} bin-name quota full - can't add new bin-name %s", rd->ns->name, name);
			return NULL;
		}

		i = as_bin_inuse_count(rd);
	}

	if (i >= rd->n_bins) {
		cf_crash(AS_BIN, "ran out of allocated bins in rd");
	}

	b = &rd->bins[i];

	if (id == (uint32_t)-1) {
		as_bin_init(rd->ns, b, name);
	}
	else {
		as_bin_init_nameless(rd->ns, b);
		b->id = (uint16_t)id;
	}

	return b;
}

// Does not check bin name length.
// Checks bin name quota and bin-level policy - use appropriately.
as_bin *
as_bin_get_or_create_from_buf(as_storage_rd *rd, byte *name, size_t namesz,
		bool create_only, bool replace_only, int *p_result)
{
	if (rd->ns->single_bin) {
		if (! as_bin_inuse_has(rd)) {
			as_bin_init_nameless(rd->ns, rd->bins);
		}

		// Ignored bin-level policy - single-bin needs only record-level policy.
		return rd->bins;
	}

	char zname[namesz + 1];

	memcpy(zname, name, namesz);
	zname[namesz] = 0;

	uint32_t id = (uint32_t)-1;
	uint16_t i;
	as_bin *b;

	if (cf_vmapx_get_index(rd->ns->p_bin_name_vmap, zname, &id) == CF_VMAPX_OK) {
		for (i = 0; i < rd->n_bins; i++) {
			b = &rd->bins[i];

			if (! as_bin_inuse(b)) {
				break;
			}

			if ((uint32_t)b->id == id) {
				if (as_bin_is_hidden(b)) {
					cf_warning(AS_BIN, "cannot manipulate hidden bin directly");
					*p_result = AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
					return NULL;
				}

				if (create_only) {
					*p_result = AS_PROTO_RESULT_FAIL_BIN_EXISTS;
					return NULL;
				}

				return b;
			}
		}
	}
	else {
		if (cf_vmapx_count(rd->ns->p_bin_name_vmap) >= BIN_NAMES_QUOTA) {
			cf_warning(AS_BIN, "{%s} bin-name quota full - can't add new bin-name %s", rd->ns->name, zname);
			*p_result = AS_PROTO_RESULT_FAIL_BIN_NAME;
			return NULL;
		}

		i = as_bin_inuse_count(rd);
	}

	if (replace_only) {
		*p_result = AS_PROTO_RESULT_FAIL_BIN_NOT_FOUND;
		return NULL;
	}

	if (i >= rd->n_bins) {
		cf_crash(AS_BIN, "ran out of allocated bins in rd");
	}

	b = &rd->bins[i];

	if (id == (uint32_t)-1) {
		as_bin_init(rd->ns, b, zname);
	}
	else {
		as_bin_init_nameless(rd->ns, b);
		b->id = (uint16_t)id;
	}

	return b;
}

int32_t
as_bin_get_index(as_storage_rd *rd, const char *name)
{
	if (rd->ns->single_bin) {
		return as_bin_inuse_has(rd) ? 0 : -1;
	}

	uint32_t id;

	if (cf_vmapx_get_index(rd->ns->p_bin_name_vmap, name, &id) != CF_VMAPX_OK) {
		return -1;
	}

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];

		if (! as_bin_inuse(b)) {
			break;
		}

		if ((uint32_t)b->id == id) {
			return (int32_t)i;
		}
	}

	return -1;
}

int32_t
as_bin_get_index_from_buf(as_storage_rd *rd, byte *name, size_t namesz)
{
	if (rd->ns->single_bin) {
		return as_bin_inuse_has(rd) ? 0 : -1;
	}

	uint32_t id;

	if (! as_bin_get_id_from_name_buf(rd->ns, name, namesz, &id)) {
		return -1;
	}

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];

		if (! as_bin_inuse(b)) {
			break;
		}

		if ((uint32_t)b->id == id) {
			return (int32_t)i;
		}
	}

	return -1;
}

void
as_bin_allocate_bin_space(as_record *r, as_storage_rd *rd, int32_t delta) {
	if (rd->n_bins == 0) {
		rd->n_bins = (uint16_t)delta;

		as_bin_space* bin_space = (as_bin_space*)
				cf_malloc(sizeof(as_bin_space) + (rd->n_bins * sizeof(as_bin)));

		rd->bins = bin_space->bins;
		as_bin_set_all_empty(rd);

		bin_space->n_bins = rd->n_bins;
		as_index_set_bin_space(r, bin_space);
	}
	else {
		uint16_t new_n_bins = (uint16_t)((int32_t)rd->n_bins + delta);

		if (delta < 0) {
			as_record_clean_bins_from(rd, new_n_bins);
		}

		uint16_t old_n_bins = rd->n_bins;

		rd->n_bins = new_n_bins;

		if (new_n_bins != 0) {
			as_bin_space* bin_space = (as_bin_space*)
					cf_realloc((void*)as_index_get_bin_space(r), sizeof(as_bin_space) + (rd->n_bins * sizeof(as_bin)));

			rd->bins = bin_space->bins;

			if (delta > 0) {
				as_bin_set_empty_from(rd, old_n_bins);
			}

			bin_space->n_bins = rd->n_bins;
			as_index_set_bin_space(r, bin_space);
		}
		else {
			cf_free((void*)as_index_get_bin_space(r));
			as_index_set_bin_space(r, NULL);
			rd->bins = NULL;
		}
	}
}

void
as_bin_destroy(as_storage_rd *rd, uint16_t i)
{
	as_bin_particle_destroy(&rd->bins[i], rd->ns->storage_data_in_memory);
	as_bin_set_empty_shift(rd, i);
}

void
as_bin_destroy_from(as_storage_rd *rd, uint16_t from)
{
	for (uint16_t i = from; i < rd->n_bins; i++) {
		if (! as_bin_inuse(&rd->bins[i])) {
			break;
		}

		as_bin_particle_destroy(&rd->bins[i], rd->ns->storage_data_in_memory);
	}

	as_bin_set_empty_from(rd, from);
}

void
as_bin_destroy_all(as_storage_rd *rd)
{
	as_bin_destroy_from(rd, 0);
}

uint16_t
as_bin_inuse_count(as_storage_rd *rd)
{
	uint16_t i;
	// TODO: rd NULL is an error condition, 0 is a valid value, change function semantics
	if (!rd) {
		return 0;
	}
	for (i = 0; i < rd->n_bins; i++) {
		if (! as_bin_inuse(&rd->bins[i])) {
			break;
		}
	}

	return (i);
}
