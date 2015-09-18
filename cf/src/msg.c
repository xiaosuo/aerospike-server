/*
 * msg.c
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
 * This is a generic binary format message parsing system
 * You create the definition of the message not by an IDL, but by
 * a .H file. Eventually we're going to need to do a similar thing using java,
 * though, which would promote an IDL-style approach
 * All rights reserved
 */

#include "msg.h"

#include <endian.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <netinet/in.h> // for htonl, htons

#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_types.h>
#include <citrusleaf/alloc.h>

#include "dynbuf.h"
#include "fault.h"


// Define this if you want extra sanity checks enabled
// #define EXTRA_CHECKS 1

//
// "msg" Object Accounting:
//
//   Total number of "msg" objects allocated
cf_atomic_int g_num_msgs = 0;
//   Total number of "msg" objects allocated per type
cf_atomic_int g_num_msgs_by_type[M_TYPE_MAX] = { 0 };

// Number of "msg" obejcts per type that can be allocated.
// (Default to -1, meaning there is no limit on allowed number of "msg" objects per type.)
static int64_t g_max_msgs_per_type = -1;

// Limit the maximum number of "msg" objects per type (-1 means unlimited.)
void
msg_set_max_msgs_per_type(int64_t max_msgs)
{
	g_max_msgs_per_type = max_msgs;
}

int
msg_create(msg **m_r, msg_type type, const msg_template *mt, size_t mt_sz)
{
	// Place a limit on the number of "msg" objects of each type that may be allocated at a given time.
	// (The default value of -1 means there is no limit.)
	if (g_max_msgs_per_type > 0) {
		if (cf_atomic_int_get(g_num_msgs_by_type[type]) >= g_max_msgs_per_type) {
			cf_warning(CF_MSG, "refusing to allocate more than %d msg of type %d", g_max_msgs_per_type, type);
			return -1;
		}
	}

	// Figure out how many bytes you need
	int mt_rows = mt_sz / sizeof(msg_template);
	if (mt_rows <= 0)
		cf_crash( CF_MSG, "msg create: invalid parameter");
	unsigned int max_id = 0;
	for (int i=0;i<mt_rows;i++) {
		if (mt[i].id >= max_id) {
			max_id = mt[i].id;
		}
	}
	max_id++;

	// DEBUG - can tell if it's so sparse that we're wasting lots of memory
	if (max_id > mt_rows * 2) {
		// It would be nice if there was a human readable string for debugging
		// in the message descriptor
		cf_debug(CF_MSG, "msg_create: found sparse message, %d ids, only %d rows consider recoding",max_id,mt_rows);
	}

	// allocate memory (if necessary)
	size_t m_sz = sizeof(msg_field) * max_id;
	msg *m;
	size_t a_sz = sizeof(msg) + m_sz;
	a_sz = (a_sz + 511) & ~511UL;
	m = cf_rc_alloc(a_sz);

	cf_debug(CF_MSG, "msg_create(type: %d): a_sz: %d", type, a_sz);

	cf_assert(m, CF_MSG, CF_CRITICAL, "malloc");
	m->len = max_id;
	m->bytes_used = sizeof(msg) + m_sz;
	m->bytes_alloc = a_sz;
	m->type = type;
	m->mt = mt;

	// debug - not strictly necessary, but saves the user if they
	// have an invalid field
	for (int i=0;i<max_id;i++)
		m->f[i].is_valid = false;

	// fill in the fields - rather minimalistic, save the dcache
	for (int i=0;i<mt_rows;i++) {
		msg_field *f = &(m->f[ mt[i].id ] );
		f->id = mt[i].id;
		f->type = mt[i].type;
		f->rc_free = f->free = 0;
		f->is_set = false;
		f->is_valid = true;
	}

	*m_r = m;

	// Keep track of allocated msgs.
	cf_atomic_int_incr(&g_num_msgs);
	cf_atomic_int_incr(&(g_num_msgs_by_type[type]));

	return(0);
}

//
// Increment the reference count of the message.
// Everyone must call destroy in the end, and that must all be matched up

void
msg_incr_ref(msg *m)
{
	cf_rc_reserve(m);
}

void
msg_decr_ref(msg *m)
{
	cf_rc_release(m);
}



// Here's where the wire protocol is defined. All the rest is just copying
// stupid buffers around.
//
// Currently, there aren't a lot of alignment checks here. We assume we're
// running on an intel architecture where the chip does the fastest possible
// thing for us.
//
// Should probably have some kind of version field, because there are better
// ways of doing this on a number of levels
//
// Current protocol:
// uint32_t size-in-bytes (not including this header, network byte order)
// uint16_t type (still included in the header)
//      2 byte - field id
// 		1 byte - field type
//      4 bytes - field size
//      [x] - field
//      (7 + field sz)

// The integer arrays are kept in network format and swapped in the accessor functions.
// this allows the the zero-copy nature of the interface to be simpler

// msg_parse - parse a buffer into a message, which thus can be accessed
int
msg_parse(msg *m, const uint8_t *buf, const size_t buflen, bool copy)
{
	if (buflen < 6) {
		cf_debug(CF_MSG,"msg_parse: but not enough data! will get called again len %d need 6.",buflen);
		return(-2);
	}
	uint32_t len = ntohl( *(uint32_t *) buf );
	if (buflen < len + 6) {
		cf_debug(CF_MSG,"msg_parse: but not enough data! will get called again. buf %p len %d need %d",buf, buflen, (len + 6));
		return(-2);
	}
	buf += 4;

	uint16_t type = ntohs( *(uint16_t *) buf );
	if (m->type != type) {
		cf_debug(CF_MSG,"msg_parse: trying to parse incoming type %d into msg type %d, bad bad",type, m->type);
		return(-1);
	}
	buf += 2;

	const uint8_t *eob = buf + len;

	while (buf < eob) {

		// Grab the ID
		uint32_t id = (buf[0] << 8) | buf[1];
		buf += 2;

		// find the field in the message
		msg_field *mf;
		if (id >= m->len) {
			cf_debug(CF_MSG," received message with id greater than current definition, kind of OK, ignoring field");
			mf = 0;
		}
		else {
			mf = &(m->f[id]);
			if (! mf->is_valid ) {
				cf_debug(CF_MSG," received message with id no longer valid, kind of OK, ignoring field");
				mf = 0;
			}
		}

		msg_field_type ft = (msg_field_type) *buf++;
		uint32_t flen = ntohl(*(uint32_t *)buf);
		buf += 4;

		if (mf && (ft != mf->type)) {
			cf_debug(CF_MSG," received message with incorrect field type from definition, kind of OK, ignoring field");
			mf = 0;
		}

		if (mf) {

			switch (mf->type) {
				case M_FT_INT32:
					mf->u.i32 = ntohl( *(uint32_t *) buf ); // problem is htonl really only works on unsigned?
					break;
				case M_FT_UINT32:
					mf->u.ui32 = ntohl( *(uint32_t *) buf );
					break;
				case M_FT_INT64:
					mf->u.i64 = be64toh( *(uint64_t *) buf);
					break;
				case M_FT_UINT64:
					mf->u.ui64 = be64toh( *(uint64_t *) buf);
					break;
				case M_FT_STR:
				case M_FT_BUF:
					mf->field_len = flen;
					if (copy) {
						if (m->bytes_alloc - m->bytes_used >= flen) {
							mf->u.buf = ((uint8_t *)m) + m->bytes_used;
							m->bytes_used += flen;
							mf->free = mf->rc_free = 0;
						}
						else {
							mf->u.buf = cf_malloc(flen);
							cf_assert(mf->u.buf, CF_MSG, CF_WARNING, "malloc");
							mf->free = mf->u.buf;
							mf->rc_free = 0;
						}
						memcpy(mf->u.buf, buf, flen);
					}
					else {
						mf->u.buf = (uint8_t *) buf;
						mf->rc_free = mf->free = 0;
					}
					break;
				case M_FT_ARRAY_UINT32:
					mf->field_len = flen;
					if (copy) {
						if (m->bytes_alloc - m->bytes_used >= flen) {
							mf->u.ui32_a = (uint32_t *) (((uint8_t *)m) + m->bytes_used);
							m->bytes_used += flen;
							mf->free = mf->rc_free = 0;
						}
						else {
							mf->u.ui32_a = cf_malloc(flen);
							cf_assert(mf->u.ui32_a, CF_MSG, CF_WARNING, "malloc");
							mf->free = mf->u.ui32_a;
							mf->rc_free = 0;
						}
						memcpy(mf->u.ui32_a, buf, flen);
					}
					else {
						mf->u.ui32_a = (uint32_t *) buf;
						mf->rc_free = mf->free = 0;
					}

					break;

				case M_FT_ARRAY_UINT64:
					mf->field_len = flen;
					if (copy) {
						if (m->bytes_alloc - m->bytes_used >= flen) {
							mf->u.ui64_a = (uint64_t *) (((uint8_t *)m) + m->bytes_used);
							m->bytes_used += flen;
							mf->free = mf->rc_free = 0;
						}
						else {
							mf->u.ui32_a = cf_malloc(flen);
							cf_assert(mf->u.ui64_a, CF_MSG, CF_WARNING, "malloc");
							mf->free = mf->u.ui64_a;
							mf->rc_free = 0;
						}
						memcpy(mf->u.ui64_a, buf, flen);
					}
					else {
						mf->u.ui64_a = (uint64_t *) buf;
						mf->rc_free = mf->free = 0;
					}

					break;

				case M_FT_ARRAY_STR:
					mf->field_len = flen;
					if (copy) {
						if (m->bytes_alloc - m->bytes_used >= flen) {
							mf->u.str_a = (msg_str_array *) (((uint8_t *)m) + m->bytes_used);
							m->bytes_used += flen;
							mf->free = mf->rc_free = 0;
						}
						else {
							mf->u.str_a = cf_malloc(flen);
							cf_assert(mf->u.str_a, CF_MSG, CF_WARNING, "malloc");
							mf->free = mf->u.str_a;
							mf->rc_free = 0;
						}
						memcpy(mf->u.str_a, buf, flen);
					}
					else {
						mf->u.str_a = (msg_str_array *) buf;
						mf->rc_free = mf->free = 0;
					}
					break;

				case M_FT_ARRAY_BUF:
					mf->field_len = flen;
					if (copy) {
						if (m->bytes_alloc - m->bytes_used >= flen) {
							mf->u.buf_a = (msg_buf_array *) (((uint8_t *)m) + m->bytes_used);
							m->bytes_used += flen;
							mf->free = mf->rc_free = 0;
						}
						else {
							mf->u.buf_a = cf_malloc(flen);
							cf_assert(mf->u.buf_a, CF_MSG, CF_WARNING, "malloc");
							mf->free = mf->u.buf_a;
							mf->rc_free = 0;
						}
						memcpy(mf->u.buf_a, buf, flen);
					}
					else {
						mf->u.buf_a = (msg_buf_array *) buf;
						mf->rc_free = mf->free = 0;
					}
					break;

				default:
					cf_debug(CF_MSG,"msg_parse: field type not supported, but skipping over anyway: %d",mf->type);
			}
			mf->is_set = true;
		}
		buf += flen;
	};

	return(0);
}

//
int
msg_get_initial(uint32_t *size_r, msg_type *type_r, const uint8_t *buf, const uint32_t buflen)
{
	if (buflen < 6)
		return(-2);

	uint32_t size = * (uint32_t *) buf;
	size = ntohl(size);
	// size does not include this header
	size += 6;
	*size_r = size;

	buf += 4;
	uint16_t type = * (uint16_t *) buf;
	type = ntohs(type);
	*type_r = type;

	return( 0 );
}


static inline size_t
msg_get_wire_field_size(const msg_field *mf)
{
	switch (mf->type) {
		case M_FT_INT32:
		case M_FT_UINT32:
			return(4 + 7);
		case M_FT_INT64:
		case M_FT_UINT64:
			return(8 + 7);
		case M_FT_STR:
		case M_FT_BUF:
		case M_FT_ARRAY_UINT32:
		case M_FT_ARRAY_UINT64:
		case M_FT_ARRAY_STR:
		case M_FT_ARRAY_BUF:
			if (mf->field_len >= ( 1 << 24 ))
				cf_debug(CF_MSG,"field length %d too long, not yet supported", mf->field_len);
			return(mf->field_len + 7);
		default:
			cf_debug(CF_MSG,"field type not supported, internal error: %d",mf->type);
	}
	return(0);
}

// Write a field into the buffer at this point, returning the
// number of bytes written
// returns the number of bytes written

static inline uint32_t
msg_stamp_field(uint8_t *buf, const msg_field *mf)
{
	// Stamp the ID
	buf[0] = (mf->id >> 8) & 0xff;
	buf[1] = mf->id & 0xff;
	buf += 2;

	// stamp the type
	*buf++ = (msg_field_type) mf->type;

	// Stamp the field itself (forward over the length, we'll patch that later
	uint32_t flen;
	buf += 4;
	switch(mf->type) {
		case M_FT_INT32:
			flen = 4;
			int32_t *b_i32 = (int32_t *)buf;
			*b_i32 = htonl(mf->u.i32);
			break;

		case M_FT_UINT32:
			flen = 4;
			uint32_t *b_ui32 = (uint32_t *)buf;
			*b_ui32 = htonl(mf->u.ui32);
			break;

		case M_FT_INT64:
			flen = 8;
			int64_t *b_i64 = (int64_t *)buf;
			*b_i64 = htobe64(mf->u.i64);
			break;

		case M_FT_UINT64:
			flen = 8;
			uint64_t *b_ui64 = (uint64_t *)buf;
			*b_ui64 = htobe64(mf->u.ui64);
			break;

		case M_FT_STR:
			flen = mf->field_len;
			memcpy(buf, mf->u.str, flen);
			break;

		case M_FT_BUF:
			flen = mf->field_len;
			memcpy(buf, mf->u.buf, flen);
			break;

		case M_FT_ARRAY_UINT32:
			flen = mf->field_len;
			memcpy(buf, mf->u.ui32_a, flen);
			break;

		case M_FT_ARRAY_UINT64:
			flen = mf->field_len;
			memcpy(buf, mf->u.ui64_a, flen);
			break;

		case M_FT_ARRAY_STR:
			flen = mf->field_len;
			memcpy(buf, mf->u.str_a, flen);
			break;

		case M_FT_ARRAY_BUF:
			flen = mf->field_len;
			memcpy(buf, mf->u.buf_a, flen);
			break;

		default:
			cf_debug(CF_MSG,"field type not supported, internal error: %d",mf->type);
			return(0);
	}

	// Now, patch the length back in
	buf[-4] = (flen >> 24) & 0xFF;
	buf[-3] = (flen >> 16) & 0xff;
	buf[-2] = (flen >> 8) & 0xff;
	buf[-1] = flen & 0xff;

//	if (flen > 1024 * 1024)
//		fprintf(stderr, "large field: size %zd\n",flen);

	return(7 + flen);
}


// msg_tobuf - parse a message out into a buffer.
// Interesting point: for a sparse buffer, it's better to rip through the msg
// description and deference to the table, but for non-sparse it's kinder to the
// cache lines to just rip through the actual table. We assume non-sparse at the
// moment.

int
msg_fillbuf(const msg *m, uint8_t *buf, size_t *buflen)
{
	// Figure out the size
	uint32_t	sz = 6;

	for (int i=0;i<m->len;i++) {
		const msg_field *mf = &m->f[i];
		if ((mf->is_valid==true) && (mf->is_set==true)) {
			sz += msg_get_wire_field_size(mf);
		}
	}

	// validate the size
	if (sz > *buflen) {
		cf_debug(CF_MSG,"msg_fillbuf: passed in size too small want %d have %d",sz,*buflen);
		*buflen = sz; // tell the caller how much size you're really going to need
		return(-2);
	}
	*buflen = sz;

	// stamp the size in the buf
	(* (uint32_t *) buf) = htonl(sz - 6);
	buf += 4;
	// stamp the type
	(* (uint16_t *) buf) = htons(m->type);
	buf += 2;

	// copy the fields
	for (int i=0;i<m->len;i++) {
		const msg_field *mf = &m->f[i];
		if ((mf->is_valid==true) && (mf->is_set==true)) {
			buf += msg_stamp_field(buf, mf);
		}
	}

	return(0);
}

//
// Purpose of msg_reset is to reset its internal state and make it ready for more reading or parsing

void
msg_reset(msg *m)
{
	m->bytes_used = (m->len * sizeof(msg_field)) + sizeof(msg);
	for (int i=0 ; i < m->len ; i++) {
		if (m->f[i].is_valid == true) {
			if (m->f[i].is_set == true) {
//				cf_debug(CF_MSG,"msg_reset: freeing %p rcfree %p",m->f[i].free,m->f[i].rc_free);
				if (m->f[i].free) cf_free(m->f[i].free);
				if (m->f[i].rc_free) cf_rc_releaseandfree(m->f[i].rc_free);
				m->f[i].is_set = false;
			}
		}
	}
}

#ifdef EXTRA_CHECKS
void VALIDATE(const msg *m, int field_id, int type) {
	if (! m->f[field_id].is_valid) {
		cf_crash(CF_MSG, "msg: invalid id %d in field ",field_id);
		return; // fault thread scope shouldn't reach here
	}

	if ( m->f[field_id].type != type ) {
		cf_crash(CF_MSG, "msg: mismatch setter field type wants %d has %d",m->f[field_id].type, type);
		return; // fault thread scope shouldn't reach here
	}
}
#else
#define VALIDATE(_m, _field_id, _type)
#endif



//
// Getters and setters
//
int
msg_get_uint32(const msg *m, int field_id, uint32_t *r)
{
	VALIDATE(m, field_id, M_FT_UINT32);

	if ( ! m->f[field_id].is_set ) {
//		cf_fault(CF_FAULT_SCOPE_THREAD, CF_FAULT_SEVERITY_NOTICE, "msg %p: attempt to retrieve unset field %d",m,field_id);
//		msg_dump(m);
		*r = 0;
		return(-2);
	}

	*r = m->f[field_id].u.ui32;

	return(0);
}

int msg_get_int32(const msg *m, int field_id, int32_t *r)
{
	VALIDATE(m, field_id, M_FT_INT32);

	if ( ! m->f[field_id].is_set ) {
//		cf_fault(CF_FAULT_SCOPE_THREAD, CF_FAULT_SEVERITY_NOTICE, "msg: attempt to retrieve unset field %d",field_id);
//		msg_dump(m);
		*r = 0;
		return(-2);
	}

	*r = m->f[field_id].u.i32;

	return(0);
}

int msg_get_uint64(const msg *m, int field_id, uint64_t *r)
{
	VALIDATE(m, field_id, M_FT_UINT64);

	if ( ! m->f[field_id].is_set ) {
//		cf_fault(CF_FAULT_SCOPE_THREAD, CF_FAULT_SEVERITY_NOTICE, "msg %p: attempt to retrieve unset field %d",m,field_id);
//		msg_dump(m);
		*r = 0;
		return(-2);
	}

	*r = m->f[field_id].u.ui64;

	return(0);
}

int msg_get_int64(const msg *m, int field_id, int64_t *r)
{
	VALIDATE(m, field_id, M_FT_INT64);

	if ( ! m->f[field_id].is_set ) {
//		cf_fault(CF_FAULT_SCOPE_THREAD, CF_FAULT_SEVERITY_NOTICE, "msg %p: attempt to retrieve unset field %d",m,field_id);
//		msg_dump(m);
		*r = 0;
		return(-2);
	}

	*r = m->f[field_id].u.i64;

	return(0);
}


int
msg_get_str(const msg *m, int field_id, char **r, size_t *len, msg_get_type type)  // this length is strlen+1, the allocated size
{
	VALIDATE(m, field_id, M_FT_STR);

	if ( ! m->f[field_id].is_set ) {
//		cf_fault(CF_FAULT_SCOPE_THREAD, CF_FAULT_SEVERITY_NOTICE, "msg %p: attempt to retrieve unset field %d",m,field_id);
//		msg_dump(m);
		*r = 0;
		if (len)
			*len = 0;
		return(-2);
	}

	if (MSG_GET_DIRECT == type) {
		*r = m->f[field_id].u.str;
	}
	else if (MSG_GET_COPY_MALLOC == type) {
		*r = cf_strdup( m->f[field_id].u.str );
		cf_assert(*r, CF_MSG, CF_CRITICAL, "malloc");
	} else if (MSG_GET_COPY_RC == type) {
		size_t sz = m->f[field_id].field_len + 1;
		*r = cf_rc_alloc(sz);
		memcpy(*r, m->f[field_id].u.str, sz);
	}
	else {
		cf_warning(CF_MSG, "msg_get_str: illegal type");
		return(-2);
	}

	if (len)
		*len = m->f[field_id].field_len;

	return(0);
}

int
msg_get_str_len(const msg *m, int field_id, size_t *len)  // this length is strlen+1, the allocated size
{
	VALIDATE(m, field_id, M_FT_STR);

	if ( ! m->f[field_id].is_set ) {
//		cf_fault(CF_FAULT_SCOPE_THREAD, CF_FAULT_SEVERITY_NOTICE, "msg %p: attempt to retrieve unset field %d",m,field_id);
//		msg_dump(m);
		*len = 0;
		return(-2);
	}

	if (len)
		*len = m->f[field_id].field_len;

	return(0);
}


int
msg_get_buf(const msg *m, int field_id, uint8_t **r, size_t *len, msg_get_type type)
{
	VALIDATE(m, field_id, M_FT_BUF);

	if ( ! m->f[field_id].is_set ) {
//		cf_fault(CF_FAULT_SCOPE_THREAD, CF_FAULT_SEVERITY_NOTICE, "msg %p: attempt to retrieve unset field %d",m,field_id);
//		msg_dump(m);
		*r = 0; *len = 0;
		return(-2);
	}

	if (MSG_GET_DIRECT == type) {
		*r = m->f[field_id].u.buf;
	}
	else if (MSG_GET_COPY_MALLOC == type) {
		*r = cf_malloc( m->f[field_id].field_len );
		cf_assert(*r, CF_MSG, CF_CRITICAL, "malloc");
		memcpy(*r, m->f[field_id].u.buf, m->f[field_id].field_len );
	}
	else if (MSG_GET_COPY_RC == type) {
		*r = cf_rc_alloc( m->f[field_id].field_len );
		cf_assert(*r, CF_MSG, CF_CRITICAL, "malloc");
		memcpy(*r, m->f[field_id].u.buf, m->f[field_id].field_len );
	} else {
		cf_warning(CF_MSG, "msg_get_str: illegal type");
		return(-2);
	}

	if (len)
		*len = m->f[field_id].field_len;

	return(0);
}

int
msg_get_buf_len(const msg *m, int field_id, size_t *len)
{
	VALIDATE(m, field_id, M_FT_BUF);

	if ( ! m->f[field_id].is_set ) {
//		cf_fault(CF_FAULT_SCOPE_THREAD, CF_FAULT_SEVERITY_NOTICE, "msg %p: attempt to retrieve unset field %d",m,field_id);
//		msg_dump(m);
		*len = 0;
		return(-2);
	}

	if (len)
		*len = m->f[field_id].field_len;

	return(0);
}


// A bytearray is always a reference counted object. Now, the cool thing, when
// someone wants to get one of these, is to already have the ref-counted object
// and simply increment and hand out the pointer.
// That would demand a different MSG_field_type that is a bytearray type,
// which is eminently reasonable

int
msg_get_bytearray(const msg *m, int field_id, cf_bytearray **r)
{
	VALIDATE(m, field_id, M_FT_BUF);

	if ( ! m->f[field_id].is_set ) {
//		cf_fault(CF_FAULT_SCOPE_THREAD, CF_FAULT_SEVERITY_NOTICE, "msg %p: attempt to retrieve unset field %d",m,field_id);
//		msg_dump(m);
		*r = 0;
		return(-2);
	}
	uint32_t field_len = m->f[field_id].field_len;
	*r = cf_rc_alloc( field_len + sizeof(cf_bytearray) );
	cf_assert(*r, CF_MSG, CF_CRITICAL, "rcalloc");
	(*r)->sz = field_len;
	memcpy((*r)->data, m->f[field_id].u.buf, field_len );

	return(0);
}


int
msg_set_uint32(msg *m, int field_id, const uint32_t v)
{
	VALIDATE(m, field_id, M_FT_UINT32);

	m->f[field_id].is_set = true;
	m->f[field_id].u.ui32 = v;

	return(0);
}

int
msg_set_int32(msg *m, int field_id, const int32_t v)
{
	VALIDATE(m, field_id, M_FT_INT32);

	m->f[field_id].is_set = true;
	m->f[field_id].u.i32 = v;

	return(0);
}

int
msg_set_uint64(msg *m, int field_id, const uint64_t v)
{
	VALIDATE(m, field_id, M_FT_UINT64);

	m->f[field_id].is_set = true;
	m->f[field_id].u.ui64 = v;

	return(0);
}

int
msg_set_int64(msg *m, int field_id, const int64_t v)
{
	VALIDATE(m, field_id, M_FT_INT64);

	m->f[field_id].is_set = true;
	m->f[field_id].u.i64 = v;

	return(0);
}

int
msg_set_str(msg *m, int field_id, const char *v, msg_set_type type)
{
	VALIDATE(m, field_id, M_FT_STR);

	msg_field *mf = &(m->f[field_id]);

	// free old value if necessary
	if (mf->is_set) {
		if (mf->free) {	cf_free(mf->free); mf->free = 0; }
		if (mf->rc_free) { cf_rc_releaseandfree(mf->rc_free); mf->rc_free = 0; }
	}

	mf->field_len = strlen(v)+1;

	if (MSG_SET_COPY == type) {
		size_t len = mf->field_len;
		// If we've got a little extra memory here already, use it
		if (m->bytes_alloc - m->bytes_used >= len) {
			mf->u.str = (char *) (((uint8_t *)m) + m->bytes_used);
			m->bytes_used += len;
			mf->free = 0;
			memcpy(mf->u.str, v, len);
		}
		else {
			mf->u.str = cf_strdup(v);
			cf_assert(mf->u.str, CF_MSG, CF_CRITICAL, "malloc");
			mf->free = mf->u.str;
			mf->rc_free = 0;
		}
	} else if (MSG_SET_HANDOFF_MALLOC == type) {
		mf->u.str = (char *)v;
		mf->rc_free = 0;
		mf->free = (char *)v;
	}
	else if (MSG_SET_HANDOFF_RC == type) {
		mf->u.str = (char *)v;
		mf->rc_free = (char *)v;
		mf->free = 0;
	}

	mf->is_set = true;

	return(0);
}

int msg_set_buf(msg *m, int field_id, const uint8_t *v, size_t len, msg_set_type type)
{
	VALIDATE(m, field_id, M_FT_BUF);

	msg_field *mf = &(m->f[field_id]);

	// free old value if necessary
	if (mf->is_set) {
		if (mf->free) {	cf_free(mf->free); mf->free = 0; }
		if (mf->rc_free) { cf_rc_releaseandfree(mf->rc_free); mf->rc_free = 0; }
	}

	mf->field_len = len;

	if (MSG_SET_COPY == type) {
		// If we've got a little extra memory here already, use it
		if (m->bytes_alloc - m->bytes_used >= len) {
			mf->u.buf = ((uint8_t *)m) + m->bytes_used;
			m->bytes_used += len;
			mf->rc_free = mf->free = 0;
		}
		else {
			mf->u.buf = cf_malloc(len);
			if (mf->u.buf == NULL)
				cf_info(CF_MSG, "could not allocate: len %d",len);
			cf_assert(mf->u.buf, CF_MSG, CF_CRITICAL, "malloc");
			mf->free = mf->u.buf; // free on exit/reset
			mf->rc_free = 0;
		}

		memcpy(mf->u.buf, v, len);

	} else if (type == MSG_SET_HANDOFF_MALLOC) {
		mf->u.buf = (void *)v;
		mf->free = (void *)v;
		mf->rc_free = 0;
	}
	else if (type == MSG_SET_HANDOFF_RC) {
		mf->u.buf = (void *)v;
		mf->rc_free = (void *)v;
		mf->free = 0;
	}

	mf->is_set = true;

	return(0);
}

int msg_set_bufbuilder(msg *m, int field_id, cf_buf_builder *bb)
{
	VALIDATE(m, field_id, M_FT_BUF);

	msg_field *mf = &(m->f[field_id]);

	// free old value if necessary
	if (mf->is_set) {
		if (mf->free) {	cf_free(mf->free); mf->free = 0; }
		if (mf->rc_free) { cf_rc_releaseandfree(mf->rc_free); mf->rc_free = 0; }
	}

	mf->field_len = bb->used_sz;

	mf->u.buf = bb->buf;
	mf->free = bb;
	mf->rc_free = 0;

	mf->is_set = true;

	return(0);
}


int
msg_get_uint32_array_size(msg *m, int field_id, int *size)
{
	VALIDATE(m, field_id, M_FT_ARRAY_UINT32);

	msg_field *mf = &(m->f[field_id]);

	if (mf->is_set == false)	return(-1);
	*size = mf->field_len >> 2;

	return(0);
}

int
msg_get_uint32_array(msg *m, int field_id, const int index, uint32_t *r)
{
	VALIDATE(m, field_id, M_FT_ARRAY_UINT32);

	msg_field *mf = &(m->f[field_id]);

	if (mf->is_set == false) return(-1);
	*r = ntohl( mf->u.ui32_a[index] );

	return(0);
}

int
msg_set_uint32_array_size(msg *m, int field_id, const int size)
{
	VALIDATE(m, field_id, M_FT_ARRAY_UINT32);

	msg_field *mf = &(m->f[field_id]);

	mf->field_len = size * sizeof(uint32_t);
	if (mf->is_set) {
		mf->u.ui32_a = cf_realloc( mf->u.ui32_a, mf->field_len);
		if (! mf->u.ui32_a) return(-1);
	}
	else {
		mf->u.ui32_a = cf_malloc( mf->field_len );
		if (! mf->u.ui32_a) return(-1);
		mf->is_set = true;
	}
	mf->free = mf->u.ui32_a;
	return(0);
}

int
msg_set_uint32_array(msg *m, int field_id, const int index, const uint32_t v)
{
	VALIDATE(m, field_id, M_FT_ARRAY_UINT32);

	msg_field *mf = &(m->f[field_id]);

	if (mf->is_set == false)	return(-1);
	if (index >= (mf->field_len >> 2)) return(-1);

	mf->u.ui32_a[index] = htonl(v);

	return(0);
}

int
msg_get_uint64_array_size(msg *m, int field_id, int *size)
{
	VALIDATE(m, field_id, M_FT_ARRAY_UINT64);

	msg_field *mf = &(m->f[field_id]);

	if (mf->is_set == false)	return(-1);
	*size = mf->field_len >> 3;

	return(0);
}

int
msg_get_uint64_array(msg *m, int field_id, const int index, uint64_t *r)
{
	VALIDATE(m, field_id, M_FT_ARRAY_UINT64);

	msg_field *mf = &(m->f[field_id]);

	if (mf->is_set == false) return(-1);
	*r = be64toh( mf->u.ui64_a[index] );

	return(0);
}

int
msg_set_uint64_array_size(msg *m, int field_id, const int size)
{
	VALIDATE(m, field_id, M_FT_ARRAY_UINT64);

	msg_field *mf = &(m->f[field_id]);

	mf->field_len = size * sizeof(uint64_t);
	if (mf->is_set == true) {

		mf->u.ui64_a = cf_realloc( mf->u.ui64_a, mf->field_len );
		if (! mf->u.ui64_a) return(-1);
	}
	else {
		mf->u.ui64_a = cf_malloc( mf->field_len );
		if (! mf->u.ui64_a) return(-1);
		mf->is_set = true;
	}
	mf->free = mf->u.ui64_a;

	return(0);
}

int
msg_set_uint64_array(msg *m, int field_id, const int index, const uint64_t v)
{
	VALIDATE(m, field_id, M_FT_ARRAY_UINT64);

	msg_field *mf = &(m->f[field_id]);

	if (mf->is_set == false)	return(-1);
	if (index >= (mf->field_len >> 3)) return(-1);

	mf->u.ui64_a[index] = htobe64(v);

	return(0);
}

msg_str_array *
msg_str_array_create(int n_strs, int total_len)
{
	int len = sizeof(msg_str_array) + n_strs * sizeof(uint32_t) + total_len;
	msg_str_array *str_a = cf_malloc(len);
	if (!str_a)	return(0);
	str_a->alloc_size = len;
	str_a->used_size = sizeof(msg_str_array) + (n_strs * sizeof(uint32_t));

	str_a->len = n_strs;
	for (int i = 0; i < n_strs ; i++)
		str_a->offset[i] = 0;

	return(str_a);
}

int
msg_str_array_set(msg_str_array *str_a, int idx, const char *v)
{
	if (idx >= str_a->len) {
		cf_info(CF_MSG, "msg_str_array: idx %u too large",idx);
		return(-1);
	}

	size_t len = strlen(v) + 1;
	if (str_a->used_size + len > str_a->alloc_size) {
		cf_info(CF_MSG, "todo: allow resize of outgoing str arrays");
		return(-1);
	}

	str_a->offset[idx] = str_a->used_size;
	char *data = ((char *) str_a) + str_a->offset[idx];
	strncpy(data, v, len);

	str_a->used_size += len;

	return(0);
}

int
msg_str_array_get(msg_str_array *str_a, int idx, char **r, size_t *len)
{
	if (idx > str_a->len) {
		cf_info(CF_MSG, "msg_str_array_get: idx %u too large",idx);
		return(-1);
	}
	if (str_a->offset[idx] == 0) {
		cf_info(CF_MSG, "msg_str_array: idx %u not set",idx);
		return(-2);
	}

	*r = ((char *) str_a) + str_a->offset[idx];
	*len = strlen(*r) + 1;

	return(0);
}

int
msg_get_str_array_size(msg *m, int field_id, int *size)
{
	VALIDATE(m, field_id, M_FT_ARRAY_STR);

	msg_field *mf = &(m->f[field_id]);
	if (mf->is_set == false) {
		cf_info(CF_MSG, "msg_str_array: field not set");
		return(-2);
	}

	if (mf->u.str_a == 0) {
		cf_info(CF_MSG, "no str array");
		return(-1);
	}

	msg_str_array *str_a = mf->u.str_a;

	*size = str_a->len;

	return(0);
}

int
msg_get_str_array(msg *m, int field_id, const int index, char **r, size_t *len, msg_get_type type)  // this length is strlen+1, the allocated size
{
	VALIDATE(m, field_id, M_FT_ARRAY_STR);

	msg_field *mf = &(m->f[field_id]);
	if (mf->is_set == false) {
		cf_info(CF_MSG, "msg_str_array: field not set");
		return(-2);
	}
	if (mf->u.str_a == 0) {
		cf_info(CF_MSG, "no str array");
		return(-1);
	}

	msg_str_array *str_a = mf->u.str_a;

	char *b;
	if (0 != msg_str_array_get(str_a, index, &b, len)) return(-1);

	switch (type) {
		case MSG_GET_DIRECT:
			*r = b;
			break;
		case MSG_GET_COPY_RC:
			*r = cf_rc_alloc(*len);
			memcpy(*r, b, *len);
			break;
		case MSG_GET_COPY_MALLOC:
			*r = cf_malloc(*len);
			memcpy(*r, b, *len);
			break;
	}

	return(0);
}

int
msg_get_str_len_array(msg *m, int field_id, const int index, size_t *len)  // this length is strlen+1, the allocated size
{
	VALIDATE(m, field_id, M_FT_ARRAY_STR);

	msg_field *mf = &(m->f[field_id]);
	if (mf->is_set == false) {
		cf_info(CF_MSG, "msg_str_array: field not set");
		*len = 0;
		return(-2);
	}
	if (mf->u.str_a == 0) {
		cf_info(CF_MSG, "no str array");
		return(-1);
	}
	if (len) {
		char *b = NULL;
		if (0 != msg_str_array_get(mf->u.str_a, index, &b, len)) return(-1);
		// (If found, value is thrown away, and length is set.)
	}

	return(0);
}

int
msg_set_str_array_size(msg *m, int field_id, const int size, const int total_len)
{
	VALIDATE(m, field_id, M_FT_ARRAY_STR);

	if ((size < 0) || (total_len < 0)) {
		cf_warning(CF_MSG, "Illegal size parameters");
		return(-1);
	}
	if ((size == 0) || (total_len == 0)) {
		// If it is zero, do not allocate memory.
		// Receiver will not process this field.
		return(0);
	}

	msg_field *mf = &(m->f[field_id]);
	if (mf->is_set == false) {
		msg_str_array *str_a = msg_str_array_create(size, total_len);
		if (str_a == 0) return(-1);
		mf->u.str_a = str_a;
		mf->free = str_a;
		mf->rc_free = 0;
		mf->field_len = str_a->used_size;

		mf->is_set = true;
	}
	else {
		cf_info(CF_MSG, "msg_str_array: does not support resize");
		return(-1);
	}

	return(0);
}

int
msg_set_str_array(msg *m, int field_id, const int index, const char *v)
{
	VALIDATE(m, field_id, M_FT_ARRAY_STR);

	msg_field *mf = &(m->f[field_id]);
	if (mf->is_set == false) {
		cf_warning(CF_MSG, "msg_str_array: must set length first");
		return(-1);
	}
	msg_str_array *str_a = mf->u.str_a;

	if (0 != msg_str_array_set(str_a, index, v)) return(-1);

	mf->field_len = str_a->used_size;

	return(0);
}

msg_buf_array *
msg_buf_array_create(int n_bufs, int buf_len)
{
	int len = sizeof(msg_buf_array) + n_bufs * (sizeof(uint32_t) + buf_len + sizeof(msg_pbuf));
	msg_buf_array *buf_a = cf_malloc( len );
	if (!buf_a)	return(0);
	buf_a->alloc_size = len;
	buf_a->used_size = sizeof(msg_buf_array) + (n_bufs * sizeof(uint32_t));

	buf_a->len = n_bufs;
	for (int i = 0; i < n_bufs ; i++)
		buf_a->offset[i] = 0;

	return(buf_a);
}

int
msg_buf_array_set(msg_buf_array *buf_a, int idx, const uint8_t *v, int len)
{
	if (idx >= buf_a->len) {
		cf_info(CF_MSG, "msg_buf_array: idx %u too large",idx);
		return(-1);
	}

	if (buf_a->used_size + len + sizeof(msg_pbuf) > buf_a->alloc_size) {
		cf_info(CF_MSG, "todo: allow resize of outgoing buf arrays");
		return(-1);
	}

	buf_a->offset[idx] = buf_a->used_size;
	msg_pbuf *pbuf = (msg_pbuf *) (((uint8_t *)buf_a) + buf_a->used_size);
	pbuf->len = len;
	memcpy(pbuf->data, v, len);

	buf_a->used_size += len + sizeof(msg_pbuf);

	return(0);
}

int
msg_buf_array_get(msg_buf_array *buf_a, int idx, uint8_t **r, size_t *len)
{
	if (idx > buf_a->len) {
		cf_info(CF_MSG, "msg_buf_array_get: idx %u too large",idx);
		return(-1);
	}
	if (buf_a->offset[idx] == 0) {
		cf_info(CF_MSG, "msg_buf_array: idx %u not set",idx);
		return(-2);
	}

	msg_pbuf *pbuf = (msg_pbuf *) (((uint8_t *) buf_a) + buf_a->offset[idx]);

	*len = pbuf->len;
	*r = pbuf->data;

	return(0);
}

int
msg_get_buf_array_size(msg *m, int field_id, int *size)
{
	VALIDATE(m, field_id, M_FT_ARRAY_BUF);

	msg_field *mf = &(m->f[field_id]);
	if (mf->is_set == false) {
		cf_info(CF_MSG, "msg_buf_array: field not set");
		return(-2);
	}

	if (mf->u.buf_a == 0) {
		cf_info(CF_MSG, "no buf array");
		return(-1);
	}

	msg_buf_array *buf_a = mf->u.buf_a;

	*size = buf_a->len;

	return(0);
}

int
msg_get_buf_array(msg *m, int field_id, const int index, uint8_t **r, size_t *len, msg_get_type type)
{
	VALIDATE(m, field_id, M_FT_ARRAY_BUF);

	msg_field *mf = &(m->f[field_id]);
	if (mf->is_set == false) {
		cf_info(CF_MSG, "msg_buf_array: field not set");
		return(-2);
	}
	if (mf->u.buf_a == 0) {
		cf_info(CF_MSG, "no buf array");
		return(-1);
	}

	msg_buf_array *buf_a = mf->u.buf_a;

	uint8_t *b;
	if (0 != msg_buf_array_get(buf_a, index, &b, len))	return(-1);

	switch (type) {
		case MSG_GET_DIRECT:
			*r = b;
			break;
		case MSG_GET_COPY_RC:
			*r = cf_rc_alloc(*len);
			memcpy(*r, b, *len);
			break;
		case MSG_GET_COPY_MALLOC:
			*r = cf_malloc(*len);
			memcpy(*r, b, *len);
			break;
	}

	return(0);
}

int
msg_set_buf_array_size(msg *m, int field_id, const int size, const int elem_size)
{
	VALIDATE(m, field_id, M_FT_ARRAY_BUF);

	if ((size <= 0) || (elem_size <= 0)) {
		cf_warning(CF_MSG, "Illegal size parameters");
		return(-1);
	}

	msg_field *mf = &(m->f[field_id]);
	if (mf->is_set == false)	{
		msg_buf_array *buf_a = msg_buf_array_create(size, elem_size);
		if (buf_a == 0)	return(-1);
		mf->u.buf_a = buf_a;
		mf->free = buf_a;
		mf->rc_free = 0;
		mf->field_len = buf_a->used_size;

		mf->is_set = true;
	}
	else {
		cf_info(CF_MSG, "msg_buf_array: does not support resize");
		return(-1);
	}

	return(0);
}

int
msg_set_buf_array(msg *m, int field_id, const int index, const uint8_t *v, size_t len)
{
	VALIDATE(m, field_id, M_FT_ARRAY_BUF);

	msg_field *mf = &(m->f[field_id]);
	if (mf->is_set == false) {
		cf_warning(CF_MSG, "msg_buf_array: must set length first");
		return(-1);
	}
	msg_buf_array *buf_a = mf->u.buf_a;

	if (0 != msg_buf_array_set(buf_a, index, v, len )) return(-1);

	mf->field_len = buf_a->used_size;

	return(0);
}



//
// There are some cases, like reusing a message, where you have a set field and
// you'd like to unset just that field
//

void msg_set_unset(msg *m, int field_id)
{
#ifdef EXTRA_CHECKS
	if (! m->f[field_id].is_valid) {
		cf_crash(CF_MSG, "msg: invalid id %d in field set",field_id);
		return; // not sure the meaning of ERROR - will it throw or not?
	}
#endif
	msg_field *mf = &m->f[field_id];

	if (mf->is_set == false)	return;

	switch (mf->type) {
		case M_FT_BUF:
		case M_FT_STR:
			if (mf->free)	cf_free(mf->free);
			if (mf->rc_free) cf_rc_releaseandfree(mf->rc_free);
			break;
		default:
			break;
	}

	mf->is_set = false;
}

bool msg_isset(msg *m, int field_id)
{
#ifdef EXTRA_CHECKS
	if (! m->f[field_id].is_valid) {
		cf_crash(CF_MSG, "msg: invalid id %d in field set",field_id);
	}
#endif
	return(m->f[field_id].is_set);
}


int msg_set_bytearray(msg *m, int field_id, const cf_bytearray *v)
{
	VALIDATE(m, field_id, M_FT_BUF);

	msg_field *mf = &(m->f[field_id]);

	// free old value if necessary
	if (mf->is_set) {
		if (mf->free) {	cf_free(mf->free); mf->free = 0; }
		if (mf->rc_free) { cf_rc_releaseandfree(mf->rc_free); mf->rc_free = 0; }
	}

	mf->field_len = v->sz;
	mf->u.buf = (void *) v->data;
	mf->rc_free = (void *) v;
	mf->free = 0;

	mf->is_set = true;

	return(0);
}



// very useful for test code! one can encode and decode messages and see if
// they're the same!

int
msg_compare(const msg *m1, const msg *m2) {
	cf_debug(CF_MSG,"msg_compare: stub");
	return(-2);
}

// Free up a "msg" object
//  Call this function instead of freeing the msg directly in order to keep track of all msgs.
void msg_put(msg *m)
{
	cf_debug(CF_MSG, "freeing msg %p type %d", m, m->type);

	cf_atomic_int_decr(&g_num_msgs);
	cf_atomic_int_decr(&(g_num_msgs_by_type[m->type]));
	cf_rc_free(m);
}

// And, finally, the destruction of a message
void msg_destroy(msg *m)
{
	if (0 == cf_rc_release(m)) {
		for (int i=0;i<m->len;i++) {
			if ((m->f[i].is_valid == true) && (m->f[i].is_set == true)) {

				if (m->f[i].free) cf_free(m->f[i].free);
				if (m->f[i].rc_free) cf_rc_releaseandfree(m->f[i].free);
			}
		}
		msg_put(m);
	}
	return;
}

//
// Debug routine to dump all the information about a msg to stdout
//

void
msg_dump(const msg *m, const char *info)
{
	cf_info(CF_MSG, "msg_dump: %s: msg %p rc %d flen %d bytes-used %u bytes-alloc'd %u type %d  mt %p",
			info, m, (int)cf_rc_count((void*)m), m->len, m->bytes_used,
			m->bytes_alloc, m->type, m->mt);

	for (int i = 0; i < m->len; i++) {
		const msg_field *mf =  &m->f[i];

		cf_info(CF_MSG, "mf %02d: id %u is-valid %d is-set %d",
				i, mf->id, mf->is_valid, mf->is_set);

		if (mf->is_valid && mf->is_set) {
			switch(mf->type) {
			case M_FT_INT32:
				cf_info(CF_MSG, "   type INT32 value %d", mf->u.i32);
				break;
			case M_FT_UINT32:
				cf_info(CF_MSG, "   type UINT32 value %u", mf->u.ui32);
				break;
			case M_FT_INT64:
				cf_info(CF_MSG, "   type INT64 value %ld", mf->u.i64);
				break;
			case M_FT_UINT64:
				cf_info(CF_MSG, "   type UINT64 value %lu", mf->u.ui64);
				break;
			case M_FT_STR:
				cf_info(CF_MSG, "   type STR len %u free %p value %s",
						mf->field_len, mf->free, mf->u.str);
				break;
			case M_FT_BUF:
				cf_info_binary(CF_MSG, mf->u.buf, mf->field_len,
						CF_DISPLAY_HEX_COLUMNS,
						"   type BUF len %u free %p value ",
						mf->field_len, mf->free);
				break;
			case M_FT_ARRAY_UINT32:
				cf_info(CF_MSG, "   type ARRAY_UINT32: len %u n-uint32 %u free %p",
						mf->field_len, mf->field_len >> 2, mf->free);
				{
					int n_ints = mf->field_len >> 2;
					for (int j = 0; j < n_ints; j++) {
						cf_info(CF_MSG, "      idx %d value %u",
								j, ntohl(mf->u.ui32_a[j]));
					}
				}
				break;
			case M_FT_ARRAY_UINT64:
				cf_info(CF_MSG, "   type ARRAY_UINT64: len %u n-uint64 %u free %p",
						mf->field_len, mf->field_len >> 3, mf->free);
				{
					int n_ints = mf->field_len >> 3;
					for (int j = 0; j < n_ints; j++) {
						cf_info(CF_MSG, "      idx %d value %lu",
								j, be64toh(mf->u.ui64_a[j]));
					}
				}
				break;
			default:
				cf_info(CF_MSG, "   type %d unknown", mf->type);
				break;
			}
		}
	}
}
