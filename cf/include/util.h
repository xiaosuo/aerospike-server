/*
 * util.h
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

#pragma once

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <citrusleaf/cf_digest.h>

// TODO - as_ .c files depend on this:
#include <asm/byteorder.h>


/* cf_hash_fnv
 * The 64-bit Fowler-Noll-Vo hash function (FNV-1a) */
static inline uint64_t
cf_hash_fnv(void *buf, size_t bufsz)
{
    uint64_t hash = 0xcbf29ce484222325ULL;
    uint8_t *bufp = (uint8_t *) buf;
    uint8_t *bufe = bufp + bufsz;

    while (bufp < bufe) {
        /* XOR the current byte into the bottom of the hash */
        hash ^= (uint64_t)*bufp++;

        /* Multiply by the 64-bit FNV magic prime */
        hash *= 0x100000001b3ULL;
    }

    return(hash);
}


/* cf_hash_oneatatime
 * The 64-bit One-at-a-Time hash function */
static inline uint64_t
cf_hash_oneatatime(void *buf, size_t bufsz)
{
    size_t i;
    uint64_t hash = 0;
    uint8_t *b = (uint8_t *)buf;

    for (i = 0; i < bufsz; i++) {
        hash += b[i];
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);

    return(hash);
}


/* cf_swap64
 * Swap a 64-bit value
 * --- there's a fair amount of commentary on the web as to whether this kind
 * of swap optimization is useful for superscalar architectures.  Leave it in
 * for now */
#define cf_swap64(a, b) (void)(((a) == (b)) || (((a) ^= (b)), ((b) ^= (a)), ((a) ^= (b))))
// #define cf_swap64(a, b) { uint64_t __t; __t = a; a = b; b = __t; }

/* cf_contains64
 * See if a vector of uint64s contains an certain value */
static inline int
cf_contains64(uint64_t *v, int vsz, uint64_t a)
{
    for (int i = 0; i < vsz; i++) {
        if (v[i] == a)
            return(1);
    }

    return(0);
}

/* cf_compare_ptr
 * Compare the first sz bytes from two regions referenced by pointers */
static inline int
cf_compare_ptr(const void *a, const void *b, ssize_t sz)
{
    return(memcmp(a, b, sz));
}

/* cf_compare_uint64ptr
 * Compare two integers */
static inline int
cf_compare_uint64ptr(const void *pa, const void *pb)
{
    int r;
    const uint64_t *a = pa, *b = pb;

    if (*a == *b)
        return(0);

    r = (*a > *b) ? -1 : 1;

    return(r);
}


/* cf_compare_byteptr
 * Compare the regions pointed to by two sized byte pointers */
static inline int
cf_compare_byteptr(const void *pa, const size_t asz, const void *pb, const size_t bsz)
{
    if (asz != bsz)
        return(asz - bsz);

    return(memcmp(pa, pb, asz));
}

extern cf_digest cf_digest_zero;

// This one compare is probably the hottest line in the system.
// Thus, please benchmark with these different compare functions and
// see which is faster/better
#if 0
static inline int
cf_digest_compare( cf_digest *d1, cf_digest *d2 )
{
    if (d1->digest[0] != d2->digest[0]) {
        if (d1->digest[0] < d2->digest[0])
            return(-1);
        else
            return(1);
    }
    return( memcmp( d1, d2, sizeof(cf_digest) ) );
}
#endif

#if 1
static inline int
cf_digest_compare( cf_digest *d1, cf_digest *d2 )
{
    return( memcmp( d1->digest, d2->digest, CF_DIGEST_KEY_SZ) );
}
#endif

// Sorry, too lazy to create a whole new file for just one function
#define CF_NODE_UNSET (0xFFFFFFFFFFFFFFFF)
typedef uint64_t cf_node;
extern uint32_t cf_nodeid_shash_fn(void *value);
extern uint32_t cf_nodeid_rchash_fn(void *value, uint32_t value_len);
typedef enum hb_mode_enum { AS_HB_MODE_UNDEF, AS_HB_MODE_MCAST, AS_HB_MODE_MESH } hb_mode_enum;
typedef enum hb_protocol_enum { AS_HB_PROTOCOL_UNDEF, AS_HB_PROTOCOL_NONE, AS_HB_PROTOCOL_V1, AS_HB_PROTOCOL_V2, AS_HB_PROTOCOL_RESET } hb_protocol_enum;
typedef enum paxos_protocol_enum { AS_PAXOS_PROTOCOL_UNDEF, AS_PAXOS_PROTOCOL_NONE, AS_PAXOS_PROTOCOL_V1, AS_PAXOS_PROTOCOL_V2, AS_PAXOS_PROTOCOL_V3, AS_PAXOS_PROTOCOL_V4 } paxos_protocol_enum;
typedef enum paxos_recovery_policy_enum { AS_PAXOS_RECOVERY_POLICY_UNDEF, AS_PAXOS_RECOVERY_POLICY_MANUAL, AS_PAXOS_RECOVERY_POLICY_AUTO_DUN_MASTER, AS_PAXOS_RECOVERY_POLICY_AUTO_DUN_ALL } paxos_recovery_policy_enum;
extern int cf_nodeid_get( unsigned short port, cf_node *id, char **node_ipp, hb_mode_enum hb_mode, char **hb_addrp, const char **interface_names);
extern unsigned short cf_nodeid_get_port(cf_node id);

extern int cf_sort_firstk(uint64_t *v, size_t sz, int k);

extern void cf_process_daemonize(int *fd_ignore_list, int list_size);

/* daemon.c */
extern void cf_process_privsep(uid_t uid, gid_t gid);
