/*
 * geospatial.h
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
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */

#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#include "citrusleaf/cf_types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void * geo_region_t;

extern bool geo_map_point(const char * buf, size_t bufsz, uint64_t * cellid);

extern bool geo_region_parse(const char * buf,
							 size_t bufsz,
							 geo_region_t * regionp);

extern bool geo_region_cover(geo_region_t region,
							 int maxnumcells,
							 uint64_t * cellminp,
							 uint64_t * cellmaxp,
							 int * numcellsp);

extern bool geo_region_contains(geo_region_t region,
								const char * buf,
								size_t bufsz);

extern void geo_region_destroy(geo_region_t region);

#ifdef __cplusplus
} // end extern "C"
#endif

// Local Variables:
// mode: C++
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim: tabstop=4:shiftwidth=4
