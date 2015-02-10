/*
 * geospatial.c
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

#include <errno.h>
#include <limits.h>
#include <string.h>

#include "base/geospatial.h"

bool
geo_map_point(const char * buf, size_t buf_sz, uint64_t * cellidp)
{
    // FIXME - need to actually parse the JSON.

    // FIXME - Need to sensibly report parsing errors with cf_warning(AS_GEO, ...)

    // Aerospike 37.421282, -122.098808
    *cellidp = 0x808fba11c5ceb39fULL;

    return true;
}

bool geo_region_parse(const char * buf, size_t bufsz, geo_region_t * regionp)
{
	// FIXME - need to actually parse the JSON and return a region.

	// FIXME - Need to sensibly report parsing errors with cf_warning(AS_GEO, ...)

	*regionp = (geo_region_t) 42;
	return true;
}

bool geo_region_cover(geo_region_t region,
					  int maxnumcells,
					  uint64_t * cellminp,
					  uint64_t * cellmaxp,
					  int * numcellsp)
{
	// FIXME - need to actually cover the region.

	size_t ndx0 = 0;
	size_t ndx1 = 0;

	cellminp[ndx0++] = 0x8085400000000001ULL; cellmaxp[ndx1++] = 0x80855fffffffffffULL;
	cellminp[ndx0++] = 0x8085600000000001ULL; cellmaxp[ndx1++] = 0x80857fffffffffffULL;
	cellminp[ndx0++] = 0x8085800000000001ULL; cellmaxp[ndx1++] = 0x80859fffffffffffULL;
	cellminp[ndx0++] = 0x8085a00000000001ULL; cellmaxp[ndx1++] = 0x8085a7ffffffffffULL;
	cellminp[ndx0++] = 0x808e000000000001ULL; cellmaxp[ndx1++] = 0x808e7fffffffffffULL;
	cellminp[ndx0++] = 0x808ee00000000001ULL; cellmaxp[ndx1++] = 0x808effffffffffffULL;
	cellminp[ndx0++] = 0x808f000000000001ULL; cellmaxp[ndx1++] = 0x808f1fffffffffffULL;
	cellminp[ndx0++] = 0x808f600000000001ULL; cellmaxp[ndx1++] = 0x808f7fffffffffffULL;
	cellminp[ndx0++] = 0x808f800000000001ULL; cellmaxp[ndx1++] = 0x808fffffffffffffULL;
	cellminp[ndx0++] = 0x8090000000000001ULL; cellmaxp[ndx1++] = 0x80907fffffffffffULL;
	cellminp[ndx0++] = 0x8091800000000001ULL; cellmaxp[ndx1++] = 0x8091ffffffffffffULL;
	cellminp[ndx0++] = 0x809aa00000000001ULL; cellmaxp[ndx1++] = 0x809abfffffffffffULL;

	*numcellsp = ndx0;

	return true;
}

bool geo_region_contains(geo_region_t region,
						 const byte * buf,
						 size_t bufsz)
{
	// FIXME - plug in actual region checking code here.
	return true;
}

void geo_region_destroy(geo_region_t region)
{
	// FIXME - need to deallocate the region here.
}

// Local Variables:
// mode: C
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim: tabstop=4:shiftwidth=4
