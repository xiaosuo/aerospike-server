/*
 * geospatial.cpp
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

#include <stdexcept>

#include <s2regioncoverer.h>

extern "C" {
#include "fault.h"
} // end extern "C"

#include "geospatial/geospatial.h"
#include "geospatial/geojson.h"

using namespace std;

class PointHandler : public GeoJSON::GeometryHandler
{
public:
	virtual void handle_point(S2CellId const & cellid) {
		m_cellid = cellid;
	}
	S2CellId	m_cellid;
};

bool
geo_map_point(const char * buf, size_t bufsz, uint64_t * cellidp)
{
	try
	{
		PointHandler phandler;
		GeoJSON::parse(phandler, string(buf, bufsz));
		*cellidp = phandler.m_cellid.id();
		cf_detail(AS_GEO, "cell 0x%llx", *cellidp);
		return true;
	}
	catch (exception const & ex)
	{
		cf_warning(AS_GEO, (char *) "failed to parse point: %s", ex.what());
		return false;
	}
}

class RegionHandler: public GeoJSON::GeometryHandler
{
public:
	virtual bool handle_region(S2Region * regionp) {
		m_regionp = regionp;
		return false;	// Don't delete this region, please.
	}
	S2Region * m_regionp;
};

bool geo_region_parse(const char * buf, size_t bufsz, geo_region_t * regionp)
{
	try
	{
		RegionHandler rhandler;
		GeoJSON::parse(rhandler, string(buf, bufsz));
		*regionp = (geo_region_t) rhandler.m_regionp;
		return true;
	}
	catch (exception const & ex)
	{
		cf_warning(AS_GEO, (char *) "failed to parse point: %s", ex.what());
		return false;
	}
}

bool geo_region_cover(geo_region_t region,
					  int maxnumcells,
					  uint64_t * cellminp,
					  uint64_t * cellmaxp,
					  int * numcellsp)
{
	S2Region * regionp = (S2Region *) region;

    S2RegionCoverer coverer;
    coverer.set_min_level(1);
    coverer.set_max_level(30);
    coverer.set_max_cells(12);
    coverer.set_level_mod(1);
    vector<S2CellId> covering;
    coverer.GetCovering(*regionp, &covering);
	for (size_t ii = 0; ii < covering.size(); ++ii)
	{
		if (ii == maxnumcells)
		{
			cf_warning(AS_GEO,
					   (char *) "region covered with %d cells, only %d allowed",
					   covering.size(), maxnumcells);
			return false;
		}

		cellminp[ii] = covering[ii].range_min().id();
		cellmaxp[ii] = covering[ii].range_max().id();

		cf_detail(AS_GEO, "cell[%02d]: [0x%llx, 0x%llx]",
				  ii, cellminp[ii], cellmaxp[ii]);
	}

	*numcellsp = covering.size();
	return true;
}

bool geo_region_contains(geo_region_t region,
						 const char * buf,
						 size_t bufsz)
{
	S2Region * regionp = (S2Region *) region;

	try
	{
		PointHandler phandler;
		GeoJSON::parse(phandler, string(buf, bufsz));
		return regionp->VirtualContainsPoint(phandler.m_cellid.ToPoint());
	}
	catch (exception const & ex)
	{
		cf_warning(AS_GEO, (char *) "failed to parse point: %s", ex.what());
		return false;
	}
}

void geo_region_destroy(geo_region_t region)
{
	S2Region * regionp = (S2Region *) region;
	if (regionp)
		delete regionp;
}

// Local Variables:
// mode: C++
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim: tabstop=4:shiftwidth=4
