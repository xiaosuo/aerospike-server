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

extern bool geo_parse_json(const byte * buf, size_t buf_sz, uint64_t * cellid);

// Local Variables:
// mode: C
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim: tabstop=4:shiftwidth=4
