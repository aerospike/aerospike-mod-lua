/*
 * Copyright 2008-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#include "test_logger.h"
#include <aerospike/as_log.h>
#include "../test.h"

static bool
as_lua_log_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	atf_log_line(stderr, as_log_level_tostring(level), ATF_LOG_PREFIX, NULL, 0, fmt, ap);
	va_end(ap);
	return true;
}

void
as_lua_log_init()
{
	as_log_set_level(AS_LOG_LEVEL_INFO);
	as_log_set_callback(as_lua_log_callback);
}
