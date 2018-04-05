/*
 * Copyright 2008-2018 Aerospike, Inc.
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
#include <citrusleaf/cf_clock.h>
#include "test.h"

static bool
before(atf_plan* plan)
{
	return cf_clock_init();
}

PLAN(mod_lua_test)
{
	plan_before(before);

	plan_add(hash_udf);
	plan_add(list_udf);
	plan_add(record_udf);
	plan_add(stream_udf);
	plan_add(validation_basics);
}
