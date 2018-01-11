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
#pragma once

#include <aerospike/as_aerospike.h>

 /*****************************************************************************
  * GLOBALS
  *****************************************************************************/

extern as_udf_context ctx;

#if !defined(_MSC_VER)
#define AS_START_DIR "./"
#else
#define AS_START_DIR "../../"
#endif

 /*****************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_aerospike* 
test_aerospike_new();

as_aerospike*
test_aerospike_init(as_aerospike *);

bool
test_suite_before(atf_suite* suite);

bool
test_suite_after(atf_suite* suite);
