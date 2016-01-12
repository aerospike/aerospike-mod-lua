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

#include "../test.h"
#include <aerospike/as_stream.h>
#include <aerospike/as_types.h>
#include <limits.h>
#include <stdlib.h>

#include <aerospike/as_module.h>
#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>

#include "../util/test_aerospike.h"
#include "../util/test_logger.h"
#include "../util/map_rec.h"

/******************************************************************************
 * VARIABLES
 *****************************************************************************/

static as_aerospike as;
static as_udf_context ctx;

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( record_udf_1, "echo bin a of {a = 123, b = 456 }" )
{
    as_rec * rec = map_rec_new();
    as_rec_set(rec, "a", (as_val *) as_integer_new(123));

	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

    as_arraylist arglist;
    as_arraylist_inita(&arglist, 1);
    as_arraylist_append_str(&arglist, "a");

    as_result * res = as_success_new(NULL);

    int rc = as_module_apply_record(&mod_lua, &ctx, "records", "getbin", rec, (as_list *) &arglist, res);

    assert_int_eq( rc, 0 );
    assert_true( res->is_success );
    assert_not_null( res->value );
    assert_int_eq( as_integer_toint((as_integer *) res->value), 123 );

    as_rec_destroy(rec);
    as_arraylist_destroy(&arglist);
    as_result_destroy(res);
}

TEST( record_udf_2, "concat bins a and b of {a = 'abc', b = 'def' }" )
{
    as_rec * rec = map_rec_new();
    as_rec_set(rec, "a", (as_val *) as_string_new("abc",false));
    as_rec_set(rec, "b", (as_val *) as_string_new("def",false));

	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

    as_arraylist arglist;
    as_arraylist_inita(&arglist, 2);
    as_arraylist_append_str(&arglist, "a");
    as_arraylist_append_str(&arglist, "b");

    as_result * res = as_success_new(NULL);

    int rc = as_module_apply_record(&mod_lua, &ctx, "records", "cat", rec, (as_list *) &arglist, res);

    assert_int_eq( rc, 0);
    assert_true( res->is_success );
    assert_not_null( res->value );
    assert_string_eq( as_string_tostring((as_string *)res->value), "abcdef");

    as_rec_destroy(rec);
    as_arraylist_destroy(&arglist);
    as_result_destroy(res);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite)
{

    test_aerospike_init(&as);
	ctx.as = &as;

    mod_lua_config config = {
        .server_mode    = true,
        .cache_enabled  = false,
        .system_path    = {'\0'},
        .user_path      = "src/test/lua"
    };

    char * system_path = getenv("AS_SYSTEM_LUA");
    if ( system_path != NULL ) {
	    strncpy(config.system_path, system_path, 255);
	    config.system_path[255] = '\0';
    }
    else {
    	error("environment variable 'AS_SYSTEM_LUA' should be set to point to the directory containing system lua files.")
    	return false;
    }

	as_lua_log_init();
    
    int rc = as_module_configure(&mod_lua, &config);

    if ( rc != 0 ) {
        error("as_module_configure failed: %d", rc);
        return false;
    }
 
    return true;
}

static bool after(atf_suite * suite)
{
    return true;
}

SUITE( record_udf, "stream udf tests" )
{
    suite_before( before );
    suite_after( after );
    
    suite_add( record_udf_1 );
    suite_add( record_udf_2 );
}
