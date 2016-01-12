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
#include "../util/producer_stream.h"
#include "../util/consumer_stream.h"

/******************************************************************************
 * VARIABLES
 *****************************************************************************/

static as_aerospike as;
static as_udf_context auctx =
{
	.as = &as,
	.timer = NULL,
	.memtracker = NULL
};

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

static uint32_t limit = 0;
static uint32_t produced = 0;
static uint32_t consumed = 0;

static as_val * produce1()
{
	if ( produced >= limit ) {
		return AS_STREAM_END;
	}

	produced++;
	
	return (as_val *) as_integer_new(produced);
}

static as_stream_status consume1(as_val * v)
{
	if ( v != AS_STREAM_END ) {
		consumed++;
	}
	
	as_val_destroy(v);

	return AS_STREAM_OK;
}

TEST( stream_udf_1, "filter even numbers from range (1-10)" )
{
 
	limit = 10;
	produced = 0;
	consumed = 0;
    
    as_stream * istream = producer_stream_new(produce1);
    as_stream * ostream = consumer_stream_new(consume1);
    as_list *   arglist = NULL;

    int rc = as_module_apply_stream(&mod_lua, &auctx, "aggr", "even", istream, arglist, ostream,NULL);

    assert_int_eq( rc, 0);
    assert_int_eq( produced, limit);
    assert_int_eq( consumed, produced / 2);

    as_stream_destroy(istream);
    as_stream_destroy(ostream);
}

TEST( stream_udf_2, "increment range (1-10)" )
{

	limit = 10;
	produced = 0;
	consumed = 0;
    
    as_stream * istream = producer_stream_new(produce1);
    as_stream * ostream = consumer_stream_new(consume1);
    as_list *   arglist = NULL;

    int rc = as_module_apply_stream(&mod_lua, &auctx, "aggr", "increment", istream, arglist, ostream,NULL);

    assert_int_eq( rc, 0);
    assert_int_eq( produced, limit);
    assert_int_eq( consumed, produced);

    as_stream_destroy(istream);
    as_stream_destroy(ostream);
}

static as_integer * result3 = NULL;

static as_val * produce3()
{
	if ( produced >= limit ) {
		return AS_STREAM_END;
	}
	
	produced++;

	return (as_val *) as_integer_new(produced);
}

static as_stream_status consume3(as_val * v) 
{
	if ( v != AS_STREAM_END ) {
		consumed++;
		result3 = (as_integer *) v;
	}

	return AS_STREAM_OK;
}

TEST( stream_udf_3, "sum range (1-1,000,000)" )
{
	limit = 1000 * 1000;
	produced = 0;
	consumed = 0;

	result3 = NULL;
    
    as_stream * istream = producer_stream_new(produce3);
    as_stream * ostream = consumer_stream_new(consume3);
    as_list *   arglist = NULL;

    int rc = as_module_apply_stream(&mod_lua, &auctx, "aggr", "sum", istream, arglist, ostream,NULL);

    uint64_t result = (uint64_t) as_integer_get(result3);

    assert_int_eq( rc, 0);
    assert_int_eq( produced, limit);
    assert_int_eq( consumed, 1);
    assert_int_eq( result, 500000500000);

    as_integer_destroy(result3);
    as_stream_destroy(istream);
    as_stream_destroy(ostream);
}

TEST( stream_udf_4, "product range (1-10)" ) 
{
 
	limit = 10;
	produced = 0;
	consumed = 0;

    result3 = NULL;
    
    as_stream * istream = producer_stream_new(produce3);
    as_stream * ostream = consumer_stream_new(consume3);
    as_list *   arglist = NULL;

    int rc = as_module_apply_stream(&mod_lua, &auctx, "aggr", "product", istream, arglist, ostream,NULL);

    assert_int_eq( rc, 0);
    assert_int_eq( produced, limit);
    assert_int_eq( consumed, 1);
    assert_int_eq( as_integer_get(result3), 3628800);

    as_integer_destroy(result3);
    as_stream_destroy(istream);
    as_stream_destroy(ostream);
}

static as_map * result5 = NULL;

static as_val * produce5()
{
	if ( produced >= limit ) {
		return AS_STREAM_END;
	}

	produced++;
	
	as_rec * rec = map_rec_new();
	as_rec_set(rec, "id", (as_val *) as_integer_new(produced));
	as_rec_set(rec, "campaign", (as_val *) as_integer_new(produced % 10));
	as_rec_set(rec, "views", (as_val *) as_integer_new(produced * 2919 % 1000));
	
	return (as_val *) rec;
}

static as_stream_status consume5(as_val * v)
{
	if ( v != AS_STREAM_END ) {
		consumed++;
		result5 = (as_map *) v;
	}

	return AS_STREAM_OK;
}

TEST( stream_udf_5, "campaign rollup w/ map & reduce" )
{
	limit = 100;
	produced = 0;
	consumed = 0;

	result5 = NULL;
    
    as_stream * istream = producer_stream_new(produce5);
    as_stream * ostream = consumer_stream_new(consume5);
    as_list *   arglist = (as_list *) as_arraylist_new(0,0);

    int rc = as_module_apply_stream(&mod_lua, &auctx, "aggr", "rollup", istream, arglist, ostream,NULL);

    assert_int_eq( rc, 0);
    assert_int_eq( produced, limit);
    assert_int_eq( consumed, 1);

    as_integer i;

    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 0))), 5450);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 1))), 4740);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 2))), 4930);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 3))), 5120);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 4))), 4310);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 5))), 5500);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 6))), 4690);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 7))), 4880);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 8))), 5070);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 9))), 5260);


	as_list_destroy(arglist);
    as_map_destroy(result5);
    as_stream_destroy(istream);
    as_stream_destroy(ostream);
}


TEST( stream_udf_6, "campaign rollup w/ aggregate" )
{
    limit = 100;
    produced = 0;
    consumed = 0;

    result5 = NULL;

    as_stream * istream = producer_stream_new(produce5);
    as_stream * ostream = consumer_stream_new(consume5);
    as_list *   arglist = (as_list *) as_arraylist_new(0,0);

    int rc = as_module_apply_stream(&mod_lua, &auctx, "aggr", "rollup2", istream, arglist, ostream,NULL);

    assert_int_eq( rc, 0);
    assert_int_eq( produced, limit);
    assert_int_eq( consumed, 1);

    as_integer i;

    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 0))), 5450);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 1))), 4740);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 2))), 4930);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 3))), 5120);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 4))), 4310);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 5))), 5500);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 6))), 4690);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 7))), 4880);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 8))), 5070);
    assert_int_eq(as_integer_get((as_integer *) as_map_get(result5, (as_val *) as_integer_init(&i, 9))), 5260);

	as_list_destroy(arglist);
    as_map_destroy(result5);
    as_stream_destroy(istream);
    as_stream_destroy(ostream);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/


static bool before(atf_suite * suite)
{
    
    test_aerospike_init(&as);

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

SUITE( stream_udf, "stream udf tests" )
{
    suite_before( before );
    suite_after( after );
    
    suite_add( stream_udf_1 );
    suite_add( stream_udf_2 );
    suite_add( stream_udf_3 );
    suite_add( stream_udf_4 );
    suite_add( stream_udf_5 );
    suite_add( stream_udf_6 );
}
