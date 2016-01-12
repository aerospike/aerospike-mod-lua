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

#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/as_module.h>
#include <aerospike/as_types.h>
#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>

#include "../test.h"
#include "../util/map_rec.h"
#include "../util/test_aerospike.h"
#include "../util/test_logger.h"

/******************************************************************************
 * VARIABLES
 *****************************************************************************/

static as_aerospike as;
static as_udf_context ctx;

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( list_udf_1, "create a list" )
{
	as_rec * rec = map_rec_new();
	
	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 5);
	as_arraylist_append_int64(&arglist, 1);
	as_arraylist_append_int64(&arglist, 2);
	as_arraylist_append_int64(&arglist, 3);
	as_arraylist_append_int64(&arglist, 4);
	as_arraylist_append_double(&arglist, 5.55);

	as_result * res = as_success_new(NULL);

	int rc = as_module_apply_record(&mod_lua, &ctx, "lists", "create", rec, (as_list *) &arglist, res);

	assert_int_eq( rc, 0 );
	assert_true( res->is_success );
	assert_not_null( res->value );
	as_list * rlist = (as_list *) res->value;
	assert_int_eq( as_list_size(rlist), 5 );
	assert_int_eq( as_list_get_int64(rlist,0), 1 );
	assert_int_eq( as_list_get_int64(rlist,1), 2 );
	assert_int_eq( as_list_get_int64(rlist,2), 3 );
	assert_int_eq( as_list_get_int64(rlist,3), 4 );
	assert_double_eq( as_list_get_double(rlist,4), 5.55 );

	as_rec_destroy(rec);
	as_arraylist_destroy(&arglist);
	as_result_destroy(res);
}

TEST( list_udf_2, "get the size of a list" )
{
	as_arraylist list;
	as_arraylist_init(&list, 5, 5);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);
	as_arraylist_append_int64(&list, 4);
	as_arraylist_append_double(&list, 5.55);

	as_rec * rec = map_rec_new();
	as_rec_set(rec, "listbin", (as_val *) &list);
	
	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_str(&arglist, "listbin");

	as_result * res = as_success_new(NULL);

	int rc = as_module_apply_record(&mod_lua, &ctx, "lists", "size", rec, (as_list *) &arglist, res);

	assert_int_eq( rc, 0);
	assert_true( res->is_success );
	assert_not_null( res->value );
	assert_int_eq( as_integer_toint((as_integer *) res->value), 5 );

	as_rec_destroy(rec);
	as_arraylist_destroy(&list);
	as_arraylist_destroy(&arglist);
	as_result_destroy(res);
}

TEST( list_udf_3, "iterate over a list" )
{
	as_arraylist list;
	as_arraylist_init(&list, 5, 5);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);
	as_arraylist_append_int64(&list, 4);
	as_arraylist_append_double(&list, 5.55);

	as_rec * rec = map_rec_new();
	as_rec_set(rec, "listbin", (as_val *) &list);

	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_str(&arglist, "listbin");

	as_result * res = as_success_new(NULL);

	int rc = as_module_apply_record(&mod_lua, &ctx, "lists", "iterate", rec, (as_list *) &arglist, res);

	assert_int_eq( rc, 0);
	assert_true( res->is_success );
	assert_not_null( res->value );
	as_list * rlist = (as_list *) res->value; // returned list should look just like the input one
	assert_int_eq( as_list_size(rlist), 5 );
	assert_int_eq( as_list_get_int64(rlist,0), 1 );
	assert_int_eq( as_list_get_int64(rlist,1), 2 );
	assert_int_eq( as_list_get_int64(rlist,2), 3 );
	assert_int_eq( as_list_get_int64(rlist,3), 4 );
	assert_double_eq( as_list_get_double(rlist,4), 5.55 );

	as_rec_destroy(rec);
	as_arraylist_destroy(&list);
	as_arraylist_destroy(&arglist);
	as_result_destroy(res);
}

TEST( list_udf_4, "insert an element into a list" )
{
	as_arraylist list;
	as_arraylist_init(&list, 4, 5);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_str(&list, "text");
	as_arraylist_append_int64(&list, 4);

	as_rec * rec = map_rec_new();
	as_rec_set(rec, "listbin", (as_val *) &list);

	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 3);
	as_arraylist_append_str(&arglist, "listbin");
	as_arraylist_append_int64(&arglist, 3);
	as_arraylist_append_int64(&arglist, 7);

	as_result * res = as_success_new(NULL);

	int rc = as_module_apply_record(&mod_lua, &ctx, "lists", "insert", rec, (as_list *) &arglist, res);

	assert_int_eq( rc, 0);
	assert_true( res->is_success );
	assert_not_null( res->value );
	as_list * rlist = (as_list *) res->value;
	assert_int_eq( as_list_size(rlist), 5 );
	assert_int_eq( as_list_get_int64(rlist,0), 1 );
	assert_int_eq( as_list_get_int64(rlist,1), 2 );
	assert_int_eq( as_list_get_int64(rlist,2), 7 );
	assert_string_eq( as_list_get_str(rlist,3), "text" );
	assert_int_eq( as_list_get_int64(rlist,4), 4 );

	as_rec_destroy(rec);
	as_arraylist_destroy(&list);
	as_arraylist_destroy(&arglist);
	as_result_destroy(res);
}

TEST( list_udf_5, "append an element to a list" )
{
	as_arraylist list;
	as_arraylist_init(&list, 4, 5);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);
	as_arraylist_append_int64(&list, 4);

	as_rec * rec = map_rec_new();
	as_rec_set(rec, "listbin", (as_val *) &list);

	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_str(&arglist, "listbin");
	as_arraylist_append_double(&arglist, 7.1);

	as_result * res = as_success_new(NULL);

	int rc = as_module_apply_record(&mod_lua, &ctx, "lists", "append", rec, (as_list *) &arglist, res);

	assert_int_eq( rc, 0);
	assert_true( res->is_success );
	assert_not_null( res->value );
	as_list * rlist = (as_list *) res->value;
	assert_int_eq( as_list_size(rlist), 5 );
	assert_int_eq( as_list_get_int64(rlist,0), 1 );
	assert_int_eq( as_list_get_int64(rlist,1), 2 );
	assert_int_eq( as_list_get_int64(rlist,2), 3 );
	assert_int_eq( as_list_get_int64(rlist,3), 4 );
	assert_double_eq( as_list_get_double(rlist,4), 7.1 );

	as_rec_destroy(rec);
	as_arraylist_destroy(&list);
	as_arraylist_destroy(&arglist);
	as_result_destroy(res);
}

TEST( list_udf_6, "prepend an element to a list" )
{
	as_arraylist list;
	as_arraylist_init(&list, 4, 5);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);
	as_arraylist_append_int64(&list, 4);

	as_rec * rec = map_rec_new();
	as_rec_set(rec, "listbin", (as_val *) &list);

	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_str(&arglist, "listbin");
	as_arraylist_append_double(&arglist, -7.1);

	as_result * res = as_success_new(NULL);

	int rc = as_module_apply_record(&mod_lua, &ctx, "lists", "prepend", rec, (as_list *) &arglist, res);

	assert_int_eq( rc, 0);
	assert_true( res->is_success );
	assert_not_null( res->value );
	as_list * rlist = (as_list *) res->value;
	assert_int_eq( as_list_size(rlist), 5 );
	assert_double_eq( as_list_get_double(rlist,0), -7.1 );
	assert_int_eq( as_list_get_int64(rlist,1), 1 );
	assert_int_eq( as_list_get_int64(rlist,2), 2 );
	assert_int_eq( as_list_get_int64(rlist,3), 3 );
	assert_int_eq( as_list_get_int64(rlist,4), 4 );

	as_rec_destroy(rec);
	as_arraylist_destroy(&list);
	as_arraylist_destroy(&arglist);
	as_result_destroy(res);
}

TEST( list_udf_7, "select the first 2 elements of a list" )
{
	as_arraylist list;
	as_arraylist_init(&list, 4, 5);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_double(&list, 2.2);
	as_arraylist_append_int64(&list, 3);
	as_arraylist_append_int64(&list, 4);

	as_rec * rec = map_rec_new();
	as_rec_set(rec, "listbin", (as_val *) &list);

	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_str(&arglist, "listbin");
	as_arraylist_append_int64(&arglist, 2);

	as_result * res = as_success_new(NULL);

	int rc = as_module_apply_record(&mod_lua, &ctx, "lists", "take", rec, (as_list *) &arglist, res);

	assert_int_eq( rc, 0);
	assert_true( res->is_success );
	assert_not_null( res->value );
	as_list * rlist = (as_list *) res->value;
	assert_int_eq( as_list_size(rlist), 2 );
	assert_int_eq( as_list_get_int64(rlist,0), 1 );
	assert_double_eq( as_list_get_double(rlist,1), 2.2 );

	as_rec_destroy(rec);
	as_arraylist_destroy(&list);
	as_arraylist_destroy(&arglist);
	as_result_destroy(res);
}

TEST( list_udf_8, "remove an element from a list" )
{
	as_arraylist list;
	as_arraylist_init(&list, 4, 5);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);
	as_arraylist_append_int64(&list, 4);

	as_rec * rec = map_rec_new();
	as_rec_set(rec, "listbin", (as_val *) &list);

	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_str(&arglist, "listbin");
	as_arraylist_append_int64(&arglist, 2);

	as_result * res = as_success_new(NULL);

	int rc = as_module_apply_record(&mod_lua, &ctx, "lists", "remove", rec, (as_list *) &arglist, res);

	assert_int_eq( rc, 0);
	assert_true( res->is_success );
	assert_not_null( res->value );
	as_list * rlist = (as_list *) res->value;
	assert_int_eq( as_list_size(rlist), 3 );
	assert_int_eq( as_list_get_int64(rlist,0), 1 );
	assert_int_eq( as_list_get_int64(rlist,1), 3 );
	assert_int_eq( as_list_get_int64(rlist,2), 4 );

	as_rec_destroy(rec);
	as_arraylist_destroy(&list);
	as_arraylist_destroy(&arglist);
	as_result_destroy(res);
}

TEST( list_udf_9, "drop the first 3 elements of a list" )
{
	as_arraylist list;
	as_arraylist_init(&list, 4, 5);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);
	as_arraylist_append_int64(&list, 4);
	as_arraylist_append_int64(&list, 5);

	as_rec * rec = map_rec_new();
	as_rec_set(rec, "listbin", (as_val *) &list);

	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_str(&arglist, "listbin");
	as_arraylist_append_int64(&arglist, 3);

	as_result * res = as_success_new(NULL);

	int rc = as_module_apply_record(&mod_lua, &ctx, "lists", "drop", rec, (as_list *) &arglist, res);

	assert_int_eq( rc, 0);
	assert_true( res->is_success );
	assert_not_null( res->value );
	as_list * rlist = (as_list *) res->value;
	assert_int_eq( as_list_size(rlist), 2 );
	assert_int_eq( as_list_get_int64(rlist,0), 4 );
	assert_int_eq( as_list_get_int64(rlist,1), 5 );

	as_rec_destroy(rec);
	as_arraylist_destroy(&list);
	as_arraylist_destroy(&arglist);
	as_result_destroy(res);
}

TEST( list_udf_10, "trim a list" )
{
	as_arraylist list;
	as_arraylist_init(&list, 4, 5);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);
	as_arraylist_append_str(&list, "text");
	as_arraylist_append_int64(&list, 4);

	as_rec * rec = map_rec_new();
	as_rec_set(rec, "listbin", (as_val *) &list);

	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_str(&arglist, "listbin");
	as_arraylist_append_int64(&arglist, 3);

	as_result * res = as_success_new(NULL);

	int rc = as_module_apply_record(&mod_lua, &ctx, "lists", "trim", rec, (as_list *) &arglist, res);

	assert_int_eq( rc, 0);
	assert_true( res->is_success );
	assert_not_null( res->value );
	as_list * rlist = (as_list *) res->value;
	assert_int_eq( as_list_size(rlist), 2 );
	assert_int_eq( as_list_get_int64(rlist,0), 1 );
	assert_int_eq( as_list_get_int64(rlist,1), 2 );

	as_rec_destroy(rec);
	as_arraylist_destroy(&list);
	as_arraylist_destroy(&arglist);
	as_result_destroy(res);
}

TEST( list_udf_11, "clone a list" )
{
	as_arraylist list;
	as_arraylist_init(&list, 4, 5);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_double(&list, 3.3);
	as_arraylist_append_int64(&list, 4);

	as_rec * rec = map_rec_new();
	as_rec_set(rec, "listbin", (as_val *) &list);

	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_str(&arglist, "listbin");

	as_result * res = as_success_new(NULL);

	int rc = as_module_apply_record(&mod_lua, &ctx, "lists", "clone", rec, (as_list *) &arglist, res);

	assert_int_eq( rc, 0);
	assert_true( res->is_success );
	assert_not_null( res->value );
	as_list * rlist = (as_list *) res->value;
	assert_int_eq( as_list_size(rlist), 4 );
	assert_int_eq( as_list_get_int64(rlist,0), 1 );
	assert_int_eq( as_list_get_int64(rlist,1), 2 );
	assert_double_eq( as_list_get_double(rlist,2), 3.3 );
	assert_int_eq( as_list_get_int64(rlist,3), 4 );

	as_rec_destroy(rec);
	as_arraylist_destroy(&list);
	as_arraylist_destroy(&arglist);
	as_result_destroy(res);
}

TEST( list_udf_12, "concatenate two lists" )
{
	as_arraylist list1;
	as_arraylist_init(&list1, 4, 5);
	as_arraylist_append_int64(&list1, 1);
	as_arraylist_append_int64(&list1, 2);
	as_arraylist_append_int64(&list1, 3);
	as_arraylist_append_int64(&list1, 4);

	as_arraylist list2;
	as_arraylist_init(&list2, 4, 5);
	as_arraylist_append_int64(&list2, 7);
	as_arraylist_append_int64(&list2, 8);
	as_arraylist_append_int64(&list2, 9);

	as_rec * rec = map_rec_new();
	as_rec_set(rec, "list1bin", (as_val *) &list1);
	as_rec_set(rec, "list2bin", (as_val *) &list2);

	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_str(&arglist, "list1bin");
	as_arraylist_append_str(&arglist, "list2bin");

	as_result * res = as_success_new(NULL);

	int rc = as_module_apply_record(&mod_lua, &ctx, "lists", "concat", rec, (as_list *) &arglist, res);

	assert_int_eq( rc, 0);
	assert_true( res->is_success );
	assert_not_null( res->value );
	as_list * rlist = (as_list *) res->value;
	assert_int_eq( as_list_size(rlist), 7 );
	assert_int_eq( as_list_get_int64(rlist,0), 1 );
	assert_int_eq( as_list_get_int64(rlist,1), 2 );
	assert_int_eq( as_list_get_int64(rlist,2), 3 );
	assert_int_eq( as_list_get_int64(rlist,3), 4 );
	assert_int_eq( as_list_get_int64(rlist,4), 7 );
	assert_int_eq( as_list_get_int64(rlist,5), 8 );
	assert_int_eq( as_list_get_int64(rlist,6), 9 );

	as_rec_destroy(rec);
	as_arraylist_destroy(&list1);
	as_arraylist_destroy(&list2);
	as_arraylist_destroy(&arglist);
	as_result_destroy(res);
}

TEST( list_udf_13, "merge two lists" )
{
	as_arraylist list1;
	as_arraylist_init(&list1, 4, 5);
	as_arraylist_append_int64(&list1, 1);
	as_arraylist_append_int64(&list1, 2);
	as_arraylist_append_int64(&list1, 3);

	as_arraylist list2;
	as_arraylist_init(&list2, 4, 5);
	as_arraylist_append_int64(&list2, 6);
	as_arraylist_append_int64(&list2, 7);
	as_arraylist_append_double(&list2, 8.8);
	as_arraylist_append_int64(&list2, 9);

	as_rec * rec = map_rec_new();
	as_rec_set(rec, "list1bin", (as_val *) &list1);
	as_rec_set(rec, "list2bin", (as_val *) &list2);

	// as_module_apply_record() will decrement ref count and attempt to free,
	// so add extra reserve and free later.
	as_val_reserve(rec);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_str(&arglist, "list1bin");
	as_arraylist_append_str(&arglist, "list2bin");

	as_result * res = as_success_new(NULL);

	int rc = as_module_apply_record(&mod_lua, &ctx, "lists", "merge", rec, (as_list *) &arglist, res);

	assert_int_eq( rc, 0);
	assert_true( res->is_success );
	assert_not_null( res->value );
	as_list * rlist = (as_list *) res->value;
	assert_int_eq( as_list_size(rlist), 7 );
	assert_int_eq( as_list_get_int64(rlist,0), 1 );
	assert_int_eq( as_list_get_int64(rlist,1), 2 );
	assert_int_eq( as_list_get_int64(rlist,2), 3 );
	assert_int_eq( as_list_get_int64(rlist,3), 6 );
	assert_int_eq( as_list_get_int64(rlist,4), 7 );
	assert_double_eq( as_list_get_double(rlist,5), 8.8 );
	assert_int_eq( as_list_get_int64(rlist,6), 9 );

	as_rec_destroy(rec);
	as_arraylist_destroy(&list1);
	as_arraylist_destroy(&list2);
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

SUITE( list_udf, "list udf tests" )
{
	suite_before( before );
 	suite_after( after );

	suite_add( list_udf_1 );
	suite_add( list_udf_2 );
	suite_add( list_udf_3 );
	suite_add( list_udf_4 );
	suite_add( list_udf_5 );
	suite_add( list_udf_6 );
	suite_add( list_udf_7 );
	suite_add( list_udf_8 );
	suite_add( list_udf_9 );
	suite_add( list_udf_10 );
	suite_add( list_udf_11 );
	suite_add( list_udf_12 );
	suite_add( list_udf_13 );
}
