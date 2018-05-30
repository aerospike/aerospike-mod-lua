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
#include <aerospike/as_module.h>
#include <aerospike/as_types.h>
#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>
#include <citrusleaf/alloc.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "../test.h"
#include "../util/map_rec.h"
#include "../util/test_aerospike.h"
#include "../util/test_logger.h"

struct lua_hash_ele_s;
typedef struct lua_hash_ele_s lua_hash_ele;

struct lua_hash_s;
typedef struct lua_hash_s lua_hash;
struct cache_entry_s;
typedef struct cache_entry_s cache_entry;

void lua_hash_clear(lua_hash* h, void (*cb)(cache_entry *));
lua_hash* lua_hash_create(uint32_t key_size, uint32_t n_rows);
void lua_hash_destroy(lua_hash* h);
bool lua_hash_get(const lua_hash* h, const char* key, cache_entry** p_value);
cache_entry* lua_hash_put(lua_hash* h, const char* key, cache_entry* value);
cache_entry* lua_hash_remove(lua_hash* h, const char* key);

static const char* a_val = "test1";
static const char* b_val = "test2";
static const char* c_val = "test3";
static const char* d_val = "test4";

static const char* orig_val = "me";
static const char* replacement_val = "you";

static uint8_t val_count;
static char vals[5][6];

lua_hash* h;
/******************************************************************************
 * TEST CASES
 *****************************************************************************/
static void
hash_udf_setup_test()
{
	h = lua_hash_create(5, 3);
	lua_hash_put(h, "a", (cache_entry*)a_val);
	lua_hash_put(h, "b", (cache_entry*)b_val);
	lua_hash_put(h, "c", (cache_entry*)c_val);
	lua_hash_put(h, "d", (cache_entry*)d_val);
	debug("setting up");
	val_count = 0;
	vals[0][0] = '\0';
	vals[1][0] = '\0';
	vals[2][0] = '\0';
	vals[3][0] = '\0';
}

static void
hash_udf_teardown_test()
{
	debug("tearing down");
	lua_hash_destroy(h);
}

void
clear_cb(cache_entry* centry) {
	char* entry_str = (char*)centry;
	strncpy((vals[val_count++]), (char*)entry_str, 5);
}

// asserts don't work from outside of test functions therefore this bit of "..."
bool
remove_ok(const char* key, const char* val)
{
	char* from_hash;

	return lua_hash_get(h, key, (cache_entry**)&from_hash) && // key exists
			from_hash == val && // with an expected value
			lua_hash_remove(h, key) == (cache_entry*)val && // removing succeeded
			! lua_hash_get(h, key, (cache_entry**)&from_hash) && // key ! exists
			lua_hash_remove(h, key) == NULL; // second remove fails
}

TEST(hash_udf_1, "gets succeed and return correct value if key exists")
{

	hash_udf_setup_test();

	char* from_hash;

	assert_true(lua_hash_get(h, "a", (cache_entry**)&from_hash));
	assert_true(from_hash == a_val);
	assert_true(lua_hash_get(h, "b", (cache_entry**)&from_hash));
	assert_true(from_hash == b_val);
	assert_true(lua_hash_get(h, "c", (cache_entry**)&from_hash));
	assert_true(from_hash == c_val);
	assert_true(lua_hash_get(h, "d", (cache_entry**)&from_hash));
	assert_true(from_hash == d_val);

	assert_true(lua_hash_get(h, "d", NULL));
	assert_false(lua_hash_get(h, "FAKWAN", NULL));
	assert_false(lua_hash_get(h, "FAKWAN", (cache_entry**)&from_hash));

	hash_udf_teardown_test();
}

TEST(hash_udf_2, "removes work correctly for all positions in hash")
{
	hash_udf_setup_test();

	assert_null(lua_hash_remove(h, "FAKWAN"));
	assert_true(remove_ok("c", c_val));
	assert_true(remove_ok("a", a_val));
	assert_true(remove_ok("b", b_val));
	assert_true(remove_ok("d", d_val));

	hash_udf_teardown_test();
}

TEST(hash_udf_3, "clear removes all items in hash")
{
	hash_udf_setup_test();

	lua_hash_clear(h, NULL);
	assert_false(lua_hash_get(h, "a", NULL));
	assert_false(lua_hash_get(h, "b", NULL));
	assert_false(lua_hash_get(h, "c", NULL));
	assert_false(lua_hash_get(h, "d", NULL));
	lua_hash_clear(h, NULL); // should be able to call on empty hash without crash

	hash_udf_teardown_test();
}

TEST(hash_udf_4, "cleared hash still usable")
{
	char* from_hash;
	hash_udf_setup_test();

	lua_hash_clear(h, NULL);
	lua_hash_put(h, "a", (cache_entry*)a_val);
	lua_hash_put(h, "b", (cache_entry*)b_val);
	lua_hash_put(h, "c", (cache_entry*)c_val);
	lua_hash_put(h, "d", (cache_entry*)d_val);
	assert_true(lua_hash_get(h, "a", (cache_entry**)&from_hash));
	assert_true(from_hash == a_val);
	assert_true(lua_hash_get(h, "b", (cache_entry**)&from_hash));
	assert_true(from_hash == b_val);
	assert_true(lua_hash_get(h, "c", (cache_entry**)&from_hash));
	assert_true(from_hash == c_val);
	assert_true(lua_hash_get(h, "d", (cache_entry**)&from_hash));
	assert_true(from_hash == d_val);

	hash_udf_teardown_test();
}

TEST(hash_udf_5, "cleared hash callback is called correctly")
{
	hash_udf_setup_test();

	lua_hash_clear(h, &clear_cb);
	// These depend on the order in the hash. The hash is stable so we can
	// hardcode it.
	assert_string_eq(vals[0], "test1");
	assert_string_eq(vals[1], "test4");
	assert_string_eq(vals[2], "test2");
	assert_string_eq(vals[3], "test3");
	assert_int_eq(val_count, 4);

	hash_udf_teardown_test();
}

TEST(hash_udf_6, "replacing a key works and returns previous value, long keys get truncated.")
{
	hash_udf_setup_test();

	char* from_hash;
	cache_entry* ret = lua_hash_put(h, "help", (cache_entry*)orig_val);
	assert_null(ret);
	assert_true(lua_hash_get(h, "help", (cache_entry**)&from_hash));
	assert_true(from_hash == orig_val);
	ret = lua_hash_put(h, "help", (cache_entry*)replacement_val);
	assert_true(ret == (cache_entry*)orig_val);
	assert_true(lua_hash_get(h, "help", (cache_entry**)&from_hash));
	assert_string_eq(from_hash, replacement_val);

	hash_udf_teardown_test();
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/
SUITE(hash_udf, "list udf tests")
{
	suite_before(test_suite_before);
	suite_after(test_suite_after);

	suite_add(hash_udf_1);
	suite_add(hash_udf_2);
	suite_add(hash_udf_3);
	suite_add(hash_udf_4);
	suite_add(hash_udf_5);
	suite_add(hash_udf_6);
}
