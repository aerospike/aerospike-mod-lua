/*
 * Copyright 2008-2026 Aerospike, Inc.
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
#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>
#include <citrusleaf/alloc.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "../test.h"
#include "../util/test_aerospike.h"
#include "../util/test_logger.h"

/******************************************************************************
 * HELPERS
 *****************************************************************************/

static bool
configure(bool unsafe_lua_disabled)
{
	mod_lua_config cfg = {
		.server_mode = true,
		.cache_enabled = false,
		.unsafe_lua_disabled = unsafe_lua_disabled,
		.user_path = AS_START_DIR "src/test/lua"
	};

	return as_module_configure(&mod_lua, &cfg) == 0;
}

static int
validate_src(const char* filename, const char* src, as_module_error* err)
{
	return as_module_validate(&mod_lua, ctx.as, filename, src,
			(uint32_t)strlen(src), err);
}

// Captures bytes from lua_dump.
typedef struct {
	uint8_t* buf;
	size_t len;
	size_t cap;
} dump_buf;

static int
dump_writer(lua_State* L, const void* p, size_t sz, void* ud)
{
	(void)L;

	dump_buf* d = (dump_buf*)ud;

	if (d->len + sz > d->cap) {
		size_t new_cap = (d->cap == 0 ? 256 : d->cap * 2);

		while (new_cap < d->len + sz) {
			new_cap *= 2;
		}

		uint8_t* nb = (uint8_t*)cf_realloc(d->buf, new_cap);

		if (nb == NULL) {
			return 1;
		}

		d->buf = nb;
		d->cap = new_cap;
	}

	memcpy(d->buf + d->len, p, sz);
	d->len += sz;
	return 0;
}

// Compiles `src` to Lua bytecode, returning malloced bytes (caller frees).
static uint8_t*
compile_to_bytecode(const char* src, size_t* out_len)
{
	lua_State* L = luaL_newstate();

	if (L == NULL) {
		return NULL;
	}

	if (luaL_loadstring(L, src) != 0) {
		lua_close(L);
		return NULL;
	}

	dump_buf d = { 0 };
	int rc = lua_dump(L, dump_writer, &d, 0);

	lua_close(L);

	if (rc != 0) {
		cf_free(d.buf);
		return NULL;
	}

	*out_len = d.len;
	return d.buf;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(sandbox_drops_globals,
		"unsafe_lua_disabled: dangerous globals are nil during validate")
{
	assert(configure(true));

	// Each of these globals must be unreachable from user code in the
	// sandboxed state. If any one is non-nil, the assert raises and validate
	// fails the pcall.
	//
	// Note - aerospike.lua, which runs after sandbox_state, re-binds the
	// global "debug" to a logging function. So checking `debug == nil` would
	// fail even with the sandbox on. The security-relevant property is that
	// the standard debug *library* (debug.getinfo, debug.sethook, ...) is
	// unreachable, which is what the pcall below verifies: if `debug` is the
	// logger function or nil, indexing it raises.
	const char* src =
		"assert(os == nil, 'os leaked')\n"
		"assert(io == nil, 'io leaked')\n"
		"assert(dofile == nil, 'dofile leaked')\n"
		"assert(loadfile == nil, 'loadfile leaked')\n"
		"assert(load == nil, 'load leaked')\n"
		// loadstring is gone in Lua 5.2+ but the sandbox nils it defensively.
		"assert(loadstring == nil, 'loadstring leaked')\n"
		"assert(not pcall(function() return debug.getinfo end), "
				"'debug library reachable')\n";

	as_module_error err;
	int rc = validate_src("sandbox_globals.lua", src, &err);

	if (rc != 0) {
		info("unexpected validate failure: %s", err.message);
	}

	assert_int_eq(rc, 0);
}

TEST(sandbox_off_globals_present,
		"unsafe_lua_disabled=false: dangerous globals still exist")
{
	assert(configure(false));

	// "debug" is rebound by aerospike.lua regardless of sandbox state, so
	// only os/io meaningfully differ. With sandbox off they're the standard
	// libraries (tables).
	const char* src =
		"assert(type(os) == 'table', 'os missing')\n"
		"assert(type(io) == 'table', 'io missing')\n";

	as_module_error err;
	int rc = validate_src("sandbox_off.lua", src, &err);

	if (rc != 0) {
		info("unexpected validate failure: %s", err.message);
	}

	assert_int_eq(rc, 0);
}

TEST(sandbox_blocks_require_modules,
		"unsafe_lua_disabled: require('os'|'io'|'debug') fails")
{
	assert(configure(true));

	// package.loaded entries are cleared and the C searchers are dropped, so
	// require() can't hand back the C modules.
	const char* src =
		"assert(not pcall(require, 'os'), 'require(os) succeeded')\n"
		"assert(not pcall(require, 'io'), 'require(io) succeeded')\n"
		"assert(not pcall(require, 'debug'), 'require(debug) succeeded')\n";

	as_module_error err;
	int rc = validate_src("require_check.lua", src, &err);

	if (rc != 0) {
		info("unexpected validate failure: %s", err.message);
	}

	assert_int_eq(rc, 0);
}

TEST(sandbox_rejects_so_filename,
		"unsafe_lua_disabled: .so filename rejected with code 14")
{
	assert(configure(true));

	const char* src = "return 1";
	as_module_error err;
	int rc = as_module_validate(&mod_lua, ctx.as, "evil.so", src,
			(uint32_t)strlen(src), &err);

	assert_int_eq(rc, 14);
	assert_int_eq(err.scope, 2);
}

TEST(sandbox_accepts_lua_filename,
		"unsafe_lua_disabled: trivial .lua file accepted")
{
	assert(configure(true));

	const char* src = "local x = 1";
	as_module_error err;
	int rc = validate_src("trivial.lua", src, &err);

	if (rc != 0) {
		info("unexpected validate failure: %s", err.message);
	}

	assert_int_eq(rc, 0);
}

TEST(sandbox_off_accepts_so_filename,
		"unsafe_lua_disabled=false: .so filename passes the extension gate")
{
	assert(configure(false));

	// With the sandbox off, the .so gate in validate() is bypassed entirely
	// and validate() short-circuits for .so files without parsing content.
	as_module_error err;
	int rc = as_module_validate(&mod_lua, ctx.as, "ok.so", "", 0, &err);

	assert_int_eq(rc, 0);
}

TEST(sandbox_rejects_bytecode,
		"unsafe_lua_disabled: precompiled bytecode rejected via mode='t'")
{
	// First, generate a real bytecode chunk using a throwaway Lua state.
	size_t bc_len = 0;
	uint8_t* bc = compile_to_bytecode("return 42", &bc_len);

	assert_not_null(bc);
	assert(bc_len > 0);

	assert(configure(true));

	as_module_error err;
	int rc = as_module_validate(&mod_lua, ctx.as, "bytecode.lua",
			(char*)bc, (uint32_t)bc_len, &err);

	cf_free(bc);

	// Validate must reject; the exact code isn't pinned (luaL_loadbufferx
	// returns LUA_ERRSYNTAX → populate_error maps to code 10), but rc must
	// be non-zero.
	assert_int_ne(rc, 0);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(sandbox_basics, "sandbox_state and UDF-policy gate")
{
	suite_before(test_suite_before);

	suite_add(sandbox_drops_globals);
	suite_add(sandbox_off_globals_present);
	suite_add(sandbox_blocks_require_modules);
	suite_add(sandbox_rejects_so_filename);
	suite_add(sandbox_accepts_lua_filename);
	suite_add(sandbox_off_accepts_so_filename);
	suite_add(sandbox_rejects_bytecode);
}
