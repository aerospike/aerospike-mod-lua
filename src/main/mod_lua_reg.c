/* 
 * Copyright 2008-2023 Aerospike, Inc.
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

#include <stddef.h>

#include <aerospike/as_val.h>
#include <aerospike/mod_lua_reg.h>

#include "internal.h"

int
mod_lua_reg_object(lua_State* l, const char* name, const luaL_Reg* table,
		const luaL_Reg* metatable)
{
	lua_newtable(l);                   // -0 +1
	luaL_setfuncs(l, table, 0);        // -0 +0
	lua_pushvalue(l, -1);              // -0 +1
	lua_setglobal(l, name);            // -1 +0

	int table_id = lua_gettop(l);      // -0 +0

	lua_newtable(l);                   // -0 +1
	luaL_setfuncs(l, metatable, 0);    // -0 +0

	int metatable_id = lua_gettop(l);  // -0 +0

	lua_pushvalue(l, metatable_id);    // -0 +1
	lua_setmetatable(l, table_id);     // -1 +0

	lua_pushliteral(l, "__metatable"); // -0 +1
	lua_pushvalue(l, table_id);        // -0 +1
	lua_rawset(l, metatable_id);       // -2 +0

	lua_pop(l, 2);                     // -2 +0 - pop metatable and table

	return 0;
}

int
mod_lua_reg_class(lua_State* l, const char* name, const luaL_Reg* table,
		const luaL_Reg* metatable)
{
	int table_id = 0;
	int pop_cnt = 0;

	if (table != NULL) {
		lua_newtable(l);                   // -0 +1
		luaL_setfuncs(l, table, 0);        // -0 +0
		lua_pushvalue(l, -1);              // -0 +1
		lua_setglobal(l, name);            // -1 +0
		table_id = lua_gettop(l);          // -0 +0
		pop_cnt++;
	}

	int metatable_id = 0;

	if (metatable != NULL) {
		luaL_newmetatable(l, name);        // -0 +1
		luaL_setfuncs(l, metatable, 0);    // -0 +0
		metatable_id = lua_gettop(l);      // -0 +0
		pop_cnt++;
	}

	if (table != NULL && metatable != NULL) {
		lua_pushliteral(l, "__index");     // -0 +1
		lua_pushvalue(l, table_id);        // -0 +1
		lua_rawset(l, metatable_id);       // -2 +0

		lua_pushliteral(l, "__metatable"); // -0 +1
		lua_pushvalue(l, table_id);        // -0 +1
		lua_rawset(l, metatable_id);       // -2 +0
	}

	lua_pop(l, pop_cnt);                   // -pop_cnt +0

	return 0;
}
