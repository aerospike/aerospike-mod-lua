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

#include <lauxlib.h>
#include <lua.h>

struct lua_State;

//
// logging
//

#define LOG(fmt, ...) \
    // __log_append(__FILE__, __LINE__, fmt, ##__VA_ARGS__);

void __log_append(const char * file, int line, const char * fmt, ...);

// A copy of luaL_typerror which was dropped in lua-5.3.
static inline int
mod_lua_typerror(struct lua_State *L, int narg, const char *tname)
{
	const char *msg = lua_pushfstring(L, "%s expected, got %s",
			tname, luaL_typename(L, narg));
	return luaL_argerror(L, narg, msg);
}

#define DO_PRAGMA(x) _Pragma (#x)
#define TODO(x) DO_PRAGMA(message ("TODO - " #x))
