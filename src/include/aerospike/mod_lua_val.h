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
#pragma once

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <aerospike/as_types.h>

typedef struct mod_lua_box_s mod_lua_box;

typedef enum {
    MOD_LUA_SCOPE_LUA,      // The value can be freed by Lua
    MOD_LUA_SCOPE_HOST      // The value must not be freed by Lua
}  mod_lua_scope;

struct mod_lua_box_s {
    mod_lua_scope scope;
    void * value;
};

as_val * mod_lua_takeval(lua_State * l, int i);
as_val * mod_lua_retval(lua_State * l);
as_val * mod_lua_toval(lua_State *, int);
int mod_lua_pushval(lua_State *, const as_val *);

mod_lua_box * mod_lua_newbox(lua_State *, mod_lua_scope, void *, const char *);
mod_lua_box * mod_lua_pushbox(lua_State *, mod_lua_scope, void *, const char *);
mod_lua_box * mod_lua_tobox(lua_State *, int, const char *);
mod_lua_box * mod_lua_checkbox(lua_State *, int, const char *);
int mod_lua_freebox(lua_State *, int, const char *);
void * mod_lua_box_value(mod_lua_box *);
