/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/
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
