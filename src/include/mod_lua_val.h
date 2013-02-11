#pragma once

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "as_types.h"

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
