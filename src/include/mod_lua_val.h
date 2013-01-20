#pragma once

#include "as_types.h"
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

typedef enum mod_lua_scope_t mod_lua_scope;
typedef struct mod_lua_box_s mod_lua_box;

enum mod_lua_scope_t {
    MOD_LUA_SCOPE_HOST  = 0,
    MOD_LUA_SCOPE_LUA   = 1
};

struct mod_lua_box_s {
    mod_lua_scope scope;
    as_val * value;
};

static inline mod_lua_box * mod_lua_newbox(lua_State * l, mod_lua_scope scope, as_val * value, const char * type) {
    mod_lua_box * box = (mod_lua_box *) lua_newuserdata(l, sizeof(mod_lua_box));
    box->scope = scope;
    box->value = value;
    return box;
}

static inline mod_lua_box * mod_lua_pushbox(lua_State * l, mod_lua_scope scope, as_val * value, const char * type) {
    mod_lua_box * box = (mod_lua_box *) mod_lua_newbox(l, scope, value, type);
    luaL_getmetatable(l, type);
    lua_setmetatable(l, -2);
    return box;
}

static inline mod_lua_box * mod_lua_tobox(lua_State * l, int index, const char * type) {
    mod_lua_box * box = (mod_lua_box *) lua_touserdata(l, index);
    if (box == NULL && type != NULL ) luaL_typerror(l, index, type);
    return box;
}

static inline mod_lua_box * mod_lua_checkbox(lua_State * l, int index, const char * type) {
    luaL_checktype(l, index, LUA_TUSERDATA);
    mod_lua_box * box = (mod_lua_box *) luaL_checkudata(l, index, type);
    if (box == NULL) luaL_typerror(l, index, type);
    return box;
}

static inline int mod_lua_freebox(lua_State * l, int index, const char * type) {
    mod_lua_box * box = mod_lua_checkbox(l, index, type);
    if ( box && box->scope == MOD_LUA_SCOPE_LUA ) {
        as_val * val = box->value;
        as_val_free(val);
        box->value = NULL;
    }
    return 0;
}

static inline as_val * mod_lua_box_value(mod_lua_box * box) {
    return box ? box->value : NULL;
}

as_val * mod_lua_takeval(lua_State * l, int i);
as_val * mod_lua_retval(lua_State * l);
as_val * mod_lua_toval(lua_State *, int);
int mod_lua_pushval(lua_State *, mod_lua_scope, const as_val *);
