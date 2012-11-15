#pragma once

#include "as_module.h"
#include <lua.h>

/**
 * Lua Module
 */
extern as_module mod_lua;

typedef struct mod_lua_context_s mod_lua_context;

mod_lua_context * mod_lua_context_create(const char *, lua_State *);

int mod_lua_context_free(mod_lua_context *);
