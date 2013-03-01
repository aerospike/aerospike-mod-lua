#pragma once

#include "as_module.h"
#include <lua.h>

/**
 * Lua Module
 */
extern as_module mod_lua;


/**
 * Locks
 */
int mod_lua_rdlock(as_module * m);
int mod_lua_wrlock(as_module * m);
int mod_lua_unlock(as_module * m);
