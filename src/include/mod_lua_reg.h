#pragma once

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <stdio.h>



/**
 * Registers an Object
 * An Object is a Lua Table that is not bound to userdata.
 */
int mod_lua_reg_object(lua_State *, const char *, const luaL_reg *, const luaL_reg *);

/**
 * Registers a Class
 * A Class is a Lua Table that is bound to userdata.
 */
int mod_lua_reg_class(lua_State *, const char *, const luaL_reg *, const luaL_reg *);