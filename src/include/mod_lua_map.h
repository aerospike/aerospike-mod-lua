#pragma once

#include "as_map.h"
#include "mod_lua_val.h"
#include <lua.h>

int mod_lua_map_register(lua_State *);

as_map * mod_lua_pushmap(lua_State *, as_map * );

as_map * mod_lua_tomap(lua_State *, int);
