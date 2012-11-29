#pragma once

#include "as_list.h"

#include <lua.h>

int mod_lua_list_register(lua_State *);

as_list * mod_lua_pushlist(lua_State *, as_list * );

as_list * mod_lua_tolist(lua_State *, int);
