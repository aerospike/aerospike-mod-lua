#pragma once

#include "as_types.h"
#include <lua.h>

as_val * mod_lua_toval(lua_State *, int);

int mod_lua_pushval(lua_State *, const as_val *);
