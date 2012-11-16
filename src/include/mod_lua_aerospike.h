#pragma once

#include "as_aerospike.h"

#include <lua.h>

int mod_lua_aerospike_register(lua_State *);

as_aerospike * mod_lua_pushaerospike(lua_State *, as_aerospike * );

as_aerospike * mod_lua_toaerospike(lua_State *, int);
