#pragma once

#include "as_bytes.h"
#include "mod_lua_val.h"
#include <lua.h>

int mod_lua_bytes_register(lua_State *);

as_bytes * mod_lua_pushbytes(lua_State *, as_bytes * );

as_bytes * mod_lua_tobytes(lua_State *, int);
