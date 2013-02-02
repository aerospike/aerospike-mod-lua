#pragma once

#include "as_stream.h"
#include <lua.h>

int mod_lua_stream_register(lua_State *);

as_stream * mod_lua_pushstream(lua_State *, as_stream *);

as_stream * mod_lua_tostream(lua_State *, int);
