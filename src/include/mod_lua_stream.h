#ifndef _MOD_LUA_STREAM_H
#define _MOD_LUA_STREAM_H

#include "as_stream.h"
#include <lua.h>

int mod_lua_stream_register(lua_State *);

as_stream * mod_lua_pushstream(lua_State *, as_stream *);

as_stream * mod_lua_tostream(lua_State *, int);

#endif // _MOD_LUA_STREAM_H