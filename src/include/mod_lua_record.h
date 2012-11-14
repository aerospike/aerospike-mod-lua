#ifndef _MOD_LUA_RECORD_H
#define _MOD_LUA_RECORD_H

#include "as_rec.h"
#include <lua.h>

int mod_lua_record_register(lua_State *);

as_rec * mod_lua_pushrecord(lua_State *, as_rec * );

as_rec * mod_lua_torecord(lua_State *, int);

#endif // _MOD_LUA_RECORD_H