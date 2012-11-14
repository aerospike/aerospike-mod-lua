#ifndef _MOD_LUA_ITERATOR_H
#define _MOD_LUA_ITERATOR_H

#include "as_iterator.h"
#include <lua.h>

int mod_lua_iterator_register(lua_State *);

as_iterator * mod_lua_pushiterator(lua_State *, as_iterator *);

as_iterator * mod_lua_toiterator(lua_State *, int);

#endif // _MOD_LUA_ITERATOR_H