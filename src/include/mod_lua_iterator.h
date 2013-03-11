#pragma once

#include "as_iterator.h"
#include <lua.h>

int mod_lua_iterator_register(lua_State *);

// Pushes an iterator userdata object, and returns that
// object so it can be initialized
// (works different than some of the other calls)
as_iterator * mod_lua_pushiterator(lua_State *);

as_iterator * mod_lua_toiterator(lua_State *, int);
