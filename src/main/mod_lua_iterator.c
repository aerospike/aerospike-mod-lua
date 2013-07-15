/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <aerospike/as_val.h>

#include <aerospike/mod_lua_val.h>
#include <aerospike/mod_lua_reg.h>
#include <aerospike/mod_lua_iterator.h>

#include "internal.h"

/*******************************************************************************
 * MACROS
 ******************************************************************************/

#define OBJECT_NAME "iterator"
#define CLASS_NAME  "Iterator"

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

//
// NOTE ITERATORS ARE NOT AS_VALS
//

 as_iterator * mod_lua_toiterator(lua_State * l, int index) {
    as_iterator * itr = (as_iterator *) lua_touserdata(l, index);
    return (as_iterator *) itr;
}

as_iterator * mod_lua_pushiterator(lua_State * l) {
    as_iterator * i = (as_iterator *) lua_newuserdata(l, sizeof(as_iterator));
    memset(i, 0, sizeof(as_iterator));
    luaL_getmetatable(l, CLASS_NAME);
    lua_setmetatable(l, -2);
    return i;
}

static as_iterator * mod_lua_checkiterator(lua_State * l, int index) {
    luaL_checktype(l, index, LUA_TUSERDATA);
    as_iterator * itr = (as_iterator *) luaL_checkudata(l, index, CLASS_NAME);
    if (itr == NULL) luaL_typerror(l, index, CLASS_NAME);
    return itr;
}

static int mod_lua_iterator_gc(lua_State * l) {
    as_iterator * itr = (as_iterator *) lua_touserdata(l, 1);
    if (itr) as_iterator_destroy(itr);
    return 0;
}

/**
 * Tests to see if there are any more entries in the iterator
 */
static int mod_lua_iterator_has_next(lua_State * l) {
    as_iterator * i = mod_lua_checkiterator(l, 1);
    bool b = as_iterator_has_next(i);
    lua_pushboolean(l, b);
    return 1;
}

/**
 * Tests to see if there are any more entries in the iterator
 */
static int mod_lua_iterator_next(lua_State * l) {
    as_iterator * i = mod_lua_checkiterator(l, 1);
    as_val * v = (as_val *) as_iterator_next(i);
    if ( v != NULL ) {
        mod_lua_pushval(l,v);
    }
    else {
        lua_pushnil(l);
    }
    return 1;
}


/******************************************************************************
 * OBJECT TABLE
 *****************************************************************************/

static const luaL_reg object_table[] = {
    {"has_next",        mod_lua_iterator_has_next},
    {"next",            mod_lua_iterator_next},
    {0, 0}
};

static const luaL_reg object_metatable[] = {
    {"__call",          mod_lua_iterator_next},
    {0, 0}
};

/******************************************************************************
 * CLASS TABLE
 *****************************************************************************/

static const luaL_reg class_table[] = {
    {"has_next",        mod_lua_iterator_has_next},
    {"next",            mod_lua_iterator_next},
    {0, 0}
};

static const luaL_reg class_metatable[] = {
    {"__gc",            mod_lua_iterator_gc},
    {0, 0}
};

/******************************************************************************
 * REGISTER
 *****************************************************************************/

int mod_lua_iterator_register(lua_State * l) {
    mod_lua_reg_object(l, OBJECT_NAME, object_table, object_metatable);
    mod_lua_reg_class(l, CLASS_NAME, class_table, class_metatable);
    return 1;
}

