#include "mod_lua_val.h"
#include "mod_lua_reg.h"
#include "mod_lua_iterator.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#define OBJECT_NAME "iterator"
#define CLASS_NAME  "Iterator"

/**
 * Read the item at index and convert to a iterator
 */
as_iterator * mod_lua_toiterator(lua_State * l, int index) {
    as_iterator * i = (as_iterator *) lua_touserdata(l, index);
    if (i == NULL) luaL_typerror(l, index, CLASS_NAME);
    return i;
}

/**
 * Push a iterator on to the lua stack
 */
as_iterator * mod_lua_pushiterator(lua_State * l, as_iterator * i) {
    as_iterator * li = (as_iterator *) lua_newuserdata(l, sizeof(as_iterator));
    *li = *i;
    luaL_getmetatable(l, CLASS_NAME);
    lua_setmetatable(l, -2);
    return li;
}

/**
 * Get the user iterator from the stack at index
 */
static as_iterator * mod_lua_checkiterator(lua_State * l, int index) {
    as_iterator * i = NULL;
    luaL_checktype(l, index, LUA_TUSERDATA);
    i = (as_iterator *) luaL_checkudata(l, index, CLASS_NAME);
    if (i == NULL) luaL_typerror(l, index, CLASS_NAME);
    return i;
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

/**
 * Garbage collection 
 */
static int mod_lua_iterator_gc(lua_State * l) {
    // as_iterator * i = mod_lua_checkiterator(l, 1);
    // as_iterator_free(i);
    return 0;
}

/*******************************************************************************
 * ~~~ Object ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 ******************************************************************************/

static const luaL_reg object_table[] = {
    {"has_next",        mod_lua_iterator_has_next},
    {"next",            mod_lua_iterator_next},
    {0, 0}
};

static const luaL_reg object_metatable[] = {
    {0, 0}
};

/*******************************************************************************
 * ~~~ Class ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 ******************************************************************************/

static const luaL_reg class_table[] = {
    {"has_next",        mod_lua_iterator_has_next},
    {"next",            mod_lua_iterator_next},
    {0, 0}
};

static const luaL_reg class_metatable[] = {
    {"__gc",            mod_lua_iterator_gc},
    {0, 0}
};

/*******************************************************************************
 * ~~~ Register ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 ******************************************************************************/

int mod_lua_iterator_register(lua_State * l) {
    mod_lua_reg_object(l, OBJECT_NAME, object_table, object_metatable);
    mod_lua_reg_class(l, CLASS_NAME, class_table, class_metatable);
    return 1;
}

