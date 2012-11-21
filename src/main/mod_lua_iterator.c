/**
 * Provides a lua interface to the aerospike struct and functions
 *
 *
 *      aerospike.get(namespace, set, key): result<record>
 *      aerospike.put(namespace, set, key, table)
 *      aerospike.remove(namespace, set, key): result<bool>
 *
 *      aerospike.update(record): result<record>
 *
 *
 */

#include "mod_lua_val.h"
#include "mod_lua_iterator.h"
#include "mod_lua_record.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#define MOD_LUA_ITERATOR "Iterator"

/**
 * Read the item at index and convert to a iterator
 */
as_iterator * mod_lua_toiterator(lua_State * l, int index) {
    as_iterator * i = (as_iterator *) lua_touserdata(l, index);
    if (i == NULL) luaL_typerror(l, index, MOD_LUA_ITERATOR);
    return i;
}

/**
 * Push a iterator on to the lua stack
 */
as_iterator * mod_lua_pushiterator(lua_State * l, as_iterator * i) {
    as_iterator * li = (as_iterator *) lua_newuserdata(l, sizeof(as_iterator));
    *li = *i;
    luaL_getmetatable(l, MOD_LUA_ITERATOR);
    lua_setmetatable(l, -2);
    return li;
}

/**
 * Get the user iterator from the stack at index
 */
static as_iterator * mod_lua_checkiterator(lua_State * l, int index) {
    as_iterator * i = NULL;
    luaL_checktype(l, index, LUA_TUSERDATA);
    i = (as_iterator *) luaL_checkudata(l, index, MOD_LUA_ITERATOR);
    if (i == NULL) luaL_typerror(l, index, MOD_LUA_ITERATOR);
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
 * Thought: Possibly not needed because the external (to lua) 
 * environment should handle the lifecycle of the record.
 */
static int mod_lua_iterator_gc(lua_State * l) {
    as_iterator * i = mod_lua_checkiterator(l, 1);
    as_iterator_free(i);
    return 0;
}


/**
 * iterator table
 */
static const luaL_reg mod_lua_iterator_table[] = {
    {"has_next",        mod_lua_iterator_has_next},
    {"next",            mod_lua_iterator_next},
    {0, 0}
};

/**
 * iterator metatable
 */
static const luaL_reg mod_lua_iterator_metatable[] = {
    {"__gc",            mod_lua_iterator_gc},
    {0, 0}
};

/**
 * Registers the iterator library
 */
int mod_lua_iterator_register(lua_State * l) {

    int table, metatable;

    // register the table
    luaL_register(l, MOD_LUA_ITERATOR, mod_lua_iterator_table);
    table = lua_gettop(l);

    // register the metatable
    luaL_newmetatable(l, MOD_LUA_ITERATOR);
    luaL_register(l, 0, mod_lua_iterator_metatable);
    metatable = lua_gettop(l);

    lua_pushliteral(l, "__index");
    lua_pushvalue(l, table);
    lua_rawset(l, metatable);

    lua_pushliteral(l, "__metatable");
    lua_pushvalue(l, table);
    lua_rawset(l, metatable);
    
    lua_pop(l, 1);

    return 1;
}
