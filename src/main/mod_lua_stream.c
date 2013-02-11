/**
 * Provides a lua interface to the aerospike struct and functions
 *
 *
 *      aerospike.get(namespace, set, key): result<record>
 *      aerospike.put(namespace, set, key, table)
 *      aerospike.remove(namespace, set, key): result<bool>
 *      aerospike.update(record): result<record>
 *
 *
 */

#include "mod_lua_stream.h"
#include "mod_lua_iterator.h"

#include "as_val.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#define MOD_LUA_STREAM_TABLE "stream"
#define MOD_LUA_STREAM_METATABLE "Stream"

/**
 * Read the item at index and convert to a stream
 */
as_stream * mod_lua_tostream(lua_State * l, int index) {
    as_stream * s = (as_stream *) lua_touserdata(l, index);
    if (s == NULL) luaL_typerror(l, index, MOD_LUA_STREAM_METATABLE);
    return s;
}

/**
 * Push a stream on to the lua stack
 */
as_stream * mod_lua_pushstream(lua_State * l, as_stream * s) {
    as_stream * ls = (as_stream *) lua_newuserdata(l, sizeof(as_stream));
    *ls = *s;
    luaL_getmetatable(l, MOD_LUA_STREAM_METATABLE);
    lua_setmetatable(l, -2);
    return ls;
}

/**
 * Get the stream from the stack at index
 */
static as_stream * mod_lua_checkstream(lua_State * l, int index) {
    as_stream * s = NULL;
    luaL_checktype(l, index, LUA_TUSERDATA);
    s = (as_stream *) luaL_checkudata(l, index, MOD_LUA_STREAM_METATABLE);
    if (s == NULL) luaL_typerror(l, index, MOD_LUA_STREAM_METATABLE);
    return s;
}

/**
 * Gets an iterator for a stream
 *
 *    stream.iterator(s: Stream): Iterator
 * 
 */
static int mod_lua_stream_iterator(lua_State * l) {
    as_stream * s = mod_lua_checkstream(l, 1);
    as_iterator * i = as_stream_iterator_new(s);
    mod_lua_pushiterator(l, i);
    return 1;
}

/**
 * stream table
 *    stream.iterator(s: Stream): Iterator
 */
static const luaL_reg mod_lua_stream_table[] = {
    {"iterator",        mod_lua_stream_iterator},
    {0, 0}
};

/**
 * Stream metatable
 */
static const luaL_reg mod_lua_stream_metatable[] = {
    {0, 0}
};

/**
 * Registers the table and metatable
 */
int mod_lua_stream_register(lua_State * l) {
    
    int table, metatable;

    // register the table
    luaL_register(l, MOD_LUA_STREAM_TABLE, mod_lua_stream_table);
    table = lua_gettop(l);

    // register the metatable
    luaL_newmetatable(l, MOD_LUA_STREAM_METATABLE);
    luaL_register(l, 0, mod_lua_stream_metatable);
    metatable = lua_gettop(l);

    // lua_pushliteral(l, "__index");
    // lua_pushvalue(l, table);
    // lua_rawset(l, metatable);

    lua_pushliteral(l, "__metatable");
    lua_pushvalue(l, table);
    lua_rawset(l, metatable);
    
    lua_pop(l, 1);

    return 1;
}
