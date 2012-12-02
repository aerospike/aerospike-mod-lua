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

#include "as_aerospike.h"
#include "mod_lua_aerospike.h"
#include "mod_lua_record.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>



#define MOD_LUA_AEROSPIKE "Aerospike"

/**
 * Read the item at index and convert to a aerospike
 */
as_aerospike * mod_lua_toaerospike(lua_State * l, int index) {
    as_aerospike * a = (as_aerospike *) lua_touserdata(l, index);
    if (a == NULL) luaL_typerror(l, index, MOD_LUA_AEROSPIKE);
    return a;
}

/**
 * Push aerospike on to the lua stack
 */
as_aerospike * mod_lua_pushaerospike(lua_State * l, as_aerospike * a) {
    as_aerospike * la = (as_aerospike *) lua_newuserdata(l, sizeof(as_aerospike));
    // *la = *a;
    la->source = a->source;
    la->hooks = a->hooks;
    luaL_getmetatable(l, MOD_LUA_AEROSPIKE);
    lua_setmetatable(l, -2);
    return a;
}

/**
 * Get aerospike from the stack at index
 */
static as_aerospike * mod_lua_checkaerospike(lua_State * l, int index) {
    as_aerospike * a = NULL;
    luaL_checktype(l, index, LUA_TUSERDATA);
    a = (as_aerospike *) luaL_checkudata(l, index, MOD_LUA_AEROSPIKE);
    if (a == NULL) luaL_typerror(l, index, MOD_LUA_AEROSPIKE);
    return a;
}

/**
 * aerospike.create(record) => result<bool>
 */
static int mod_lua_aerospike_create(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    as_rec *        r   = mod_lua_torecord(l, 2);
    
    return as_aerospike_create(a, r);
}

/**
 * aerospike.update(record) => result<bool>
 */
static int mod_lua_aerospike_update(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    as_rec *        r   = mod_lua_torecord(l, 2);
    
    return as_aerospike_update(a, r);
}

/**
 * aerospike.exists(record) => result<bool>
 */
static int mod_lua_aerospike_exists(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    as_rec *        r   = mod_lua_torecord(l, 2);

    return as_aerospike_exists(a, r);
}

/**
 * aerospike.remove(namespace, set, key) => result<bool>
 */
static int mod_lua_aerospike_remove(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    as_rec *        r   = mod_lua_torecord(l, 2);
    
    return as_aerospike_remove(a, r);
}

/**
 * aerospike.log(level, message)
 */
static int mod_lua_aerospike_log(lua_State * l) {
    lua_Debug       ar;
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    int             lvl = luaL_optint(l, 2, 0);
    const char *    msg = luaL_optstring(l, 3, NULL);

    lua_getstack(l, 2, &ar);
    lua_getinfo(l, "nSl", &ar);
    
    as_aerospike_log(a, ++ar.source, ar.currentline, lvl, msg);
    return 0;
}


/**
 * Garbage collection 
 */
static int mod_lua_aerospike_gc(lua_State * l) {
    // as_aerospike * a = mod_lua_checkaerospike(l, 1);
    // as_aerospike_free(a);
    return 0;
}

/**
 * aerospike table
 */
static const luaL_reg mod_lua_aerospike_table[] = {
    {"create",      mod_lua_aerospike_create},
    {"update",      mod_lua_aerospike_update},
    {"exists",       mod_lua_aerospike_exists},
    {"remove",      mod_lua_aerospike_remove},
    {"log",         mod_lua_aerospike_log},
    {0, 0}
};

/**
 * aerospike metatable
 */
static const luaL_reg mod_lua_aerospike_metatable[] = {
    {"__gc",        mod_lua_aerospike_gc},
    {0, 0}
};


/**
 * Registers the aerospike library
 */
int mod_lua_aerospike_register(lua_State * l) {

    int table, metatable;

    // register the table
    luaL_register(l, MOD_LUA_AEROSPIKE, mod_lua_aerospike_table);
    table = lua_gettop(l);

    // register the metatable
    luaL_newmetatable(l, MOD_LUA_AEROSPIKE);
    luaL_register(l, 0, mod_lua_aerospike_metatable);
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
