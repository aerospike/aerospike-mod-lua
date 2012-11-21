#include "mod_lua_record.h"
#include "mod_lua_val.h"

#include "as_rec.h"
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#define LOG(m) \
    // printf("%s:%d  -- %s\n",__FILE__,__LINE__, m);

#define MOD_LUA_RECORD "Record"

/**
 * Read the item at index and convert to a record
 */
as_rec * mod_lua_torecord(lua_State * l, int index) {
    as_rec * r = (as_rec *) lua_touserdata(l, index);
    if (r == NULL) luaL_typerror(l, index, MOD_LUA_RECORD);
    return r;
}

/**
 * Push a record on to the lua stack
 */
as_rec * mod_lua_pushrecord(lua_State * l, as_rec * r) {
    as_rec * lr = (as_rec *) lua_newuserdata(l, sizeof(as_rec));
    as_rec_update(lr, r->source, r->hooks);
    luaL_getmetatable(l, MOD_LUA_RECORD);
    lua_setmetatable(l, -2);
    return r;
}

/**
 * Get the user record from the stack at index
 */
static as_rec * mod_lua_checkrecord(lua_State * l, int index) {
    as_rec * r = NULL;
    luaL_checktype(l, index, LUA_TUSERDATA);
    r = (as_rec *) luaL_checkudata(l, index, MOD_LUA_RECORD);
    if (r == NULL) luaL_typerror(l, index, MOD_LUA_RECORD);
    return r;
}

/**
 * Get a value from the named bin
 */
static int mod_lua_record_index(lua_State * l) {
    as_rec * r = mod_lua_checkrecord(l, 1);
    const char * n = luaL_optstring(l, 2, 0);
    const as_val * v = as_rec_get(r, n);
    mod_lua_pushval(l, v);
    return 1;
}

/**
 * Set a value in the named bin
 */
static int mod_lua_record_newindex(lua_State * l) {
    as_rec * r = mod_lua_checkrecord(l, 1);
    const char * name = luaL_optstring(l, 2, 0);
    as_val * value = (as_val *) mod_lua_toval(l, 3);

    if ( value == NULL ) {
        as_rec_remove(r, name);
    }
    else {
        as_rec_set(r, name, value);
    }
    return 0;
}

/**
 * Garbage collection 
 * Thought: Possibly not needed because the external (to lua) 
 * environment should handle the lifecycle of the record.
 */
static int mod_lua_record_gc(lua_State * l) {
    as_rec * r = mod_lua_checkrecord(l, 1);
    as_rec_free(r);
    return 0;
}

/**
 * record table
 */
static const luaL_reg mod_lua_record_table[] = {
    {0, 0}
};

/**
 * record metatable
 */
static const luaL_reg mod_lua_record_metatable[] = {
    {"__index",     mod_lua_record_index},
    {"__newindex",  mod_lua_record_newindex},
    {"__gc",        mod_lua_record_gc},
    {0, 0}
};

/**
 * Registers the record type
 */
int mod_lua_record_register(lua_State * l) {

    int table, metatable;

    // register the table
    luaL_register(l, MOD_LUA_RECORD, mod_lua_record_table);
    table = lua_gettop(l);

    // register the metatable
    luaL_newmetatable(l, MOD_LUA_RECORD);
    luaL_register(l, 0, mod_lua_record_metatable);
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
