#include "mod_lua_val.h"
#include "mod_lua_map.h"
#include "mod_lua_reg.h"

#define OBJECT_NAME "map"
#define CLASS_NAME  "Map"

as_map * mod_lua_tomap(lua_State * l, int index) {
    as_map * map = (as_map *) lua_touserdata(l, index);
    if (map == NULL) luaL_typerror(l, index, CLASS_NAME);
    return map;
}

as_map * mod_lua_pushmap(lua_State * l, as_map * i) {
    as_map * map = (as_map *) lua_newuserdata(l, sizeof(as_map));
    *map = *i;
    luaL_getmetatable(l, CLASS_NAME);
    lua_setmetatable(l, -2);
    return map;
}

static as_map * mod_lua_checkmap(lua_State * l, int index) {
    as_map * map = NULL;
    luaL_checktype(l, index, LUA_TUSERDATA);
    map = (as_map *) luaL_checkudata(l, index, CLASS_NAME);
    if (map == NULL) luaL_typerror(l, index, CLASS_NAME);
    return map;
}

static int mod_lua_map_size(lua_State * l) {
    as_map *    map     = mod_lua_checkmap(l, 1);
    uint32_t    size    = as_map_size(map);
    lua_pushinteger(l, size);
    return 1;
}

static int mod_lua_map_new(lua_State * l) {
    as_map * map = as_hashmap_new(320);
    int n = lua_gettop(l);
    if ( n == 2 && lua_type(l, 2) == LUA_TTABLE) {
        lua_pushnil(l);
        while ( lua_next(l, 2) != 0 ) {
            as_val * k = mod_lua_toval(l, -2);
            as_val * v = mod_lua_toval(l, -1);
            if ( !k || !v ) {
                as_val_free(k);
                as_val_free(v);
                continue;
            }
            as_map_set(map, k, v);
            lua_pop(l, 1);
        }
    }
    mod_lua_pushmap(l, map);
    return 1;
}

static int mod_lua_map_gc(lua_State * l) {
    return 0;
}

static int mod_lua_map_index(lua_State * l) {
    as_map *    map     = mod_lua_checkmap(l, 1);
    as_val *    key     = mod_lua_toval(l, 2);
    as_val *    val     = as_map_get(map, key);
    mod_lua_pushval(l, val);
    return 1;
}

static int mod_lua_map_newindex(lua_State * l) {
    as_map *    map     = mod_lua_checkmap(l, 1);
    as_val *    key     = mod_lua_toval(l, 2);
    as_val *    val     = mod_lua_toval(l, 3);
    
    if ( val == NULL ) {
        // one day, we will remove values
    }
    else {
        as_map_set(map, key, val);
    }
    return 0;
}

/*******************************************************************************
 * ~~~ Object ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 ******************************************************************************/

static const luaL_reg object_table[] = {
    {"size",            mod_lua_map_size},
    {0, 0}
};

static const luaL_reg object_metatable[] = {
    {"__call",          mod_lua_map_new},
    {0, 0}
};

/*******************************************************************************
 * ~~~ Class ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 ******************************************************************************/

static const luaL_reg class_table[] = {
    {0, 0}
};

static const luaL_reg class_metatable[] = {
    {"__index",         mod_lua_map_index},
    {"__newindex",      mod_lua_map_newindex},
    {"__gc",            mod_lua_map_gc},
    {0, 0}
};

/*******************************************************************************
 * ~~~ Register ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 ******************************************************************************/

int mod_lua_map_register(lua_State * l) {
    mod_lua_reg_object(l, OBJECT_NAME, object_table, object_metatable);
    mod_lua_reg_class(l, CLASS_NAME, NULL, class_metatable);
    return 1;
}
