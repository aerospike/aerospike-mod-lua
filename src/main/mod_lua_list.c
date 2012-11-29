#include "mod_lua_val.h"
#include "mod_lua_list.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <stdio.h>

#define MOD_LUA_LIST_TABLE "list"
#define MOD_LUA_LIST_METATABLE "List"

as_list * mod_lua_tolist(lua_State * l, int index) {
    as_list * list = (as_list *) lua_touserdata(l, index);
    if (list == NULL) luaL_typerror(l, index, MOD_LUA_LIST_METATABLE);
    return list;
}

as_list * mod_lua_pushlist(lua_State * l, as_list * i) {
    as_list * list = (as_list *) lua_newuserdata(l, sizeof(as_list));
    *list = *i;
    luaL_getmetatable(l, MOD_LUA_LIST_METATABLE);
    lua_setmetatable(l, -2);
    return list;
}

static as_list * mod_lua_checklist(lua_State * l, int index) {
    as_list * list = NULL;
    luaL_checktype(l, index, LUA_TUSERDATA);
    list = (as_list *) luaL_checkudata(l, index, MOD_LUA_LIST_METATABLE);
    if (list == NULL) luaL_typerror(l, index, MOD_LUA_LIST_METATABLE);
    return list;
}


static int mod_lua_list_append(lua_State * l) {
    as_list *   list    = mod_lua_checklist(l, 1);
    as_val *    value   = mod_lua_toval(l, 2);

    as_list_append(list,value);

    return 0;
}

static int mod_lua_list_prepend(lua_State * l) {
    as_list *   list    = mod_lua_checklist(l, 1);
    as_val *    value   = mod_lua_toval(l, 2);

    as_list_prepend(list,value);

    return 0;
}

static int mod_lua_list_new(lua_State * l) {
    printf("mod_lua_list_new!!!!!\n");
    // as_list *   list    = mod_lua_checklist(l, 1);
    // as_list_free(list);
    return 0;
}


static int mod_lua_list_size(lua_State * l) {
    as_list *   list    = mod_lua_checklist(l, 1);
    uint32_t    size    = as_list_size(list);
    lua_pushinteger(l, size);
    return 1;
}

static int mod_lua_list_gc(lua_State * l) {
    // as_list *   list    = mod_lua_checklist(l, 1);
    // as_list_free(list);
    return 0;
}

static int mod_lua_list_index(lua_State * l) {
    printf("mod_lua_list_index!!!!!\n");
    as_list *       list    = mod_lua_checklist(l, 1);
    const uint32_t  i       = (uint32_t) luaL_optlong(l, 2, 0);
    const as_val *  value   = as_list_get(list, i-1);
    mod_lua_pushval(l, value);
    return 1;
}

static int mod_lua_list_newindex(lua_State * l) {
    as_list *   list    = mod_lua_checklist(l, 1);
    uint32_t    i       = (uint32_t) luaL_optlong(l, 2, 0);
    as_val *    value   = mod_lua_toval(l, 3);
    
    if ( value != NULL ) {
        // one day, we will remove values
    }
    else {
        as_list_set(list, i, value);
    }
    return 0;
}

/**
 * iterator table
 */
static const luaL_reg mod_lua_list_table[] = {
    {"append",          mod_lua_list_append},
    {"prepend",         mod_lua_list_prepend},
    {"size",            mod_lua_list_size},
    {"new",             mod_lua_list_new},
    {0, 0}
};

/**
 * iterator metatable
 */
static const luaL_reg mod_lua_list_metatable[] = {
    {"__gc",            mod_lua_list_gc},
    {"__index",         mod_lua_list_index},
    {"__newindex",      mod_lua_list_newindex},
    // {"__call",          mod_lua_list_call},
    {0, 0}
};

/**
 * Registers the iterator library
 */
int mod_lua_list_register(lua_State * l) {

    int table, metatable;

    // register the table
    luaL_register(l, MOD_LUA_LIST_TABLE, mod_lua_list_table);
    table = lua_gettop(l);

    // register the metatable
    luaL_newmetatable(l, MOD_LUA_LIST_METATABLE);
    luaL_register(l, 0, mod_lua_list_metatable);
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
