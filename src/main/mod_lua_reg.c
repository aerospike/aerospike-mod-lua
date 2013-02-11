#include "mod_lua_reg.h"

#include "as_val.h"
#include "internal.h"

int mod_lua_reg_object(lua_State * l, const char * name, const luaL_reg * table, const luaL_reg * metatable) {

    int tableId = 0, metatableId = 0;

    luaL_register(l, name, table);
    tableId = lua_gettop(l);

    lua_newtable(l);
    luaL_register(l, 0, metatable);
    metatableId = lua_gettop(l);

    lua_pushvalue(l, tableId);
    lua_pushvalue(l, metatableId);
    lua_setmetatable(l, 0);

    lua_pushliteral(l, "__metatable");
    lua_pushvalue(l, tableId);
    lua_rawset(l, metatableId);


    lua_pop(l, 1);

    return 0;
}

int mod_lua_reg_class(lua_State * l, const char * name, const luaL_reg * table, const luaL_reg * metatable) {

    int tableId = 0, metatableId = 0;

    if ( table ) {
        luaL_register(l, name, table);
        tableId = lua_gettop(l);
    }

    if ( metatable ) {
        luaL_newmetatable(l, name);
        luaL_register(l, 0, metatable);
        metatableId = lua_gettop(l);
    }

    if ( table && metatable ) {
        lua_pushliteral(l, "__index");
        lua_pushvalue(l, tableId);
        lua_rawset(l, metatableId);

        lua_pushliteral(l, "__metatable");
        lua_pushvalue(l, tableId);
        lua_rawset(l, metatableId);

        lua_pop(l, 1);
    }

    return 0;
}
