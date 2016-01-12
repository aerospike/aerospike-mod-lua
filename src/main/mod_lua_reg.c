/* 
 * Copyright 2008-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#include <aerospike/as_val.h>

#include <aerospike/mod_lua_reg.h>

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
