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
