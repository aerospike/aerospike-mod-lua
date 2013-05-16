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

#include <string.h>
#include <stdio.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <aerospike/as_val.h>

#include <aerospike/mod_lua_val.h>
#include <aerospike/mod_lua_list.h>
#include <aerospike/mod_lua_map.h>
#include <aerospike/mod_lua_record.h>
#include <aerospike/mod_lua_bytes.h>

#include "internal.h"

as_val * mod_lua_takeval(lua_State * l, int i) {
    return mod_lua_toval(l, i);
}

as_val * mod_lua_retval(lua_State * l) {
    return mod_lua_toval(l, -1);
}

/**
 * Reads a val from the Lua stack
 * the val returned includes a refcount that must be freed later
 *
 * @param l the lua_State to read the val from
 * @param i the position of the val on the stack
 * @returns the val if exists, otherwise NULL.
 */
as_val * mod_lua_toval(lua_State * l, int i) {
    switch( lua_type(l, i) ) {
        case LUA_TNUMBER : {
            return (as_val *) as_integer_new((long) lua_tonumber(l, i));
        }
        case LUA_TBOOLEAN : {
            return (as_val *) as_boolean_new(lua_toboolean(l, i));
        }
        case LUA_TSTRING : {
            return (as_val *) as_string_new(strdup(lua_tostring(l, i)), true);
        }
        case LUA_TUSERDATA : {
            mod_lua_box * box = (mod_lua_box *) lua_touserdata(l, i);
            if ( box && box->value ) {
                switch( as_val_type(box->value) ) {
                    case AS_BOOLEAN: 
                    case AS_INTEGER: 
                    case AS_STRING: 
                    case AS_BYTES:
                    case AS_LIST:
                    case AS_MAP:
                    case AS_REC:
                        switch (box->scope) {
                            case MOD_LUA_SCOPE_LUA:
                                as_val_reserve(box->value);
                                return box->value;
                            case MOD_LUA_SCOPE_HOST:
                                return box->value;
                        }
                    default:
                        return NULL;
                }
            }
            else {
                return (as_val *) NULL;
            }
        }
        case LUA_TNIL :
        case LUA_TTABLE :
        case LUA_TFUNCTION :
        case LUA_TLIGHTUSERDATA :
        default:
            return (as_val *) NULL;
    }
}


/**
 * Pushes a val onto the Lua stack
 *
 * @param l the lua_State to push the val onto
 * @param v the val to push on to the stack
 * @returns number of values pushed
 */
int mod_lua_pushval(lua_State * l, const as_val * v) {
    if ( v == NULL ) {
        lua_pushnil(l);
        return 1;
    }
    
    switch( as_val_type(v) ) {
        case AS_BOOLEAN: {
            lua_pushboolean(l, as_boolean_tobool((as_boolean *) v) );
            return 1;
        }
        case AS_INTEGER: {
            lua_pushinteger(l, as_integer_toint((as_integer *) v) );
            return 1;
        }
        case AS_STRING: {
            lua_pushstring(l, as_string_tostring((as_string *) v) );
            return 1;   
        }
        case AS_BYTES: {
            as_val_reserve(v);
            mod_lua_pushbytes(l, (as_bytes *) v);
            return 1;   
        }
        case AS_LIST: {
            as_val_reserve(v);
            mod_lua_pushlist(l, (as_list *) v);
            return 1;   
        }
        case AS_MAP: {
            as_val_reserve(v);
            mod_lua_pushmap(l, (as_map *) v);
            return 1;   
        }
        case AS_REC: {
            as_val_reserve(v);
            mod_lua_pushrecord(l, (as_rec *) v);
            return 1;   
        }
        case AS_PAIR: {
            as_pair * p = (as_pair *) lua_newuserdata(l, sizeof(as_pair));
            *p = *((as_pair *)v);
            return 1;   
        }
        default: {
            lua_pushnil(l);
            return 1;
        }
    }
    return 0;
}



mod_lua_box * mod_lua_newbox(lua_State * l, mod_lua_scope scope, void * value, const char * type) {
    mod_lua_box * box = (mod_lua_box *) lua_newuserdata(l, sizeof(mod_lua_box));
    box->scope = scope;
    box->value = value;
    return box;
}

mod_lua_box * mod_lua_pushbox(lua_State * l, mod_lua_scope scope, void * value, const char * type) {
    mod_lua_box * box = (mod_lua_box *) mod_lua_newbox(l, scope, value, type);
    luaL_getmetatable(l, type);
    lua_setmetatable(l, -2);
    return box;
}

mod_lua_box * mod_lua_tobox(lua_State * l, int index, const char * type) {
    mod_lua_box * box = (mod_lua_box *) lua_touserdata(l, index);
    if (box == NULL && type != NULL ) luaL_typerror(l, index, type);
    return box;
}

mod_lua_box * mod_lua_checkbox(lua_State * l, int index, const char * type) {
    luaL_checktype(l, index, LUA_TUSERDATA);
    mod_lua_box * box = (mod_lua_box *) luaL_checkudata(l, index, type);
    if (box == NULL) luaL_typerror(l, index, type);
    return box;
}

int mod_lua_freebox(lua_State * l, int index, const char * type) {
    mod_lua_box * box = mod_lua_checkbox(l, index, type);
    if ( box != NULL && box->scope == MOD_LUA_SCOPE_LUA && box->value != NULL ) {
        as_val_destroy(box->value);
        box->value = NULL;
    }
    return 0;
}

void * mod_lua_box_value(mod_lua_box * box) {
    return box ? box->value : NULL;
}

