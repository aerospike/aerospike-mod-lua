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

#include <string.h>
#include <stdio.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <aerospike/as_nil.h>
#include <aerospike/as_val.h>

#include <aerospike/mod_lua_val.h>
#include <aerospike/mod_lua_list.h>
#include <aerospike/mod_lua_map.h>
#include <aerospike/mod_lua_record.h>
#include <aerospike/mod_lua_bytes.h>
#include <aerospike/mod_lua_geojson.h>

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
			double d = lua_tonumber(l, i);
			int64_t i64 = (int64_t)d;
			
			if (d == i64) {
				return (as_val*) as_integer_new(i64);
			}
			else {
				return (as_val*) as_double_new(d);
			}
        }
        case LUA_TBOOLEAN : {
            return (as_val *) as_boolean_new(lua_toboolean(l, i));
        }
        case LUA_TSTRING : {
            return (as_val *) as_string_new(cf_strdup(lua_tostring(l, i)), true);
        }
        case LUA_TUSERDATA : {
            mod_lua_box * box = (mod_lua_box *) lua_touserdata(l, i);
            if ( box && box->value ) {
                switch( as_val_type(box->value) ) {
                    case AS_BOOLEAN: 
                    case AS_INTEGER:
					case AS_DOUBLE:
                    case AS_STRING: 
                    case AS_BYTES:
                    case AS_LIST:
                    case AS_MAP:
                    case AS_REC:
                    case AS_GEOJSON:
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
        	return (as_val *)&as_nil;
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
        case AS_DOUBLE: {
            lua_pushnumber(l, as_double_get((as_double*)v));
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
        case AS_GEOJSON: {
            as_val_reserve(v);
            mod_lua_pushgeojson(l, (as_geojson *) v);
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
