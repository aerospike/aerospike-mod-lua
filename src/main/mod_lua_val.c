#include "mod_lua_val.h"
#include "mod_lua_list.h"
#include "mod_lua_map.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <string.h>
#include <stdio.h>

#define LOG(m) \
    printf("%s:%d  -- %s\n",__FILE__,__LINE__, m);

as_val * mod_lua_retval(lua_State * l) {
    int i = -1;
    switch( lua_type(l, i) ) {
        case LUA_TNUMBER : {
            return (as_val *) as_integer_new((long) lua_tonumber(l, i));
        }
        case LUA_TBOOLEAN : {
            return (as_val *) as_boolean_new(lua_toboolean(l, i));
        }
        case LUA_TSTRING : {
            return (as_val *) as_string_new(strdup(lua_tostring(l, i)));
        }
        case LUA_TUSERDATA : {
            mod_lua_box * box = (mod_lua_box *) lua_touserdata(l, i);
            if ( box && box->value ) {
                switch ( as_val_type(box->value) ) {
                    case AS_BOOLEAN:
                    case AS_INTEGER:
                    case AS_STRING:
                    case AS_LIST:
                    case AS_MAP:
                    case AS_PAIR: {
                        as_val * value = box->value;
                        box->value = NULL;
                        box->scope = MOD_LUA_SCOPE_LUA;
                        return value;
                    }
                    default:
                        return NULL;
                }
            }
            else if ( box ) {
                box->scope = MOD_LUA_SCOPE_LUA;
            }
            else {
                return (as_val *) NULL;
            }
        }
        case LUA_TNIL :
            return (as_val *) NULL;
        case LUA_TTABLE :
            return (as_val *) NULL;
        case LUA_TFUNCTION :
            return (as_val *) NULL;
        case LUA_TLIGHTUSERDATA :
            return (as_val *) NULL;
        default:
            return (as_val *) NULL;
    }
}



/**
 * Reads a val from the Lua stack
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
            return (as_val *) as_string_new(strdup(lua_tostring(l, i)));
        }
        case LUA_TUSERDATA : {
            mod_lua_box * box = (mod_lua_box *) lua_touserdata(l, i);
            if ( box && box->value ) {
                switch ( as_val_type(box->value) ) {
                    case AS_BOOLEAN:
                    case AS_INTEGER:
                    case AS_STRING:
                    case AS_LIST:
                    case AS_MAP:
                    case AS_PAIR:
                        return box->value;
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
int mod_lua_pushval(lua_State * l, mod_lua_scope scope, const as_val * v) {
    switch( as_val_type(v) ) {
        case AS_BOOLEAN: {
            lua_pushboolean(l, as_boolean_tobool((as_boolean *) v) );
            if ( scope == MOD_LUA_SCOPE_LUA ) {
                as_val_free(v);
            }
            return 1;
        }
        case AS_INTEGER: {
            lua_pushinteger(l, as_integer_toint((as_integer *) v) );
            if ( scope == MOD_LUA_SCOPE_LUA ) {
                as_val_free(v);
            }
            return 1;
        }
        case AS_STRING: {
            lua_pushstring(l, as_string_tostring((as_string *) v) );
            if ( scope == MOD_LUA_SCOPE_LUA ) {
                as_val_free(v);
            }
            return 1;   
        }
        case AS_LIST: {
            mod_lua_pushlist(l, scope, (as_list *) v);
            return 1;   
        }
        case AS_MAP: {
            mod_lua_pushmap(l, scope, (as_map *) v);
            return 1;   
        }
        case AS_PAIR: {
            as_pair * p = (as_pair *) lua_newuserdata(l, sizeof(as_pair));
            *p = *((as_pair *)v);
            // as_val * val = (as_val *) lua_newuserdata(l, sizeof(as_val));
            // *val = *((as_val *)v);
            return 1;   
        }
        default: {
            lua_pushnil(l);
            return 1;
        }
    }
    return 0;
}
