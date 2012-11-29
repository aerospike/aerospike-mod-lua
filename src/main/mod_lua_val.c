#include "mod_lua_val.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <string.h>
#include <stdio.h>

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
        case LUA_TNIL :
        case LUA_TTABLE :
        case LUA_TFUNCTION :
        case LUA_TUSERDATA :
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
    switch( as_val_type(v) ) {
        case AS_INTEGER: {
            lua_pushinteger(l, as_integer_toint((as_integer *) v) );
            return 1;
        }
        case AS_STRING: {
            lua_pushstring(l, as_string_tostring((as_string *) v) );
            return 1;   
        }
        case AS_LIST: {
            printf("mod_lua_pushval: AS_LIST\n");
            mod_lua_pushlist(l, (as_list *) v);
            return 1;   
        }
        default: {
            lua_pushnil(l);
            return 1;
        }
    }
    return 0;
}
