#include "mod_lua_val.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <string.h>

/**
 * Reads a val from the Lua stack
 *
 * @param l the lua_State to read the val from
 * @param i the position of the val on the stack
 * @returns the val if exists, otherwise NULL.
 */
as_val * mod_lua_toval(lua_State * l, int i) {
    switch( lua_type(l, i) ) {
        case LUA_TNUMBER :
            return (as_val *) as_integer_new((long) lua_tonumber(l, i));
        case LUA_TBOOLEAN :
            return (as_val *) as_boolean_new(lua_toboolean(l, i));
        case LUA_TSTRING :
            return (as_val *) as_string_new(strdup(lua_tostring(l, i)));
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
 * @returns 0 if successful, otherwise 1
 */
int mod_lua_pushval(lua_State * l, const as_val * v) {
    switch( as_val_type(v) ) {
        case AS_INTEGER:
            lua_pushnumber(l, as_integer_toint(as_integer_fromval(v)) );
            return 0;
        case AS_STRING:
            lua_pushstring(l, as_string_tostring(as_string_fromval(v)) );
            return 0;
        default:
            lua_pushnil(l);
            return 0;
    }
    return 0;
}
