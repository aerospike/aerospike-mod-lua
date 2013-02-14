#include "mod_lua_val.h"
#include "mod_lua_stream.h"
#include "mod_lua_reg.h"
#include "as_val.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

/*******************************************************************************
 * MACROS
 ******************************************************************************/

#define OBJECT_NAME "stream"
#define CLASS_NAME "Stream"

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

as_stream * mod_lua_tostream(lua_State * l, int index) {
    mod_lua_box * box = mod_lua_tobox(l, index, CLASS_NAME);
    return (as_stream *) mod_lua_box_value(box);
}

as_stream * mod_lua_pushstream(lua_State * l, as_stream * stream) {
    mod_lua_box * box = mod_lua_pushbox(l, MOD_LUA_SCOPE_LUA, stream, CLASS_NAME);
    return (as_stream *) mod_lua_box_value(box);
}

static as_stream * mod_lua_checkstream(lua_State * l, int index) {
    mod_lua_box * box = mod_lua_checkbox(l, index, CLASS_NAME);
    return (as_stream *) mod_lua_box_value(box);
}

static int mod_lua_stream_gc(lua_State * l) {
    mod_lua_freebox(l, 1, CLASS_NAME);
    return 0;
}

static int mod_lua_stream_tostring(lua_State * l) {
    as_stream * stream = mod_lua_tostream(l, 1);
    char str[128] = { '\0' };
    snprintf(str, 128, "Stream<%p>", stream);
    lua_pushstring(l, str);
    return 1;
}

static int mod_lua_stream_read(lua_State * l) {
    as_stream * stream = mod_lua_tostream(l, 1);
    if ( stream ) {
        as_val * val = as_stream_read(stream);
        mod_lua_pushval(l, val);
        return 1;
    }
    else {
        lua_pushnil(l);
        return 1;
    }
}

static int mod_lua_stream_readable(lua_State * l) {
    as_stream * stream = mod_lua_tostream(l, 1);
    if ( stream ) {
        lua_pushboolean(l, as_stream_readable(stream));
        return 1;
    }
    else {
        lua_pushboolean(l, false);
        return 1;
    }
}

static int mod_lua_stream_write(lua_State * l) {
    as_stream * stream = mod_lua_tostream(l, 1);
    as_val * val = mod_lua_toval(l, 2);
    if ( stream && val ) {
        int rc = as_stream_write(stream, val);
        lua_pushinteger(l, rc);
        return 1;
    }
    else {
        lua_pushinteger(l, AS_STREAM_ERR);
        return 1;
    }
}

static int mod_lua_stream_writable(lua_State * l) {
    as_stream * stream = mod_lua_tostream(l, 1);
    if ( stream ) {
        lua_pushboolean(l, as_stream_readable(stream));
        return 1;
    }
    else {
        lua_pushboolean(l, false);
        return 1;
    }
}

/*******************************************************************************
 * OBJECT TABLE
 ******************************************************************************/

static const luaL_reg object_table[] = {
    {"read",            mod_lua_stream_write},
    {"write",           mod_lua_stream_read},
    {"readable",        mod_lua_stream_readable},
    {"writable",        mod_lua_stream_writable},
    {"tostring",        mod_lua_stream_tostring},
    {0, 0}
};

static const luaL_reg object_metatable[] = {
    {0, 0}
};

/*******************************************************************************
 * CLASS TABLE
 ******************************************************************************/

static const luaL_reg class_table[] = {
    {0, 0}
};

static const luaL_reg class_metatable[] = {
    {"__tostring",      mod_lua_stream_tostring},
    {"__gc",            mod_lua_stream_gc},
    {0, 0}
};

/*******************************************************************************
 * REGISTER
 ******************************************************************************/

int mod_lua_stream_register(lua_State * l) {
    mod_lua_reg_object(l, OBJECT_NAME, object_table, object_metatable);
    mod_lua_reg_class(l, CLASS_NAME, NULL, class_metatable);
    return 1;
}
