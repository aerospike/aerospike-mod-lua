#include "mod_lua_record.h"
#include "mod_lua_val.h"
#include "as_rec.h"
#include "mod_lua_reg.h"

#define OBJECT_NAME "record"
#define CLASS_NAME  "Record"

/**
 * Read the item at index and convert to a record
 */
as_rec * mod_lua_torecord(lua_State * l, int index) {
    as_rec * r = (as_rec *) lua_touserdata(l, index);
    if (r == NULL) luaL_typerror(l, index, CLASS_NAME);
    return r;
}

/**
 * Push a record on to the lua stack
 */
as_rec * mod_lua_pushrecord(lua_State * l, as_rec * r) {
    as_rec * lr = (as_rec *) lua_newuserdata(l, sizeof(as_rec));
    as_rec_init(lr, r->source, r->hooks);
    luaL_getmetatable(l, CLASS_NAME);
    lua_setmetatable(l, -2);
    return r;
}

/**
 * Get the user record from the stack at index
 */
static as_rec * mod_lua_checkrecord(lua_State * l, int index) {
    as_rec * r = NULL;
    luaL_checktype(l, index, LUA_TUSERDATA);
    r = (as_rec *) luaL_checkudata(l, index, CLASS_NAME);
    if (r == NULL) luaL_typerror(l, index, CLASS_NAME);
    return r;
}

/**
 * Get a record metadata
 */
static int mod_lua_record_metadata(lua_State * l) {
    lua_pushstring(l,"hi");
    return 0;
}

/**
 * Get a value from the named bin
 */
static int mod_lua_record_index(lua_State * l) {
    as_rec * r = mod_lua_checkrecord(l, 1);
    const char * n = luaL_optstring(l, 2, 0);
    const as_val * v = as_rec_get(r, n);
    mod_lua_pushval(l, v);
    return 1;
}

/**
 * Set a value in the named bin
 */
static int mod_lua_record_newindex(lua_State * l) {
    as_rec * r = mod_lua_checkrecord(l, 1);
    const char * name = luaL_optstring(l, 2, 0);
    as_val * value = (as_val *) mod_lua_toval(l, 3);

    if ( value == NULL ) {
        as_rec_remove(r, name);
    }
    else {
        as_rec_set(r, name, value);
    }
    return 0;
}

/**
 * Garbage collection 
 */
static int mod_lua_record_gc(lua_State * l) {
    as_rec * r = mod_lua_checkrecord(l, 1);
    as_rec_free(r);
    return 0;
}

/*******************************************************************************
 * ~~~ Object ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 ******************************************************************************/

static const luaL_reg object_table[] = {
    {"metadata",        mod_lua_record_metadata},
    {0, 0}
};

static const luaL_reg object_metatable[] = {
    // {"__index",         mod_lua_record_index},
    {0, 0}
};

/*******************************************************************************
 * ~~~ Class ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 ******************************************************************************/

static const luaL_reg class_table[] = {
    {0, 0}
};

static const luaL_reg class_metatable[] = {
    {"__index",         mod_lua_record_index},
    {"__newindex",      mod_lua_record_newindex},
    {"__gc",            mod_lua_record_gc},
    {0, 0}
};

/*******************************************************************************
 * ~~~ Register ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 ******************************************************************************/

int mod_lua_record_register(lua_State * l) {
    mod_lua_reg_object(l, OBJECT_NAME, object_table, object_metatable);
    mod_lua_reg_class(l, CLASS_NAME, NULL, class_metatable);
    return 1;
}
