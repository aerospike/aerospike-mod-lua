#include "mod_lua_record.h"
#include "mod_lua_val.h"
#include "as_rec.h"
#include "mod_lua_reg.h"

#include "as_val.h"
#include "internal.h"

/*******************************************************************************
 * MACROS
 ******************************************************************************/

#define OBJECT_NAME "record"
#define CLASS_NAME  "Record"

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 * Read the item at index and convert to a record
 */
as_rec * mod_lua_torecord(lua_State * l, int index) {
    mod_lua_box * box = mod_lua_tobox(l, index, CLASS_NAME);
    return (as_rec *) mod_lua_box_value(box);
}

/**
 * Push a record on to the lua stack
 */
as_rec * mod_lua_pushrecord(lua_State * l, as_rec * r) {
    mod_lua_box * box = mod_lua_pushbox(l, MOD_LUA_SCOPE_HOST, r, CLASS_NAME);
    return (as_rec *) mod_lua_box_value(box);
}

/**
 * Get the user record from the stack at index
 */
static as_rec * mod_lua_checkrecord(lua_State * l, int index) {
    mod_lua_box * box = mod_lua_checkbox(l, index, CLASS_NAME);
    return (as_rec *) mod_lua_box_value(box);
}

/**
 * Garbage collection 
 */
static int mod_lua_record_gc(lua_State * l) {
    mod_lua_freebox(l, 1, CLASS_NAME);
    return 0;
}

/**
 * Get a record ttl:
 *      record.ttl(r)
 */
static int mod_lua_record_ttl(lua_State * l) {
    as_rec * rec = (as_rec *) mod_lua_checkrecord(l, 1);
    lua_pushinteger(l, as_rec_ttl(rec));
    return 1;
}

/**
 * Get a record generation:
 *      record.gen(r)
 */
static int mod_lua_record_gen(lua_State * l) {
    as_rec * rec = (as_rec *) mod_lua_checkrecord(l, 1);
    lua_pushinteger(l, as_rec_gen(rec));
    return 1;
}

/**
 * Get a value from the named bin
 */
static int mod_lua_record_index(lua_State * l) {
    LOG("mod_lua_record_index: begin");
    mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
    as_rec *        rec     = (as_rec *) mod_lua_box_value(box);
    const char *    name    = luaL_optstring(l, 2, 0);
    int             rc      = 0;
    if ( name != NULL ) {
        LOG("mod_lua_record_index: name is not null");
        as_val * value  = (as_val *) as_rec_get(rec, name);
        if ( value != NULL ) {
            LOG("mod_lua_record_index: value is not null, returning value");
            mod_lua_pushval(l, value);
            rc = 1;
        }
        else {
            LOG("mod_lua_record_index: value is null, returning nil");
            lua_pushnil(l);
        }
    }
    else {
        LOG("mod_lua_record_index: name is null, returning nil");
        lua_pushnil(l);
    }
    LOG("mod_lua_record_index: end");
    return rc;
}

/**
 * Set a value in the named bin
 */
static int mod_lua_record_newindex(lua_State * l) {
    LOG("mod_lua_record_newindex: begin");
    as_rec *        rec     = mod_lua_checkrecord(l, 1);
    const char *    name    = luaL_optstring(l, 2, 0);
    if ( name != NULL ) {
        LOG("mod_lua_record_newindex: name is not null");
        as_val * value = (as_val *) mod_lua_toval(l, 3);
        if ( value != NULL ) {
            LOG("mod_lua_record_newindex: value is not null, setting bin");
            as_rec_set(rec, name, value);
            as_val_destroy(value);
        }
        else {
            LOG("mod_lua_record_newindex: value is null, removing bin");
            as_rec_remove(rec, name);
        }
    }
    else {
        LOG("mod_lua_record_newindex: name is null");
    }
    LOG("mod_lua_record_newindex: end");
    return 0;
}

/******************************************************************************
 * OBJECT TABLE
 *****************************************************************************/

static const luaL_reg object_table[] = {
    {"ttl",        mod_lua_record_ttl},
    {"gen",        mod_lua_record_gen},
    {0, 0}
};

static const luaL_reg object_metatable[] = {
    // {"__index",         mod_lua_record_index},
    {0, 0}
};

/******************************************************************************
 * CLASS TABLE
 *****************************************************************************/

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
