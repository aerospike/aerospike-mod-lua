#include "mod_lua_record.h"
#include "mod_lua_val.h"
#include "mod_lua_bytes.h"
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
    // I am hoping the following is correct use of the is_malloc flag
    mod_lua_box * box = mod_lua_pushbox(l, r->_.is_malloc ? MOD_LUA_SCOPE_LUA : MOD_LUA_SCOPE_HOST, r, CLASS_NAME);
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
 * Get a record digest:
 *      record.digest(r)
 */
static int mod_lua_record_digest(lua_State * l) {
    as_rec * rec = (as_rec *) mod_lua_checkrecord(l, 1);
	as_bytes * b = as_rec_digest(rec);
	mod_lua_pushbytes(l, b);
    return 1;
}

/**
 * Get a record numbins:
 *      record.numbins(r)
 */
static int mod_lua_record_numbins(lua_State * l) {
    as_rec * rec = (as_rec *) mod_lua_checkrecord(l, 1);
    lua_pushinteger(l, as_rec_numbins(rec));
    return 1;
}

/**
 * Get a value from the named bin
 */
static int mod_lua_record_index(lua_State * l) {
    mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
    as_rec *        rec     = (as_rec *) mod_lua_box_value(box);
    const char *    name    = luaL_optstring(l, 2, 0);
    if ( name != NULL ) {
        as_val * value  = (as_val *) as_rec_get(rec, name);
        if ( value != NULL ) {
            mod_lua_pushval(l, value);
            return 1;
        }
        else {
            lua_pushnil(l);
            return 1;
        }
    }
    else {
        lua_pushnil(l);
        return 1;
    }
}

/**
 * Set a value in the named bin
 */
static int mod_lua_record_newindex(lua_State * l) {
    as_rec *        rec     = mod_lua_checkrecord(l, 1);
    const char *    name    = luaL_optstring(l, 2, 0);
    if ( name != NULL ) {
        // reference to this value is created by mod_lua_toval
        // then stashed in the record cache
        as_val * value = (as_val *) mod_lua_toval(l, 3);
        if ( value != NULL ) {
            as_rec_set(rec, name, value);
        }
        else {
            as_rec_remove(rec, name);
        }
    }
    return 0;
}

/******************************************************************************
 * OBJECT TABLE
 *****************************************************************************/

static const luaL_reg object_table[] = {
    {"ttl",        mod_lua_record_ttl},
    {"gen",        mod_lua_record_gen},
    {"digest",     mod_lua_record_digest},
    {"numbins",    mod_lua_record_numbins},
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
