#include "as_val.h"
#include "as_aerospike.h"
#include "internal.h"

#include "mod_lua_aerospike.h"
#include "mod_lua_record.h"
#include "mod_lua_val.h"
#include "mod_lua_reg.h"

/*******************************************************************************
 * MACROS
 ******************************************************************************/

#define CLASS_NAME "Aerospike"

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 * Read the item at index and convert to a aerospike
 */
as_aerospike * mod_lua_toaerospike(lua_State * l, int index) {
    mod_lua_box * box = mod_lua_tobox(l, index, CLASS_NAME);
    return (as_aerospike *) mod_lua_box_value(box);
}

/**
 * Push aerospike on to the lua stack
 */
as_aerospike * mod_lua_pushaerospike(lua_State * l, as_aerospike * a) {
    mod_lua_box * box = mod_lua_pushbox(l, MOD_LUA_SCOPE_HOST, a, CLASS_NAME);
    return (as_aerospike *) mod_lua_box_value(box);
}

/**
 * Get aerospike from the stack at index
 */
static as_aerospike * mod_lua_checkaerospike(lua_State * l, int index) {
    mod_lua_box * box = mod_lua_checkbox(l, index, CLASS_NAME);
    return (as_aerospike *) mod_lua_box_value(box);
}

/**
 * Garbage collection 
 */
static int mod_lua_aerospike_gc(lua_State * l) {
    LOG("mod_lua_aerospike_gc: begin");
    mod_lua_freebox(l, 1, CLASS_NAME);
    LOG("mod_lua_aerospike_gc: end");
    return 0;
}


/**
 * aerospike.create(record) => result<bool>
 */
static int mod_lua_aerospike_rec_create(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    as_rec *        r   = mod_lua_torecord(l, 2);
    int             rc  = as_aerospike_rec_create(a, r);
    lua_pushinteger(l, rc);
    return 1;
}

/**
 * aerospike.update(record) => result<bool>
 */
static int mod_lua_aerospike_rec_update(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    as_rec *        r   = mod_lua_torecord(l, 2);
    int             rc  = as_aerospike_rec_update(a, r);
    lua_pushinteger(l, rc);
    return 1;
}

/**
 * aerospike.exists(record) => result<bool>
 */
static int mod_lua_aerospike_rec_exists(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    as_rec *        r   = mod_lua_torecord(l, 2);
    int             rc  = as_aerospike_rec_exists(a, r);
    lua_pushboolean(l, rc == 1);
    return 1;
}

/**
 * aerospike.remove(namespace, set, key) => result<bool>
 */
static int mod_lua_aerospike_rec_remove(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    as_rec *        r   = mod_lua_torecord(l, 2);
    int             rc  = as_aerospike_rec_remove(a, r);
    lua_pushinteger(l, rc);
    return 1;
}

/**
 * aerospike.log(level, message)
 */
static int mod_lua_aerospike_log(lua_State * l) {
    lua_Debug       ar;
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    int             lvl = luaL_optint(l, 2, 0);
    const char *    msg = luaL_optstring(l, 3, NULL);

    lua_getstack(l, 2, &ar);
    lua_getinfo(l, "nSl", &ar);
    
    as_aerospike_log(a, ++ar.source, ar.currentline, lvl, msg);
    return 0;
}

/******************************************************************************
 * CLASS TABLE
 *****************************************************************************/

static const luaL_reg class_table[] = {
    {"create",      mod_lua_aerospike_rec_create},
    {"update",      mod_lua_aerospike_rec_update},
    {"exists",      mod_lua_aerospike_rec_exists},
    {"remove",      mod_lua_aerospike_rec_remove},
    {"log",         mod_lua_aerospike_log},
    {0, 0}
};

static const luaL_reg class_metatable[] = {
    {"__gc",        mod_lua_aerospike_gc},
    {0, 0}
};

/******************************************************************************
 * REGISTER
 *****************************************************************************/

int mod_lua_aerospike_register(lua_State * l) {
    mod_lua_reg_class(l, CLASS_NAME, class_table, class_metatable);
    return 1;
}
