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

#include <aerospike/as_val.h>
#include <aerospike/as_aerospike.h>

#include <aerospike/mod_lua_aerospike.h>
#include <aerospike/mod_lua_record.h>
#include <aerospike/mod_lua_val.h>
#include <aerospike/mod_lua_reg.h>

#include "internal.h"


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
 * aerospike.create_subrec(record) => result<record>
 */
static int mod_lua_aerospike_crec_create(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    as_rec *        r   = mod_lua_torecord(l, 2);
    as_rec *       rc   = as_aerospike_crec_create(a, r);
    if (!rc) return 0;
    mod_lua_pushrecord(l, rc);
    return 1;
}

/**
 * aerospike.update_subrec(record, record) => result<bool>
 */
static int mod_lua_aerospike_crec_update(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    as_rec *        cr  = mod_lua_torecord(l, 2);
    int             rc  = as_aerospike_crec_update(a, cr);
    if (!rc) return 0;
    lua_pushinteger(l, rc);
    return 1;
}

/**
 * aerospike.remove_subrec(record, record) => result<int>
 */
// static int mod_lua_aerospike_crec_remove(lua_State * l) {
//     as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
//     as_rec *        cr  = mod_lua_torecord(l, 2);
//     int             rc  = as_aerospike_crec_remove(a, cr);
//     if (!rc) return 0;
//     lua_pushinteger(l, rc);
//     return 1;
// }


/**
 * aerospike.open_subrec(record, record) => result<bool>
 */
static int mod_lua_aerospike_crec_open(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    as_rec *        r   = mod_lua_torecord(l, 2);
    char *        dig   = (char *)lua_tostring(l, 3);
    as_rec *       rc   = as_aerospike_crec_open(a, r, dig);
    if (!rc) return 0;
    mod_lua_pushrecord(l, rc);
    return 1;
}

/**
 * aerospike.update_subrec(record, record) => result<bool>
 */
static int mod_lua_aerospike_crec_close(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
//    as_rec *        r   = mod_lua_torecord(l, 2);
    as_rec *        cr  = mod_lua_torecord(l, 2);
    // We're no longer using TOP Rec parameter
//    int             rc  = as_aerospike_crec_close(a, r, cr);
    int             rc  = as_aerospike_crec_close(a, cr);
    if (!rc) return 0;
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
} // end mod_lua_aerospike_log()

/**
 * Compute the time and store it in Lua.  We will have to decide what type
 * we want to use for that -- since Lua numbers are only 56 bits precision,
 * and cf_clock is a 64 bit value.
 * It is possible that we'll have to store this as BYTES (similar to how we
 * deal with digests) -- or something.
 */
// static int mod_lua_aerospike_get_current_time(lua_State * l) {
//     as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
//     cf_clock      cur_time  = as_aerospike_get_current_time( a );
//     lua_pushinteger(l, cur_time ); // May have to push some other type @TOBY

//     return 1;
// } // end mod_lua_aerospike_get_current_time()

/******************************************************************************
 * CLASS TABLE
 *****************************************************************************/

static const luaL_reg class_table[] = {
    {"create",           mod_lua_aerospike_rec_create},
    {"update",           mod_lua_aerospike_rec_update},
    {"exists",           mod_lua_aerospike_rec_exists},
    {"remove",           mod_lua_aerospike_rec_remove},
    {"log",              mod_lua_aerospike_log},
    // {"get_current_time", mod_lua_aerospike_get_current_time},
    // {"remove_subrec",    mod_lua_aerospike_crec_remove},
    {"create_subrec",    mod_lua_aerospike_crec_create},
    {"close_subrec",     mod_lua_aerospike_crec_close},
    {"open_subrec",      mod_lua_aerospike_crec_open},
    {"update_subrec",    mod_lua_aerospike_crec_update},
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
