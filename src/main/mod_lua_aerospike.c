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
 * aerospike.create_subrec(record) => result<record>
 */
static int mod_lua_aerospike_subrec_create(lua_State * l) {
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
static int mod_lua_aerospike_subrec_update(lua_State * l) {
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
static int mod_lua_aerospike_subrec_remove(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    as_rec *        cr  = mod_lua_torecord(l, 2);
    int             rc  = as_aerospike_crec_remove(a, cr);
    if (!rc) return 0;
    lua_pushinteger(l, rc);
    return 1;
}


/**
 * aerospike.open_subrec(record, record) => result<bool>
 */
static int mod_lua_aerospike_subrec_open(lua_State * l) {
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
static int mod_lua_aerospike_subrec_close(lua_State * l) {
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
static int mod_lua_aerospike_get_current_time(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    cf_clock      cur_time  = as_aerospike_get_current_time( a );
    lua_pushinteger(l, cur_time ); // May have to push some other type @TOBY
    return 1;
}

/**
 * Hook to set execution context information 
 */
static int mod_lua_aerospike_set_context(lua_State * l) {
    as_aerospike *  a   = mod_lua_checkaerospike(l, 1);
    
    as_rec *        r   = mod_lua_torecord(l, 2);

	// Get the 2nd arg off the stack -- and process as context
    uint32_t  context   = (uint32_t)luaL_optinteger(l, 3, 0);

	int ret = as_aerospike_set_context(a, r, context);
	
	lua_pushinteger(l, ret);
    return 1;
}

/**
 * hook to fetch config information from server. 
 */
static int mod_lua_aerospike_get_config(lua_State * l) {
    as_aerospike * a    = mod_lua_checkaerospike(l, 1);
    as_rec *       r    = mod_lua_torecord(l, 2);
    const char *   name = luaL_optstring(l, 3, NULL);

    int ret = as_aerospike_get_config(a, r, name); 
    lua_pushinteger(l, ret);
    return 1;
} // end mod_lua_aerospike_get_config()

/******************************************************************************
 * CLASS TABLE
 *****************************************************************************/

static const luaL_reg class_table[] = {
    {"create",           mod_lua_aerospike_rec_create},
    {"update",           mod_lua_aerospike_rec_update},
    {"exists",           mod_lua_aerospike_rec_exists},
    {"remove",           mod_lua_aerospike_rec_remove},
    {"create_subrec",    mod_lua_aerospike_subrec_create},
    {"update_subrec",    mod_lua_aerospike_subrec_update},
    {"remove_subrec",    mod_lua_aerospike_subrec_remove},
    {"close_subrec",     mod_lua_aerospike_subrec_close},
    {"open_subrec",      mod_lua_aerospike_subrec_open},
    {"log",              mod_lua_aerospike_log},
    {"get_current_time", mod_lua_aerospike_get_current_time},
    {"set_context",      mod_lua_aerospike_set_context},
    {"get_config",       mod_lua_aerospike_get_config},
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
