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

#include <aerospike/as_rec.h>
#include <aerospike/as_val.h>

#include <aerospike/mod_lua_record.h>
#include <aerospike/mod_lua_val.h>
#include <aerospike/mod_lua_bytes.h>
#include <aerospike/mod_lua_reg.h>
#include <aerospike/mod_lua_list.h>

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
    // I am hoping the following is correct use of the free flag
    mod_lua_box * box = mod_lua_pushbox(l, r->_.free ? MOD_LUA_SCOPE_LUA : MOD_LUA_SCOPE_HOST, r, CLASS_NAME);
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
 * Get a record key:
 *      record.key(r)
 */
static int mod_lua_record_key(lua_State * l) {
    as_rec * rec = (as_rec *) mod_lua_checkrecord(l, 1);
    as_val * value  = (as_val *) as_rec_key(rec);
    if ( value != NULL ) {
        mod_lua_pushval(l, value);
        as_val_destroy(value);
        return 1;
    }
    else {
        lua_pushnil(l);
        return 1;
    }
}

/**
 * Get a set name:
 *      record.setname(r)
 */
static int mod_lua_record_setname(lua_State * l) {
    as_rec * rec = (as_rec *) mod_lua_checkrecord(l, 1);
    lua_pushstring(l, as_rec_setname(rec));
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

typedef struct {
	lua_State * state;
	int return_val;
} bin_names_data;

void bin_names_callback(char * bin_names, uint32_t nbins, uint16_t max_name_size, void * udata) {
	bin_names_data * data = (bin_names_data *) udata;
	lua_State * l = data->state;
	lua_createtable(l, nbins, 0);
	if (nbins == 1 && *bin_names == 0) { // single-bin case
		lua_pushnil(l);
		lua_rawseti(l, -2, 1);
	}
	else {
		for (uint16_t i = 0; i < nbins; i++) {
			lua_pushstring(l, &bin_names[i * max_name_size]);
			lua_rawseti(l, -2, i + 1);
		}
	}
}

/**
 * Get a table of a record's bin names:
 *      record.bin_names(r)
 */
static int mod_lua_record_bin_names(lua_State * l) {
    as_rec * rec = (as_rec *) mod_lua_checkrecord(l, 1);
    bin_names_data udata = {.state = l, .return_val = 0};

	as_rec_bin_names(rec, bin_names_callback, (void *) &udata);

    return 1;
}

/**
 * Set a FLAG in the named bin
 */
static int mod_lua_record_set_flags(lua_State * l) {
    as_rec *        rec     = mod_lua_checkrecord(l, 1);
    const char *    name    = luaL_optstring(l, 2, 0);

    // Get the third arg off the stack -- and process as flag (@LDT @TOBY)
    uint8_t   flags    = luaL_optinteger(l, 3, 0);

    // This function just sets up the arguments,
    // The udf record method will do the real work.
    as_rec_set_flags( rec, name, flags );  // DONE !!!

    return 0;
}

/**
 * Set a record TYPE (Reg, LDT, ESR, SubRec
 */
static int mod_lua_record_set_type(lua_State * l) {
    as_rec *        rec     = mod_lua_checkrecord(l, 1);

    // Get the 2nd arg off the stack -- and process as rec Type (@LDT @TOBY)
    int8_t   rec_type    = luaL_optinteger(l, 2, 0);

    // This function just sets up the arguments,
    // The udf record method will do the real work.
    as_rec_set_type( rec, rec_type );  // DONE !!!

    return 0;
}

/**
 * Set a record time to live (ttl)
 */
static int mod_lua_record_set_ttl(lua_State * l) {
    as_rec *        rec     = mod_lua_checkrecord(l, 1);

    // Get the 2nd arg off the stack -- and process as ttl
    uint32_t   ttl    = (uint32_t)luaL_optinteger(l, 2, 0);

    // This function just sets up the arguments,
    // The udf record method will do the real work.
    as_rec_set_ttl( rec, ttl );

    return 0;
}

/**
 * Drop a record's key
 */
static int mod_lua_record_drop_key(lua_State * l) {
    as_rec *        rec     = mod_lua_checkrecord(l, 1);

    // This function just sets up the arguments,
    // The udf record method will do the real work.
    as_rec_drop_key( rec );

    return 0;
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
    {"key",        mod_lua_record_key},
    {"setname",    mod_lua_record_setname},
    {"digest",     mod_lua_record_digest},
    {"numbins",    mod_lua_record_numbins},
    {"set_flags",  mod_lua_record_set_flags},
    {"set_type",   mod_lua_record_set_type},
    {"set_ttl",    mod_lua_record_set_ttl},
    {"drop_key",   mod_lua_record_drop_key},
    {"bin_names",  mod_lua_record_bin_names},
    {0, 0}
};

static const luaL_reg object_metatable[] = {
    // {"__index",         mod_lua_record_index},
    {0, 0}
};

/******************************************************************************
 * CLASS TABLE
 *****************************************************************************/
/*
static const luaL_reg class_table[] = {
    {0, 0}
};
*/

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
