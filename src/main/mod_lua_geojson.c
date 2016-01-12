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

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#include <citrusleaf/cf_byte_order.h>
#include <aerospike/as_val.h>
#include <aerospike/mod_lua_val.h>
#include <aerospike/mod_lua_geojson.h>
#include <aerospike/mod_lua_reg.h>

#include "internal.h"



/*******************************************************************************
 * MACROS
 ******************************************************************************/

#define OBJECT_NAME "geojson"
#define CLASS_NAME  "GeoJSON"

/*******************************************************************************
 * BOX FUNCTIONS
 ******************************************************************************/

as_geojson * mod_lua_togeojson(lua_State * l, int index) {
	mod_lua_box * box = mod_lua_tobox(l, index, CLASS_NAME);
	return (as_geojson *) mod_lua_box_value(box);
}

as_geojson * mod_lua_pushgeojson(lua_State * l, as_geojson * b) {
	mod_lua_box * box = mod_lua_pushbox(l, MOD_LUA_SCOPE_LUA, b, CLASS_NAME);
	return (as_geojson *) mod_lua_box_value(box);
}

#if 0
static as_geojson * mod_lua_checkgeojson(lua_State * l, int index) {
	mod_lua_box * box = mod_lua_checkbox(l, index, CLASS_NAME);
	return (as_geojson *) mod_lua_box_value(box);
}
#endif

static int mod_lua_geojson_gc(lua_State * l) {
	mod_lua_freebox(l, 1, CLASS_NAME);
	return 0;
}

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

static int mod_lua_geojson_new(lua_State * l)
{
	int argc = lua_gettop(l);
	if (argc != 2) {
		return 0;
	}

	const char * geostr = luaL_optstring(l, 2, NULL);
	if (geostr == NULL) {
		return 0;
	}

	as_geojson * geo = as_geojson_new(cf_strdup(geostr), true);
	if (geo == NULL) {
		return 0;
	}

	mod_lua_pushgeojson(l, geo);
	return 1;
}

static int mod_lua_geojson_tostring(lua_State * l)
{
	// we expect 1 arg
	if ( lua_gettop(l) != 1 ) {
		lua_pushinteger(l, 0);
		return 1;
	}

	mod_lua_box *   box = mod_lua_checkbox(l, 1, CLASS_NAME);
	as_val *        val = mod_lua_box_value(box);
	char *          str = NULL;

	if ( val ) {
		str = as_val_tostring(val);
	}

	if ( str ) {
		lua_pushstring(l, str);
		cf_free(str);
	}
	else {
		lua_pushstring(l, "GeoJSON()");
	}

	return 1;
}

/******************************************************************************
 * OBJECT TABLE
 *****************************************************************************/

static const luaL_reg geojson_object_table[] = {
	{0, 0}
};

static const luaL_reg geojson_object_metatable[] = {
	{"__call",          mod_lua_geojson_new},
	{0, 0}
};

/******************************************************************************
 * CLASS TABLE
 *****************************************************************************/

static const luaL_reg geojson_class_metatable[] = {
	{"__tostring",      mod_lua_geojson_tostring},
	{"__gc",            mod_lua_geojson_gc},
	{0, 0}
};

/******************************************************************************
 * REGISTER
 *****************************************************************************/

int mod_lua_geojson_register(lua_State * l) {
	mod_lua_reg_object(l, OBJECT_NAME,
					   geojson_object_table, geojson_object_metatable);
	mod_lua_reg_class(l, CLASS_NAME, NULL, geojson_class_metatable);
	return 1;
}
