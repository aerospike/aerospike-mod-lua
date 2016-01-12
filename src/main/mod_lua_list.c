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

#include <aerospike/as_iterator.h>
#include <aerospike/as_list.h>
#include <aerospike/as_list_iterator.h>
#include <aerospike/as_val.h>

#include <aerospike/mod_lua_val.h>
#include <aerospike/mod_lua_list.h>
#include <aerospike/mod_lua_iterator.h>
#include <aerospike/mod_lua_reg.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_serializer.h>

#include "internal.h"

/*******************************************************************************
 * MACROS
 ******************************************************************************/

#define OBJECT_NAME "list"
#define CLASS_NAME  "List"

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

as_list * mod_lua_tolist(lua_State * l, int index) {
	mod_lua_box * box = mod_lua_tobox(l, index, CLASS_NAME);
	return (as_list *) mod_lua_box_value(box);
}

as_list * mod_lua_pushlist(lua_State * l, as_list * list) {
	mod_lua_box * box = mod_lua_pushbox(l, MOD_LUA_SCOPE_LUA, list, CLASS_NAME);
	return (as_list *) mod_lua_box_value(box);
}

static as_list * mod_lua_checklist(lua_State * l, int index) {
	mod_lua_box * box = mod_lua_checkbox(l, index, CLASS_NAME);
	return (as_list *) mod_lua_box_value(box);
}

static int mod_lua_list_gc(lua_State * l) {
	LOG("mod_lua_list_gc: begin");
	mod_lua_freebox(l, 1, CLASS_NAME);
	LOG("mod_lua_list_gc: end");
	return 0;
}

static int mod_lua_list_insert(lua_State * l) {
	as_list * list = mod_lua_checklist(l, 1);
	if (list) {
		lua_Integer idx = luaL_optinteger(l, 2, 0);
		// Lua index is 1-based.
		if (idx > 0) {
			// increases ref, correct - held by box and this list
			as_val * value = mod_lua_toval(l, 3);
			if (value) {
				as_list_insert(list, (uint32_t)idx - 1, value);
			}
		}
	}
	return 0;
}

static int mod_lua_list_append(lua_State * l) {
	as_list * list = mod_lua_checklist(l, 1);
	if ( list ) {
		// increases ref, correct - held by box and this list
		as_val * value = mod_lua_toval(l, 2);
		if ( value ) {
			as_list_append(list,value);
		}
	}
	return 0;
}

static int mod_lua_list_prepend(lua_State * l) {
	as_list * list = mod_lua_checklist(l, 1);
	if ( list ) {
		as_val * value = mod_lua_toval(l, 2);
		if ( value ) {
			as_list_prepend(list,value);
		}
	}
	return 0;
}

static int mod_lua_list_remove(lua_State * l) {
	as_list * list = mod_lua_checklist(l, 1);
	if (list) {
		lua_Integer idx = luaL_optinteger(l, 2, 0);
		// Lua index is 1-based.
		if (idx > 0) {
			as_list_remove(list, (uint32_t)idx - 1);
		}
	}
	return 0;
}

static int mod_lua_list_concat(lua_State * l) {
	as_list * list = mod_lua_checklist(l, 1);
	if (list) {
		as_list * list2 = mod_lua_checklist(l, 2);
		if (list2) {
			as_list_concat(list, list2);
		}
	}
	return 0;
}

static int mod_lua_list_trim(lua_State * l) {
	as_list * list = mod_lua_checklist(l, 1);
	if (list) {
		lua_Integer idx = luaL_optinteger(l, 2, 0);
		// Lua index is 1-based.
		if (idx > 0) {
			as_list_trim(list, (uint32_t)idx - 1);
		}
	}
	return 0;
}

static int mod_lua_list_drop(lua_State * l) {
	mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
	as_list *       list    = (as_list *) mod_lua_box_value(box);
	as_list *       sub     = NULL;

	if ( list ) {
		lua_Integer n = luaL_optinteger(l, 2, 0);
		if (n > 0) {
			sub = as_list_drop(list, (uint32_t) n);
		}
	}

	if ( sub ) {
		mod_lua_pushlist(l, sub);
	}
	else {
		lua_pushnil(l);
	}

	return 1;
}

static int mod_lua_list_take(lua_State * l) {
	mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
	as_list *       list    = (as_list *) mod_lua_box_value(box);
	as_list *       sub     = NULL;

	if ( list ) {
		lua_Integer n = luaL_optinteger(l, 2, 0);
		if (n > 0) {
			sub = as_list_take(list, (uint32_t) n);
		}
	}

	if ( sub ) {
		mod_lua_pushlist(l, sub);
	}
	else {
		lua_pushnil(l);
	}

	return 1;
}

static int mod_lua_list_size(lua_State * l) {
	as_list * list = mod_lua_checklist(l, 1);
	uint32_t size = 0;

	if ( list ) {
		size = as_list_size(list);
	}

	lua_pushinteger(l, size);
	return 1;
}

static int mod_lua_list_nbytes(lua_State * l) {
	as_list * list = mod_lua_checklist(l, 1);
	uint32_t nbytes = 0;

	if ( list ) {
		as_serializer s;
		as_msgpack_init(&s);
		nbytes = as_serializer_serialize_getsize(&s, (as_val *) list);
		as_serializer_destroy(&s);
	}
	lua_pushinteger(l, nbytes);
	return 1;
}

static int mod_lua_list_new(lua_State * l) {
	int n = lua_gettop(l);
	if (n < 1 || n > 2) {
		return 0;
	}
	lua_Integer capacity = luaL_optinteger(l, 1, -1);
	if (capacity < 0) {
		return 0;
	}
	lua_Integer capacity_step = luaL_optinteger(l, 2, 10);
	if (capacity_step < 0) {
		return 0;
	}
	as_list * ll = (as_list *) as_arraylist_new((uint32_t)capacity, (uint32_t)capacity_step);
	mod_lua_pushlist(l, ll);
	return 1;
}

static int mod_lua_list_cons(lua_State * l) {
	as_list * ll = (as_list *) as_arraylist_new(5,10);
	int n = lua_gettop(l);
	if ( n == 2 && lua_type(l, 2) == LUA_TTABLE) {
		lua_pushnil(l);
		while ( lua_next(l, 2) != 0 ) {
			if ( lua_type(l, -2) == LUA_TNUMBER ) {
				as_list_append(ll, mod_lua_takeval(l, -1));
			}
			lua_pop(l, 1);
		}
	}
	mod_lua_pushlist(l, ll);
	return 1;
}

//static int mod_lua_list_iterator(lua_State * l) {
//	as_list * list  = mod_lua_checklist(l, 1);
//	if ( list ) {
//		as_iterator * itr = mod_lua_pushiterator(l, as_list_iterator_new(list));
//		as_list_iterator_init(itr, l);
//	}
//	else {
//		lua_pushnil(l);
//	}
//	return 1;
//}

static int mod_lua_list_index(lua_State * l) {
	mod_lua_box *	box		= mod_lua_checkbox(l, 1, CLASS_NAME);
	as_list *		list	= (as_list *) mod_lua_box_value(box);
	as_val *		val		= NULL;

	if ( list ) {
		const uint32_t  idx = (uint32_t) luaL_optlong(l, 2, 0);
		if (idx > 0) {
			// Lua is 1 index, C is 0
			val = as_list_get(list, idx-1);
		}
	}

	if ( val ) {
		mod_lua_pushval(l, val);
	}
	else {
		lua_pushnil(l);
	}

	return 1;
}

static int mod_lua_list_newindex(lua_State * l) {
	as_list * list = mod_lua_checklist(l, 1);

	if ( list ) {
		const uint32_t idx = (uint32_t) luaL_optlong(l, 2, 0);
		if (idx > 0) { // Lua is 1 index, C is 0
			as_val * val = mod_lua_takeval(l, 3);
			if ( val ) {
				as_list_set(list, idx - 1, val);
			}
		}
	}
	return 0;
}

static int mod_lua_list_len(lua_State * l) {
	mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
	as_list *       list    = (as_list *) mod_lua_box_value(box);
	if ( list ) {
		lua_pushinteger(l, as_list_size(list));
	}
	else {
		lua_pushinteger(l, 0);
	}
	return 1;
}

static int mod_lua_list_tostring(lua_State * l) {
	mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
	as_val *        val     = mod_lua_box_value(box);
	char *          str     = NULL;

	if ( val ) {
		str = as_val_tostring(val);
	}

	if ( str ) {
		lua_pushstring(l, str);
		cf_free(str);
	}
	else {
		lua_pushstring(l, "List()");
	}

	return 1;
}


/**
 * Generator for list.iterator()
 */
static int mod_lua_list_iterator_next(lua_State * l) {
	as_iterator * iter  = mod_lua_toiterator(l, 1);
	if ( iter && as_iterator_has_next(iter) ) {
		const as_val * val = as_iterator_next(iter);
		if ( val ) {
			mod_lua_pushval(l, val);
			return 1;
		}
	}
	return 0;
}

/**
 * USAGE:
 *      for v in list.iterator(m) do
 *      end
 */
static int mod_lua_list_iterator(lua_State * l) {
	mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
	as_list *       list    = (as_list *) mod_lua_box_value(box);
	if ( list ) {
		lua_pushcfunction(l, mod_lua_list_iterator_next);
		as_list_iterator * itr = (as_list_iterator *) mod_lua_pushiterator(l, sizeof(as_list_iterator));
		as_list_iterator_init(itr, list);
		return 2;
	}

	return 0;
}


/******************************************************************************
 * OBJECT TABLE
 *****************************************************************************/

static const luaL_reg object_table[] = {
	{"new",             mod_lua_list_new},  // Only supported in C.
	{"create",          mod_lua_list_new},  // Supported in all languages.
	{"insert",          mod_lua_list_insert},
	{"append",          mod_lua_list_append},
	{"prepend",         mod_lua_list_prepend},
	{"remove",          mod_lua_list_remove},
	{"concat",          mod_lua_list_concat},
	{"trim",            mod_lua_list_trim},
	{"take",            mod_lua_list_take},
	{"drop",            mod_lua_list_drop},
	{"size",            mod_lua_list_size},
	{"nbytes",          mod_lua_list_nbytes},
	{"iterator",        mod_lua_list_iterator},
	{"tostring",        mod_lua_list_tostring},
	{0, 0}
};

static const luaL_reg object_metatable[] = {
	{"__call",          mod_lua_list_cons},
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
	{"__index",         mod_lua_list_index},
	{"__newindex",      mod_lua_list_newindex},
	{"__len",           mod_lua_list_len},
	{"__tostring",      mod_lua_list_tostring},
	{"__gc",            mod_lua_list_gc},
	{0, 0}
};

/******************************************************************************
 * REGISTER
 *****************************************************************************/

int mod_lua_list_register(lua_State * l) {
	mod_lua_reg_object(l, OBJECT_NAME, object_table, object_metatable);
	mod_lua_reg_class(l, CLASS_NAME, NULL, class_metatable);
	return 1;
}
