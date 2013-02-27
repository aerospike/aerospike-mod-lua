#include "mod_lua_val.h"
#include "mod_lua_bytes.h"
#include "mod_lua_iterator.h"
#include "mod_lua_reg.h"

#include "as_val.h"
#include "internal.h"

/*******************************************************************************
 * MACROS
 ******************************************************************************/

#define OBJECT_NAME "bytes"
#define CLASS_NAME  "Bytes"

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

as_bytes * mod_lua_tobytes(lua_State * l, int index) {
    mod_lua_box * box = mod_lua_tobox(l, index, CLASS_NAME);
    return (as_bytes *) mod_lua_box_value(box);
}

as_bytes * mod_lua_pushbytes(lua_State * l, as_bytes * b) {
    mod_lua_box * box = mod_lua_pushbox(l, MOD_LUA_SCOPE_LUA, b, CLASS_NAME);
    return (as_bytes *) mod_lua_box_value(box);
}

static as_bytes * mod_lua_checkbytes(lua_State * l, int index) {
    mod_lua_box * box = mod_lua_checkbox(l, index, CLASS_NAME);
    return (as_bytes *) mod_lua_box_value(box);
}

static int mod_lua_bytes_gc(lua_State * l) {
    mod_lua_freebox(l, 1, CLASS_NAME);
    return 0;
}

#if 0
static int mod_lua_bytes_len(lua_State * l) {
    as_bytes *  b     = mod_lua_checkbytes(l, 1);
    uint32_t    size    = as_bytes_len(b);
    lua_pushinteger(l, size);
    return 1;
}
#endif

static int mod_lua_bytes_len(lua_State * l) {
    mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
    as_bytes *        b     = (as_bytes *) mod_lua_box_value(box);
    if ( b ) {
        lua_pushinteger(l, as_bytes_len(b));
    }
    else {
        lua_pushinteger(l, 0);
    }
    return 1;
}

static int mod_lua_bytes_new(lua_State * l) {
    as_bytes * b = 0;

    int n_args = lua_gettop(l); // number of elements passed
    if ( n_args == 2 && lua_type(l, 2) == LUA_TTABLE) {

        lua_Integer n = luaL_optinteger(l, 2, 0);

        b = as_bytes_empty_new(n /*len*/);

#if 0
        lua_pushnil(l);
        while ( lua_next(l, 2) != 0 ) {
            as_val * k = mod_lua_takeval(l, -2);
            as_val * v = mod_lua_takeval(l, -1);
            if ( !k || !v ) {
                as_val_destroy(k);
                as_val_destroy(v);
                continue;
            }
            as_bytes_set(b, k, v);
            lua_pop(l, 1);
        }
#endif
    }
    mod_lua_pushbytes(l, b);
    return 1;
}

static int mod_lua_bytes_index(lua_State * l) {
    mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
    as_bytes *        b     = (as_bytes *) mod_lua_box_value(box);
    as_val *        val     = NULL;

#if 0
    if ( b ) {
        as_val * key = mod_lua_takeval(l, 2);
        if ( key ) {
            val = as_bytes_get(b, key);
        }
    }
#endif

    if ( val ) {
        mod_lua_pushval(l, val);
    }
    else {
        lua_pushnil(l);
    }

    return 1;
}

static int mod_lua_bytes_newindex(lua_State * l) {
    as_bytes * b = mod_lua_checkbytes(l, 1);
    if ( b ) {
        as_val * key = mod_lua_takeval(l, 2);
        if ( key ) {
            as_val * val = mod_lua_takeval(l, 3);
            if ( val ) {
//                as_bytes_set(b, key, val);
            }
        }
    }
    return 0;
}



static int mod_lua_bytes_tostring(lua_State * l) {
    mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
    as_val *        val     = mod_lua_box_value(box);
    char *          str     = NULL;

    if ( val ) {
        str = as_val_tostring(val);
    }

    if ( str ) {
        lua_pushstring(l, str);
        free(str);
    }
    else {
        lua_pushstring(l, "Bytes()");
    }

    return 1;
}

#if 0
/**
 * Generator for map.pairs()
 */
static int mod_lua_bytes_pairs_next(lua_State * l) {
    as_iterator * iter  = mod_lua_toiterator(l, 1);
    if ( iter && as_iterator_has_next(iter) ) {
        as_pair * pair = (as_pair *) as_iterator_next(iter);
        if ( pair ) {
            mod_lua_pushval(l, pair->_1);
            mod_lua_pushval(l, pair->_2);
            return 2;
        }
    }
    return 0;
}

/**
 * USAGE:
 *      for k,v in map.pairs(m) do
 *      end
 * USAGE:
 *      for k,v in map.iterator(m) do
 *      end
 */
static int mod_lua_map_pairs(lua_State * l) {
    mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
    as_map *        map     = (as_map *) mod_lua_box_value(box);
    if ( map ) {
        as_iterator * iter = as_map_iterator_new(map);
        if ( iter ) {
            lua_pushcfunction(l, mod_lua_map_pairs_next);
            mod_lua_pushiterator(l, iter);
            return 2;
        }
    }

    return 0;
}

/**
 * Generator for map.keys()
 */
static int mod_lua_map_keys_next(lua_State * l) {
    as_iterator * iter  = mod_lua_toiterator(l, 1);
    if ( iter && as_iterator_has_next(iter) ) {
        as_pair * pair = (as_pair *) as_iterator_next(iter);
        if ( pair ) {
            mod_lua_pushval(l, pair->_1);
            return 1;
        }
    }
    return 0;
}

/**
 * USAGE:
 *      for k in map.keys(m) do
 *      end
 */
static int mod_lua_map_keys(lua_State * l) {
    mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
    as_map *        map     = (as_map *) mod_lua_box_value(box);
    if ( map ) {
        as_iterator * iter = as_map_iterator_new(map);
        if ( iter ) {
            lua_pushcfunction(l, mod_lua_map_keys_next);
            mod_lua_pushiterator(l, iter);
            return 2;
        }
    }

    return 0;
}

/**
 * Generator for map.values()
 */
static int mod_lua_map_values_next(lua_State * l) {
    as_iterator * iter  = mod_lua_toiterator(l, 1);
    if ( iter && as_iterator_has_next(iter) ) {
        as_pair * pair = (as_pair *) as_iterator_next(iter);
        if ( pair ) {
            mod_lua_pushval(l, pair->_2);
            return 1;
        }
    }
    return 0;
}

/**
 * USAGE:
 *      for v in map.values(m) do
 *      end
 */
static int mod_lua_map_values(lua_State * l) {
    mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
    as_map *        map     = (as_map *) mod_lua_box_value(box);
    if ( map ) {
        as_iterator * iter = as_map_iterator_new(map);
        if ( iter ) {
            lua_pushcfunction(l, mod_lua_map_values_next);
            mod_lua_pushiterator(l, iter);
            return 2;
        }
    }

    return 0;
}
#endif
/******************************************************************************
 * OBJECT TABLE
 *****************************************************************************/

static const luaL_reg object_table[] = {
    {"size",            mod_lua_bytes_len},
    {"tostring",        mod_lua_bytes_tostring},
    {0, 0}
};

static const luaL_reg object_metatable[] = {
    {"__call",          mod_lua_bytes_new},
    {0, 0}
};

/******************************************************************************
 * CLASS TABLE
 *****************************************************************************/

static const luaL_reg class_table[] = {
    {0, 0}
};

static const luaL_reg class_metatable[] = {
//    {"__index",         mod_lua_map_index},
//    {"__newindex",      mod_lua_map_newindex},
    {"__len",           mod_lua_bytes_len},
    {"__tostring",      mod_lua_bytes_tostring},
    {"__gc",            mod_lua_bytes_gc},
    {0, 0}
};

/******************************************************************************
 * REGISTER
 *****************************************************************************/

int mod_lua_bytes_register(lua_State * l) {
    mod_lua_reg_object(l, OBJECT_NAME, object_table, object_metatable);
    mod_lua_reg_class(l, CLASS_NAME, NULL, class_metatable);
    return 1;
}
