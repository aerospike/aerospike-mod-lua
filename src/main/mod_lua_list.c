#include "mod_lua_val.h"
#include "mod_lua_list.h"
#include "mod_lua_iterator.h"
#include "mod_lua_reg.h"

#include "as_val.h"
#include "internal.h"

#define OBJECT_NAME "list"
#define CLASS_NAME  "List"

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

static int mod_lua_list_append(lua_State * l) {
    as_list * list = mod_lua_checklist(l, 1);
    if ( list ) {
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

static int mod_lua_list_drop(lua_State * l) {
    mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
    as_list *       list    = (as_list *) mod_lua_box_value(box);
    as_list *       sub     = NULL;

    if ( list ) {
        lua_Integer n = luaL_optinteger(l, 2, 0);
        sub = as_list_drop(list, (uint32_t) n);
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
        sub = as_list_take(list, (uint32_t) n);
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

static int mod_lua_list_new(lua_State * l) {
    as_list * ll = as_linkedlist_new(NULL,NULL);
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

static int mod_lua_list_iterator(lua_State * l) {
    as_list * list  = mod_lua_checklist(l, 1);
    mod_lua_pushiterator(l, as_list_iterator_new(list));
    return 1;
}

static int mod_lua_list_index(lua_State * l) {
    mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
    as_list *       list    = (as_list *) mod_lua_box_value(box);
    as_val *        val     = NULL;

    if ( list ) {
        const uint32_t  idx = (uint32_t) luaL_optlong(l, 2, 0);
        val = as_list_get(list, idx-1);
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
    as_list *   list    = mod_lua_checklist(l, 1);

    if ( list ) {
        uint32_t idx = (uint32_t) luaL_optlong(l, 2, 0);
        as_val * val = mod_lua_takeval(l, 3);
        if ( val ) {
            as_list_set(list, idx, val);
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
        free(str);
    }
    else {
        lua_pushstring(l, "Map()");
    }
    
    return 1;
}

/*******************************************************************************
 * ~~~ Object ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 ******************************************************************************/

static const luaL_reg object_table[] = {
    {"append",          mod_lua_list_append},
    {"prepend",         mod_lua_list_prepend},
    {"take",            mod_lua_list_take},
    {"drop",            mod_lua_list_drop},
    {"size",            mod_lua_list_size},
    {"iterator",        mod_lua_list_iterator},
    {"tostring",        mod_lua_list_tostring},
    {0, 0}
};

static const luaL_reg object_metatable[] = {
    {"__call",          mod_lua_list_new},
    {0, 0}
};

/*******************************************************************************
 * ~~~ Class ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 ******************************************************************************/

static const luaL_reg class_table[] = {
    {0, 0}
};

static const luaL_reg class_metatable[] = {
    {"__index",         mod_lua_list_index},
    {"__newindex",      mod_lua_list_newindex},
    {"__len",           mod_lua_list_len},
    {"__tostring",      mod_lua_list_tostring},
    {"__gc",            mod_lua_list_gc},
    {0, 0}
};

/*******************************************************************************
 * ~~~ Register ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 ******************************************************************************/

int mod_lua_list_register(lua_State * l) {
    mod_lua_reg_object(l, OBJECT_NAME, object_table, object_metatable);
    mod_lua_reg_class(l, CLASS_NAME, NULL, class_metatable);
    return 1;
}
