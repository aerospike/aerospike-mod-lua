#include "mod_lua_val.h"
#include "mod_lua_list.h"
#include "mod_lua_iterator.h"
#include "mod_lua_reg.h"

#define OBJECT_NAME "list"
#define CLASS_NAME  "List"

as_list * mod_lua_tolist(lua_State * l, int index) {
    as_list * list = (as_list *) lua_touserdata(l, index);
    if (list == NULL) luaL_typerror(l, index, CLASS_NAME);
    return list;
}

as_list * mod_lua_pushlist(lua_State * l, as_list * i) {
    as_list * list = (as_list *) lua_newuserdata(l, sizeof(as_list));
    *list = *i;
    luaL_getmetatable(l, CLASS_NAME);
    lua_setmetatable(l, -2);
    return list;
}

static as_list * mod_lua_checklist(lua_State * l, int index) {
    as_list * list = NULL;
    luaL_checktype(l, index, LUA_TUSERDATA);
    list = (as_list *) luaL_checkudata(l, index, CLASS_NAME);
    if (list == NULL) luaL_typerror(l, index, CLASS_NAME);
    return list;
}

static int mod_lua_list_append(lua_State * l) {
    as_list *   list    = mod_lua_checklist(l, 1);
    as_val *    value   = mod_lua_toval(l, 2);
    as_list_append(list,value);
    return 0;
}

static int mod_lua_list_prepend(lua_State * l) {
    as_list *   list    = mod_lua_checklist(l, 1);
    as_val *    value   = mod_lua_toval(l, 2);

    as_list_prepend(list,value);

    return 0;
}

static int mod_lua_list_drop(lua_State * l) {
    as_list *   list    = mod_lua_checklist(l, 1);
    lua_Integer n       = luaL_optinteger(l, 2, 0);
    as_list *   sub     = as_list_drop(list, (uint32_t) n);
    mod_lua_pushlist(l,sub);
    return 1;
}

static int mod_lua_list_take(lua_State * l) {
    as_list *   list    = mod_lua_checklist(l, 1);
    lua_Integer n       = luaL_optinteger(l, 2, 0);
    as_list *   sub     = as_list_take(list, (uint32_t) n);
    mod_lua_pushlist(l,sub);
    return 1;
}

static int mod_lua_list_size(lua_State * l) {
    as_list *   list    = mod_lua_checklist(l, 1);
    uint32_t    size    = as_list_size(list);
    lua_pushinteger(l, size);
    return 1;
}

static int mod_lua_list_new(lua_State * l) {
    as_list * list = as_linkedlist_new(NULL,NULL);
    int n = lua_gettop(l);
    if ( n == 2 && lua_type(l, 2) == LUA_TTABLE) {
        lua_pushnil(l);
        while ( lua_next(l, 2) != 0 ) {
            if ( lua_type(l, -2) == LUA_TNUMBER ) {
                as_list_append(list, mod_lua_toval(l, -1));
            }
            lua_pop(l, 1);
        }
    }
    mod_lua_pushlist(l, list);
    return 1;
}

static int mod_lua_list_iterator(lua_State * l) {
    as_list * list  = mod_lua_checklist(l, 1);
    mod_lua_pushiterator(l, as_list_iterator(list));
    return 1;
}

static int mod_lua_list_gc(lua_State * l) {
    as_list * list = mod_lua_checklist(l, 1);
    // as_list_free(list);
    return 0;
}

static int mod_lua_list_index(lua_State * l) {
    as_list *       list    = mod_lua_checklist(l, 1);
    const uint32_t  idx     = (uint32_t) luaL_optlong(l, 2, 0);
    const as_val *  val     = as_list_get(list, idx-1);
    mod_lua_pushval(l, val);
    return 1;
}

static int mod_lua_list_newindex(lua_State * l) {
    as_list *   list    = mod_lua_checklist(l, 1);
    uint32_t    idx     = (uint32_t) luaL_optlong(l, 2, 0);
    as_val *    val     = mod_lua_toval(l, 3);
    
    if ( val == NULL ) {
        // one day, we will remove values
    }
    else {
        as_list_set(list, idx, val);
    }
    return 0;
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
