/**
 * Provides a lua interface to the aerospike struct and functions
 *
 *
 *      aerospike.get(namespace, set, key): result<record>
 *      aerospike.put(namespace, set, key, table)
 *      aerospike.remove(namespace, set, key): result<bool>
 *
 *      aerospike.update(record): result<record>
 *
 *
 */

#include "mod_lua_aerospike.h"
#include "as_rec.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

/**
 * aerospike.get(namespace, set, key) => result<record>
 */
static int mod_lua_aerospike_get(lua_State * l) {
    printf("mod_lua_aerospike_get()\n");
    return 0;
}

/**
 * aerospike.put(namespace, set, key, lua_Table) => result<bool>
 */
static int mod_lua_aerospike_put(lua_State * l) {
    printf("mod_lua_aerospike_put()\n");
    return 0;
}

/**
 * aerospike.remove(namespace, set, key) => result<bool>
 */
static int mod_lua_aerospike_remove(lua_State * l) {
    return 0;
}

/**
 * aerospike.update(record) => result<bool>
 */
static int mod_lua_aerospike_update(lua_State * l) {
    return 0;
}

/**
 * aerospike functions
 */
static const luaL_reg mod_lua_aerospike_functions[] = {
    {"get",         mod_lua_aerospike_get},
    {"put",         mod_lua_aerospike_put},
    {"remove",      mod_lua_aerospike_remove},
    {"update",      mod_lua_aerospike_update},
    {0, 0}
};

/**
 * Registers the aerospike library
 */
int mod_lua_aerospike_register(lua_State * l) {
    luaL_register(l, "aerospike", mod_lua_aerospike_functions);
    return 1;
}
