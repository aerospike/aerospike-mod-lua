#pragma once

#include <lua.h>

// typedef struct mod_lua_aerospike_s mod_lua_aerospike;
// typedef struct mod_lua_aerospike_hooks_s mod_lua_aerospike_hooks;



// struct mod_lua_aerospike_hooks_s {
//     int (*get)
// };


int mod_lua_aerospike_register(lua_State *);
