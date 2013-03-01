#pragma once

#include <stdbool.h>

/*****************************************************************************
 * TYPES
 *****************************************************************************/

struct mod_lua_config_s;
typedef struct mod_lua_config_s mod_lua_config;

struct mod_lua_config_s {
    bool    server_mode;
    bool    cache_enabled;
    char    system_path[256];
    char    user_path[256];
};
