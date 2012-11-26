#pragma once

#include <stdbool.h>

typedef struct mod_lua_config_s mod_lua_config;

struct mod_lua_config_s {
    bool    cache_enabled;
    char *  system_path;
    char *  user_path;
};

mod_lua_config * mod_lua_config_new(bool, char *, char *);

int mod_lua_config_free(mod_lua_config *);
