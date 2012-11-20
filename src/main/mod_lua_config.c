#include "mod_lua_config.h"
#include <stdlib.h>
#include <string.h>

mod_lua_config * mod_lua_config_create(bool cache_enabled, char * system_path, char * user_path) {
    mod_lua_config * c = (mod_lua_config *) malloc(sizeof(mod_lua_config));
    c->cache_enabled = cache_enabled;
    c->system_path = system_path != NULL ? system_path : strdup("/opt/citrusleaf/sys/udf/lua");
    c->user_path = user_path != NULL ? user_path : strdup("/opt/citrusleaf/usr/udf/lua");
    return c;
}

int mod_lua_config_free(mod_lua_config * c) {
    if ( c->system_path ) free(c->system_path);
    c->system_path = NULL;
    if ( c->user_path ) free(c->user_path);
    c->user_path = NULL;
    free(c);
    return 1;
}
