#include "mod_lua_config.h"
#include <stdlib.h>
#include <string.h>

mod_lua_config * mod_lua_config_new(bool cache_enabled, char * system_path, char * user_path) {
    mod_lua_config * c = (mod_lua_config *) malloc(sizeof(mod_lua_config));
    if (!c) return NULL;
    c->cache_enabled = cache_enabled;
    c->system_path = system_path != NULL ? system_path : strdup("/opt/citrusleaf/sys/udf/lua");
    c->user_path = user_path != NULL ? user_path : strdup("/opt/citrusleaf/usr/udf/lua");
    pthread_rwlockattr_t rwattr;
    if (0 != pthread_rwlockattr_init(&rwattr)) {
        free(c); 
        return NULL;
    }
    if (0 != pthread_rwlockattr_setkind_np(&rwattr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP)) {
        free(c); 
        return NULL;
    }

    if (pthread_rwlock_init(&c->lock, &rwattr)) {
        free(c); 
        return NULL;
    }
    return c;
}

mod_lua_config * mod_lua_config_client(bool cache_enabled, char * system_path, char * user_path) {
    mod_lua_config * c = mod_lua_config_new(cache_enabled, system_path, user_path);
    c->server_mode = false;
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

int mod_lua_config_rdlock(mod_lua_config * c) {
    return pthread_rwlock_rdlock(&c->lock);
}

int mod_lua_config_wrlock(mod_lua_config * c) {
    return pthread_rwlock_wrlock(&c->lock);
}

int mod_lua_config_unlock(mod_lua_config * c) {
    return pthread_rwlock_unlock(&c->lock);
}
