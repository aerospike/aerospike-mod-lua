#pragma once

#include <stdbool.h>
#include <pthread.h>

typedef struct mod_lua_config_s mod_lua_config;
typedef struct mod_lua_config_op_s mod_lua_config_op;

#define  MOD_LUA_CONFIG_OP_ADD_FILE  1 
#define  MOD_LUA_CONFIG_OP_REM_FILE  2
#define  MOD_LUA_CONFIG_OP_INIT      3

struct mod_lua_config_s {
    bool                server_mode;
    bool                cache_enabled;
    char *              system_path;
    char *              user_path;
    pthread_rwlock_t    lock;
};

struct mod_lua_config_op_s {
    char               optype;
    void           *   arg;
    mod_lua_config *   config;
};

mod_lua_config * mod_lua_config_new(bool, char *, char *);

mod_lua_config * mod_lua_config_client(bool, char *, char *);

int mod_lua_config_free(mod_lua_config *);
int mod_lua_config_rdlock(mod_lua_config *);
int mod_lua_config_wrlock(mod_lua_config *);
int mod_lua_config_unlock(mod_lua_config *);
