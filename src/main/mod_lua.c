#include "cf_queue.h"
#include "cf_rchash.h"
#include "cf_alloc.h"

#include "mod_lua.h"
#include "mod_lua_config.h"
#include "mod_lua_aerospike.h"
#include "mod_lua_record.h"
#include "mod_lua_iterator.h"
#include "mod_lua_stream.h"
#include "mod_lua_list.h"
#include "mod_lua_map.h"
#include "mod_lua_val.h"

#include "as_aerospike.h"
#include "as_types.h"

#include "internal.h"

#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <setjmp.h>         // needed for gracefully handling lua panics

// #include <fault.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <pthread.h>

/******************************************************************************
 * MACROS
 ******************************************************************************/

#define CACHE_TABLE_ENTRY_MAX 128
#define CACHE_ENTRY_KEY_MAX 128
#define CACHE_ENTRY_GEN_MAX 128
#define CACHE_ENTRY_STATE_MAX 128
#define CACHE_ENTRY_STATE_MIN 10

/******************************************************************************
 * TYPES
 ******************************************************************************/

typedef struct context_s context;

typedef struct cache_entry_s cache_entry;
cf_rchash *centry_hash;
typedef struct cache_item_s cache_item;

struct cache_entry_s {
    char            key[CACHE_ENTRY_KEY_MAX];
    char            gen[CACHE_ENTRY_GEN_MAX];
    uint32_t        max_cache_size;
    cf_queue      * lua_state_q;
    cf_atomic32     cache_miss;
    cf_atomic32     total;
};


struct cache_item_s {
    char            key[CACHE_ENTRY_KEY_MAX];
    char            gen[CACHE_ENTRY_GEN_MAX];
    lua_State *     state;
};

/******************************************************************************
 * VARIABLES
 ******************************************************************************/

// static uint32_t cache_size = 0;

static const as_module_hooks hooks;

static jmp_buf panic_jmp;

/**
 * Lua Module Specific Data
 * This will populate the module.source field
 */
static struct context_s {
    lua_State * lua_cache;
    bool        cache_enabled;
    char        system_path[256];
    char        user_path[256];
    char        filename[256];
    pthread_rwlock_t *lock;
} lua = {
    .cache_enabled  = true,
    .system_path    = "",
    .user_path      = "",
};


/******************************************************************************
 * FUNCTION DECLS
 ******************************************************************************/

static int init(as_module *);
static int configure(as_module *, void *);
static int apply_record(as_module *, as_aerospike *, const char *, const char *, as_rec *, as_list *, as_result *);
static int apply_stream(as_module *, as_aerospike *, const char *, const char *, as_stream *, as_list *, as_stream *);

static lua_State * create_state(context *, const char *filename);
static int poll_state(context *, cache_item *);
static int offer_state(context *, cache_item *);

static void panic_setjmp(void);
// static int handle_error(lua_State *);
static int handle_panic(lua_State *);


/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

// Raj (todo) fix stupid hash function
uint32_t        
filename_hash_fn(void *filename, uint32_t len) {   
    char *b = filename;
    uint32_t acc = 0;
    for (int i=0;i<len;i++) {
        acc += *(b+i);
    }
    return(acc);
}

/**
 * Module Initializer.
 * This sets up the module before use. This is called only once on startup.
 *
 * @param m the module being initialized.
 * @return 0 on success, otherwhise 1
 */
static int init(as_module * m) {
    if (CF_RCHASH_OK != cf_rchash_create(&centry_hash, filename_hash_fn, NULL, 
                            0, 64, CF_RCHASH_CR_MT_LOCKPOOL)) {
        return 1;
    }
    return 0;
}


static inline int cache_entry_cleanup(cache_entry * centry) {
    lua_State *l = NULL;
    while(cf_queue_pop(centry->lua_state_q, &l, CF_QUEUE_NOWAIT) == CF_QUEUE_OK) {
        lua_close(l);
    }
    return 0;
}

static inline int cache_entry_populate(context *ctx, cache_entry *centry, const char *key) {
    lua_State *l = NULL;
    for ( int i = 0; i < CACHE_ENTRY_STATE_MIN; i++ ) {
        l = create_state(ctx, key);
        cf_queue_push(centry->lua_state_q, &l);
    }
    return 0;
}

/**
 * Clear the entry:
 *  - truncate the key
 *  - truncate the gen
 *  - release all lua_States
 *  - set size to 0
 */
static inline int cache_entry_init(context * ctx, cache_entry * centry, const char *key, const char *gen) {
    cache_entry_cleanup(centry);
    cache_entry_populate(ctx, centry, key);
    strncpy(centry->key, key, CACHE_ENTRY_KEY_MAX);
    strncpy(centry->gen, gen, CACHE_ENTRY_GEN_MAX);
    return 0;
}

int cache_rm(context * ctx, const char *key) {
    if (strlen(key) == 0) return 0;
    cache_entry     * centry = NULL;
    if (CF_RCHASH_OK != cf_rchash_get(centry_hash, (void *)key, strlen(key), (void *)&centry)) {
        return 0;
    }
    cache_entry_cleanup(centry);
    cf_queue_destroy(centry->lua_state_q);
    cf_rc_releaseandfree(centry);
    centry = 0;
    cf_rchash_delete(centry_hash, (void *)key, strlen(key));

	return 0;
}

int cache_init(context * ctx, const char *key, const char * gen) {
    if (strlen(key) == 0) return 0;
    cache_entry     * centry = NULL;
    if (CF_RCHASH_OK != cf_rchash_get(centry_hash, (void *)key, strlen(key), (void *)&centry)) {
        centry = cf_rc_alloc(sizeof(cache_entry)); 
        cf_atomic32_set(&centry->total, 0);
        cf_atomic32_set(&centry->cache_miss, 0);
        centry->max_cache_size = CACHE_ENTRY_STATE_MAX;
        centry->lua_state_q = cf_queue_create(sizeof(lua_State *), true);
        cache_entry_init(ctx, centry, key, gen);
        int retval = cf_rchash_put(centry_hash, (void *)key, strlen(key), (void *)centry);
        if (retval != CF_RCHASH_OK) {
            // weird should not happen
            cf_queue_destroy(centry->lua_state_q);
            cf_rc_releaseandfree(centry);
            return 1;
        } else {
            LOG( "Added [%s:%p] \n", key, centry);
        }
    } else { 
        cache_entry_init(ctx, centry, key, gen);
        cf_rc_releaseandfree(centry);
        centry = 0;
    }
	return 0;
}

static inline int cache_rmfile(context * ctx, const char * filename) {
    char    key[CACHE_ENTRY_KEY_MAX]    = "";
    memcpy(key, filename, CACHE_ENTRY_KEY_MAX);
    *(rindex(key, '.')) = '\0';
    cache_rm(ctx, key);
    return 0;
}

static inline int cache_initfile(context * ctx, const char * filename) {
    char    key[CACHE_ENTRY_KEY_MAX]    = "";
    char    gen[CACHE_ENTRY_GEN_MAX]    = "";
    memcpy(key, filename, CACHE_ENTRY_KEY_MAX);
    *(rindex(key, '.')) = '\0';
    cache_init(ctx, key, gen);
    return 0;
}

static inline int cache_initdir(context * ctx, const char * directory) {

    DIR *           dir     = NULL;
    struct dirent * dentry  = NULL;

    dir = opendir(directory);
    
    if ( dir == 0 ) return -1;

    while ( (dentry = readdir(dir)) && dentry->d_name ) {

        char *  filename                    = dentry->d_name;
        char    key[CACHE_ENTRY_KEY_MAX]    = "";
        char    gen[CACHE_ENTRY_GEN_MAX]    = "";

        memcpy(key, filename, CACHE_ENTRY_KEY_MAX);
        *(rindex(key, '.')) = '\0';
        cache_init(ctx, key, gen);
    }

    closedir(dir);

    return 0;
}

/**
 * Module Configurator. 
 * This configures and reconfigures the module. This can be called an
 * arbitrary number of times during the lifetime of the server.
 *
 * @param m the module being configured.
 * @return 0 on success, otherwhise 1
 * @sychronization: Caller should have a write lock
 */
static int configure(as_module * m, void * config_op) {

    context *           ctx = (context *) m->source;
    mod_lua_config_op * op  = (mod_lua_config_op *) config_op;
    mod_lua_config *    cfg = op->config;
    DIR *               dir = NULL;

    ctx->cache_enabled  = cfg->cache_enabled;
    ctx->lock           = &cfg->lock;

    // Attempt to open the directory.
    // If it opens, then set the ctx value.
    // Otherwise, we alert the user of the error when a UDF is called. (for now)
    dir = opendir(cfg->system_path);
    if ( dir == 0 ) {
        ctx->system_path[0] = '\0';
        strncpy(ctx->system_path+1, cfg->system_path, 255);
    }
    else {
        strncpy(ctx->system_path, cfg->system_path, 256);
        closedir(dir);
    }
    dir = NULL;

    // Attempt to open the directory.
    // If it opens, then set the ctx value.
    // Otherwise, we alert the user of the error when a UDF is called. (for now)
    dir = opendir(cfg->user_path);
    if ( dir == 0 ) {
        ctx->user_path[0] = '\0';
        strncpy(ctx->user_path+1, cfg->user_path, 255);
    }
    else {
        strncpy(ctx->user_path, cfg->user_path, 256);
        closedir(dir);
    }
    dir = NULL;

    if ( ctx->cache_enabled ) {
        if (op->optype == MOD_LUA_CONFIG_OP_INIT) {
            if ( ctx->system_path[0] != '\0' && ctx->user_path[0] != '\0' ) {
                cache_initdir(ctx, ctx->user_path);
            }
        } else if (op->optype == MOD_LUA_CONFIG_OP_ADD_FILE) {
            if (op->arg) {
                cache_initfile(ctx, (const char *) op->arg);
            } else {
                return 1;
            }
        } else if (op->optype == MOD_LUA_CONFIG_OP_REM_FILE) {
            if (op->arg) {
                cache_rmfile(ctx, (const char *)op->arg);
            } else {
                return 1;
            }
        } else {
            return 1;
        }
    }

    return 0;
}

static void package_path_set(lua_State * l, char * system_path, char * user_path) {
    int stack = 0;

    lua_getglobal(l, "package");
    lua_getfield(l, -1, "path");
    stack += 1;

    lua_pushstring(l, ";");
    lua_pushstring(l, system_path);
    lua_pushstring(l, "/?.lua");
    stack += 3;
    
    lua_pushstring(l, ";");
    lua_pushstring(l, user_path);
    lua_pushstring(l, "/?.lua");
    stack += 3;
    
    lua_concat(l, stack);

    lua_setfield(l, -2, "path");
    lua_pop(l, 1);
}

static void package_cpath_set(lua_State * l, char * system_path, char * user_path) {
    int stack = 0;

    lua_getglobal(l, "package");
    lua_getfield(l, -1, "cpath");
    stack += 1;

    lua_pushstring(l, ";");
    lua_pushstring(l, system_path);
    lua_pushstring(l, "/?.so");
    stack += 3;
    
    lua_pushstring(l, ";");
    lua_pushstring(l, user_path);
    lua_pushstring(l, "/?.so");
    stack += 3;
    
    lua_concat(l, stack);

    lua_setfield(l, -2, "cpath");
    lua_pop(l, 1);
}

/**
 * Creates a new context (lua_State) populating it with default values.
 *
 * @return a new lua_State
 */
static lua_State * create_state(context * ctx, const char * filename) {
    lua_State * l   = NULL;

    l = lua_open();

    luaL_openlibs(l);

    panic_setjmp();
    lua_atpanic(l, handle_panic);

    package_path_set(l, ctx->system_path, ctx->user_path);
    package_cpath_set(l, ctx->system_path, ctx->user_path);

    mod_lua_aerospike_register(l);
    mod_lua_record_register(l);
    mod_lua_iterator_register(l);
    mod_lua_stream_register(l);
    mod_lua_list_register(l);
    mod_lua_map_register(l);

    lua_getglobal(l, "require");
    lua_pushstring(l, "aerospike");
    lua_call(l, 1, 1);

    lua_getglobal(l, "require");
    lua_pushstring(l, filename);
    lua_call(l, 1, 1);

    return l;
}

/**
 * Leases a context (lua_State). This will attempt to reuse an 
 * existing context or create a new one as needed.
 *
 * @param m the module from which the context will be leased from.
 * @param filename name of the udf file
 * @return a lua_State to be used as the context.
 */
static int poll_state(context * ctx, cache_item * citem) {
    uint32_t miss = 0;
    uint32_t total = 1;
    if ( ctx->cache_enabled == true ) {
        cache_entry     * centry = NULL;
        int retval = cf_rchash_get(centry_hash, (void *)citem->key, strlen(citem->key), (void *)&centry);
        if (CF_RCHASH_OK == retval ) {
            if (cf_queue_pop(centry->lua_state_q, &citem->state, CF_QUEUE_NOWAIT) != CF_QUEUE_EMPTY) {
                strncpy(citem->key, centry->key, CACHE_ENTRY_KEY_MAX);
                strncpy(citem->gen, centry->gen, CACHE_ENTRY_GEN_MAX);
                LOG("[CACHE] took state: %s (%d)", citem->key, centry->size);
            } else {
                miss = cf_atomic32_incr(&centry->cache_miss);
                citem->state = NULL;
            }
            total = cf_atomic32_incr(&centry->total);
            if (((miss * 100 / total) > 1) && 
                    (total > 100000)) {
                centry->max_cache_size++;
                if (centry->max_cache_size > CACHE_ENTRY_STATE_MAX)
                    centry->max_cache_size = CACHE_ENTRY_STATE_MAX; 
            }
            cf_rc_releaseandfree(centry);
            centry = 0;
			LOG("Cache Miss %d : Total %d \n", miss, total);
        } else {
            centry = NULL;
        }
    }
    else {
        LOG("[CACHE] is disabled.");
    }

    if ( citem->state == NULL ) {
        citem->gen[0] = '\0';
        citem->state = create_state(ctx, citem->key);

        LOG("[CACHE] state created: %s", citem->key);
    }

    return 0;
}

/**
 * Release the context. 
 *
 * @param m the module from which the context was leased from.
 * @param filename name of the udf file
 * @param l the context being released
 * @return 0 on success, otherwise 1
 */
static int offer_state(context * ctx, cache_item * citem) {

    if ( ctx->cache_enabled == true ) {
		// Runnig GCCOLLECT is overkill because with every execution
		// lua itself does a garbage collection. Also do garbage 
		// collection outside the spinlock. arg for GCSTEP 2 is a 
		// random number. Experiment to get better number.
        lua_gc(citem->state, LUA_GCSTEP, 2);
        cache_entry *centry = NULL;
        if (CF_RCHASH_OK == cf_rchash_get(centry_hash, (void *)citem->key, strlen(citem->key), (void *)&centry) ) {
            LOG("[CACHE] found entry: %s (%d)", citem->key, centry->size);
            if (( CF_Q_SZ(centry->lua_state_q) < centry->max_cache_size ) 
                && ( !strncmp(centry->gen, citem->gen, CACHE_ENTRY_GEN_MAX) )) {
                cf_queue_push(centry->lua_state_q, &citem->state);
                LOG("[CACHE] returning state: %s (%d)", citem->key, centry->size);
                citem->state = NULL;
            }
            cf_rc_releaseandfree(centry);
            centry = 0;
        }
        else {
            LOG("[CACHE] entry not found: %s", citem->key);
        }
    }
    else {
        LOG("[CACHE] is disabled.");
    }
    
    // l is not NULL
    // This means that it was not returned to the cache.
    // So, we free it up.
    if ( citem->state != NULL) {
        lua_close(citem->state);
        LOG("[CACHE] state closed: %s", citem->key);
    }

    return 0;
}


typedef struct {
    lua_State * l;
    uint32_t count;
} pushargs_data;

/**
 * Pushes arguments into the Lua stack.
 * We scope the arguments to Lua, so Lua is responsible for releasing them.
 */
static void pushargs_foreach(as_val * val, void * context) {
    pushargs_data * data = (pushargs_data *) context;
    data->count += mod_lua_pushval(data->l, val);
}

/**
 * Pushes arguments from a list on to the stack
 *
 * @param l the lua_State to push arguments onto
 * @param args the list containing the arguments
 * @return the number of arguments pushed onto the stack.
 */
static int pushargs(lua_State * l, as_list * args) {
    pushargs_data data = {
        .l = l,
        .count = 0
    };

    as_list_foreach(args, &data, pushargs_foreach);
    LOG("pushargs: %d", data.count);
    return data.count;
}


static void panic_setjmp(void) {
    setjmp(panic_jmp);
}

static int handle_panic(lua_State * l) {
    // const char * msg = lua_tostring(l, 1);
    // cf_warning(AS_SPROC, (char *) msg);
    longjmp(panic_jmp, 1);
    return 0;
}

// static int handle_error(lua_State * l) {
//     // const char * msg = luaL_optstring(l, 1, 0);
//     // cf_warning(AS_SPROC, (char *) msg);
//     return 0;
// }

static int apply(lua_State * l, int err, int argc, as_result * res) {

    LOG("apply");

    // call apply_record(f, r, ...)
    LOG("call apply_record()");
    int rc = lua_pcall(l, argc, 1, err);

    // Convert the return value from a lua type to a val type
    LOG("convert lua type to val");
    as_val * rv = mod_lua_retval(l);

    if ( rc == 0 ) {
        as_result_setsuccess(res, rv);
    }
    else {
        as_result_setfailure(res, rv);
    }

    // Pop the return value off the stack
    LOG("pop return value from the stack");
    lua_pop(l, -1);

    return 0;
}

static int verify_environment(context * ctx, as_aerospike * as) {
    int rc = 0;

    pthread_rwlock_rdlock(ctx->lock);
    if ( ctx->system_path[0] == '\0' ) {
        char * p = ctx->system_path;
        char msg[256] = {'\0'};
        strcpy(msg, "system-path is invalid: ");
        strncpy(msg+24, p+1, 230);
        as_aerospike_log(as, __FILE__, __LINE__, 1, msg);
        rc += 1;
    }

    if ( ctx->user_path[0] == '\0' ) {
        char * p = ctx->user_path;
        char msg[256] = {'\0'};
        strcpy(msg, "user-path is invalid: ");
        strncpy(msg+22, p+1, 233);
        as_aerospike_log(as, __FILE__, __LINE__, 1, msg);
        rc += 2;
    }
    pthread_rwlock_unlock(ctx->lock);

    return rc;
}

/**
 * Applies a record and arguments to the function specified by a fully-qualified name.
 *
 * Proxies to `m->hooks->apply_record(m, ...)`
 *
 * TODO: Remove redunancies between apply_record() and apply_stream()
 *
 * @param m module from which the fqn will be resolved.
 * @param f fully-qualified name of the function to invoke.
 * @param r record to apply to the function.
 * @param args list of arguments for the function represented as vals 
 * @param result pointer to a val that will be populated with the result.
 * @return 0 on success, otherwise 1
 */
static int apply_record(as_module * m, as_aerospike * as, const char * filename, const char * function, as_rec * r, as_list * args, as_result * res) {

    context *   ctx     = (context *) m->source;    // mod-lua context
    lua_State * l       = (lua_State *) NULL;       // Lua State
    int         argc    = 0;                        // Number of arguments pushed onto the stack
    int         err     = 0;                        // Error handler
    int         rc      = 0;
    
    pthread_rwlock_rdlock(ctx->lock);
    rc = verify_environment(ctx, as);
    if ( rc ) {
        pthread_rwlock_unlock(ctx->lock);
        return rc;
    }

    cache_item  citem   = {
        .key    = "",
        .gen    = "",
        .state  = NULL
    };

    strncpy(citem.key, filename, CACHE_ENTRY_KEY_MAX);
    
    LOG("apply_record: BEGIN");

    // lease a state
    LOG("apply_record: poll state");
    rc = poll_state(ctx, &citem);
    pthread_rwlock_unlock(ctx->lock);

    if ( rc != 0 ) {
        LOG("apply_record: Unable to poll a state");
        return rc;
    }

    l = citem.state;

    // push error handler
    // lua_pushcfunction(l, handle_error);
    // int err = lua_gettop(l);
    
    // push aerospike into the global scope
    LOG("apply_record: push aerospike into the global scope");
    mod_lua_pushaerospike(l, as);
    lua_setglobal(l, "aerospike");
    
    // push apply_record() onto the stack
    LOG("apply_record: push apply_record() onto the stack");
    lua_getglobal(l, "apply_record");
    
    // push function onto the stack
    LOG("apply_record: push function onto the stack");
    lua_getglobal(l, function);

    // push the record onto the stack
    LOG("apply_record: push the record onto the stack");
    mod_lua_pushrecord(l, r);

    // push each argument onto the stack
    LOG("apply_record: push each argument onto the stack");
    argc = pushargs(l, args);

    // function + stream + arglist
    argc = argc + 2;
    
    // apply the function
    LOG("apply_record: apply the function");
    apply(l, err, argc, res);

    // return the state
    pthread_rwlock_rdlock(ctx->lock);
    LOG("apply_record: offer state");
    offer_state(ctx, &citem);
    pthread_rwlock_unlock(ctx->lock);
    
    LOG("apply_record: END");
    return rc;
}



/**
 * Applies function to a stream and set of arguments.
 *
 * Proxies to `m->hooks->apply_stream(m, ...)`
 *
 * TODO: Remove redunancies between apply_record() and apply_stream()
 *
 * @param m module from which the fqn will be resolved.
 * @param f fully-qualified name of the function to invoke.
 * @param s stream to apply to the function.
 * @param args list of arguments for the function represented as vals 
 * @param result pointer to a val that will be populated with the result.
 * @return 0 on success, otherwise 1
 */
static int apply_stream(as_module * m, as_aerospike * as, const char * filename, const char * function, as_stream * istream, as_list * args, as_stream * ostream) {
    int         rc      = 0;
    // context *   ctx     = (context *) m->source;    // mod-lua context
    // lua_State * l       = (lua_State *) NULL;   // Lua State
    // int         argc    = 0;                    // Number of arguments pushed onto the stack
    // int         err     = 0;                    // Error handler
    // int         rc      = 0;

    // pthread_rwlock_rdlock(ctx->lock);
    // rc = verify_environment(ctx, as);
    // if ( rc ) {
    //     pthread_rwlock_unlock(ctx->lock);
    //     return rc;
    // }

    // cache_item  citem   = {
    //     .key    = "",
    //     .gen    = "",
    //     .state  = NULL
    // };

    // strncpy(citem.key, filename, CACHE_ENTRY_KEY_MAX);

    // LOG("apply_stream: BEGIN");

    // // lease a state
    // LOG("apply_stream: poll state");
    // rc = poll_state(ctx, &citem);
    // pthread_rwlock_unlock(ctx->lock);

    // if ( rc != 0 ) {
    //     LOG("apply_stream: Unable to poll a state");
    //     return rc;
    // }

    // l = citem.state;

    // // push error handler
    // // lua_pushcfunction(l, handle_error);
    // // int err = lua_gettop(l);

    // // push aerospike into the global scope
    // LOG("apply_stream: push aerospike into the global scope");
    // mod_lua_pushaerospike(l, as);
    // lua_setglobal(l, "aerospike");

    // // push apply_stream() onto the stack
    // LOG("apply_stream: push apply_stream() onto the stack");
    // lua_getglobal(l, "apply_stream");
    
    // // push function onto the stack
    // LOG("apply_stream: push function onto the stack");
    // lua_getglobal(l, function);

    // // push the stream onto the stack
    // LOG("apply_stream: push the stream iterator onto the stack");
    // mod_lua_pushstream(l, s);

    // // push each argument onto the stack
    // LOG("apply_stream: push each argument onto the stack");
    // argc = pushargs(l, args); 

    // // function + stream + arglist
    // argc = argc + 2;
    
    // // call apply_stream(f, s, ...)
    // LOG("apply_stream: apply the function");
    // apply(l, err, argc, res);

    // // release the context
    // pthread_rwlock_rdlock(ctx->lock);
    // LOG("apply_stream: lose the context");
    // offer_state(ctx, &citem);
    // pthread_rwlock_unlock(ctx->lock);

    // LOG("END");
    return rc;
}

/**
 * Module Hooks
 */
static const as_module_hooks hooks = {
    init,
    configure,
    apply_record,
    apply_stream
};

/**
 * Module
 */
as_module mod_lua = {
    &lua,
    &hooks
};
