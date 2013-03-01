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
#include "mod_lua_bytes.h"
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

struct cache_entry_s;
typedef struct cache_entry_s cache_entry;

struct cache_item_s;
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


struct context_s;
typedef struct context_s context;

struct context_s {
    mod_lua_config      config;
    pthread_rwlock_t *  lock;
};

/******************************************************************************
 * VARIABLES
 ******************************************************************************/

static pthread_rwlock_t lock;

static cf_rchash * centry_hash = NULL;

// static uint32_t cache_size = 0;

static const as_module_hooks hooks;

static jmp_buf panic_jmp;

/**
 * Lua Module Specific Data
 * This will populate the module.source field
 */
static context mod_lua_source = {
    .config = {
        .cache_enabled  = true,
        .system_path    = "",
        .user_path      = "",
        .server_mode    = true
    },
    .lock = NULL
};


/******************************************************************************
 * STATIC FUNCTIONS
 ******************************************************************************/

static int update(as_module *, as_module_event *);
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
uint32_t filename_hash_fn(void *filename, uint32_t len) {   
    char *b = filename;
    uint32_t acc = 0;
    for (int i=0;i<len;i++) {
        acc += *(b+i);
    }
    return(acc);
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
            as_logger_trace(mod_lua.logger, "[CACHE] Added [%s:%p]", key, centry);
        }
    } else { 
        cache_entry_init(ctx, centry, key, gen);
        cf_rc_releaseandfree(centry);
        centry = 0;
    }
	return 0;
}

static int cache_remove_file(context * ctx, const char * filename) {
    char    key[CACHE_ENTRY_KEY_MAX]    = "";
    memcpy(key, filename, CACHE_ENTRY_KEY_MAX);
    *(rindex(key, '.')) = '\0';
    cache_rm(ctx, key);
    return 0;
}

static int cache_add_file(context * ctx, const char * filename) {
    char    key[CACHE_ENTRY_KEY_MAX]    = "";
    char    gen[CACHE_ENTRY_GEN_MAX]    = "";
    memcpy(key, filename, CACHE_ENTRY_KEY_MAX);
    *(rindex(key, '.')) = '\0';
    cache_init(ctx, key, gen);
    return 0;
}

static int cache_scan_dir(context * ctx, const char * directory) {

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
 * @return 0 = success, 1 = source is NULL, 2 = event.data is invalid, 3 = unable to create lock, 4 = unabled to create cache
 * @sychronization: Caller should have a write lock
 */
static int update(as_module * m, as_module_event * e) {
    
    context * ctx = (context *) (m ? m->source : NULL);

    if ( ctx == NULL ) return 1;

    switch ( e->type ) {
        case AS_MODULE_EVENT_CONFIGURE: {
            mod_lua_config * config     = (mod_lua_config *) e->data.config;

            ctx->config.server_mode     = config->server_mode;
            ctx->config.cache_enabled   = config->cache_enabled;

            if ( centry_hash == NULL && ctx->config.cache_enabled ) {
                int rc = cf_rchash_create(&centry_hash, filename_hash_fn, NULL, 0, 64, CF_RCHASH_CR_MT_LOCKPOOL);
                if ( CF_RCHASH_OK != rc ) {
                    return 1;
                }
            }

            if ( ctx->lock == NULL ) {
                ctx->lock = &lock;
                pthread_rwlockattr_t rwattr;
                if (0 != pthread_rwlockattr_init(&rwattr)) {
                    return 3;
                }
                if (0 != pthread_rwlockattr_setkind_np(&rwattr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP)) {
                    return 3;
                }
                if (0 != pthread_rwlock_init(ctx->lock, &rwattr)) {
                    return 3;
                }
            }


            DIR * dir = 0;

            // Attempt to open the directory.
            // If it opens, then set the ctx value.
            // Otherwise, we alert the user of the error when a UDF is called. (for now)

            dir = opendir(config->system_path);
            if ( dir == 0 ) {
                ctx->config.system_path[0] = '\0';
                strncpy(ctx->config.system_path+1, config->system_path, 255);
            }
            else {
                strncpy(ctx->config.system_path, config->system_path, 256);
                closedir(dir);
            }
            dir = NULL;

            // Attempt to open the directory.
            // If it opens, then set the ctx value.
            // Otherwise, we alert the user of the error when a UDF is called. (for now)
            dir = opendir(config->user_path);
            if ( dir == 0 ) {
                ctx->config.user_path[0] = '\0';
                strncpy(ctx->config.user_path+1, config->user_path, 255);
            }
            else {
                strncpy(ctx->config.user_path, config->user_path, 256);
                closedir(dir);
            }
            dir = NULL;

            if ( ctx->config.cache_enabled ) cache_scan_dir(ctx, ctx->config.user_path);

            break;
        }
        case AS_MODULE_EVENT_FILE_SCAN: {
            if ( ctx->config.user_path == NULL ) return 2;
            if ( ctx->config.cache_enabled ) cache_scan_dir(ctx, ctx->config.user_path);
            break;
        }
        case AS_MODULE_EVENT_FILE_ADD: {
            if ( e->data.filename == NULL ) return 2;
            if ( ctx->config.cache_enabled ) cache_add_file(ctx, e->data.filename);
            break;
        }
        case AS_MODULE_EVENT_FILE_REMOVE: {
            if ( e->data.filename == NULL ) return 2;
            if ( ctx->config.cache_enabled ) cache_remove_file(ctx, e->data.filename);
            break;
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

    package_path_set(l, ctx->config.system_path, ctx->config.user_path);
    package_cpath_set(l, ctx->config.system_path, ctx->config.user_path);

    mod_lua_aerospike_register(l);
    mod_lua_record_register(l);
    mod_lua_iterator_register(l);
    mod_lua_stream_register(l);
    mod_lua_list_register(l);
    mod_lua_map_register(l);
    mod_lua_bytes_register(l);

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
    if ( ctx->config.cache_enabled == true ) {
        cache_entry     * centry = NULL;
        int retval = cf_rchash_get(centry_hash, (void *)citem->key, strlen(citem->key), (void *)&centry);
        if (CF_RCHASH_OK == retval ) {
            if (cf_queue_pop(centry->lua_state_q, &citem->state, CF_QUEUE_NOWAIT) != CF_QUEUE_EMPTY) {
                strncpy(citem->key, centry->key, CACHE_ENTRY_KEY_MAX);
                strncpy(citem->gen, centry->gen, CACHE_ENTRY_GEN_MAX);
                as_logger_trace(mod_lua.logger, "[CACHE] took state: %s (%d)", citem->key, centry->max_cache_size);
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
			as_logger_trace(mod_lua.logger, "[CACHE] Miss %d : Total %d", miss, total);
        } else {
            centry = NULL;
        }
    }
    else {
        as_logger_trace(mod_lua.logger, "[CACHE] is disabled.");
    }

    if ( citem->state == NULL ) {
        citem->gen[0] = '\0';
        citem->state = create_state(ctx, citem->key);

        as_logger_trace(mod_lua.logger, "[CACHE] state created: %s", citem->key);
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

    if ( ctx->config.cache_enabled == true ) {
		// Runnig GCCOLLECT is overkill because with every execution
		// lua itself does a garbage collection. Also do garbage 
		// collection outside the spinlock. arg for GCSTEP 2 is a 
		// random number. Experiment to get better number.
        lua_gc(citem->state, LUA_GCSTEP, 2);
        cache_entry *centry = NULL;
        if (CF_RCHASH_OK == cf_rchash_get(centry_hash, (void *)citem->key, strlen(citem->key), (void *)&centry) ) {
            as_logger_trace(mod_lua.logger, "[CACHE] found entry: %s (%d)", citem->key, centry->max_cache_size);
            if (( CF_Q_SZ(centry->lua_state_q) < centry->max_cache_size ) 
                && ( !strncmp(centry->gen, citem->gen, CACHE_ENTRY_GEN_MAX) )) {
                cf_queue_push(centry->lua_state_q, &citem->state);
                as_logger_trace(mod_lua.logger, "[CACHE] returning state: %s (%d)", citem->key, centry->max_cache_size);
                citem->state = NULL;
            }
            cf_rc_releaseandfree(centry);
            centry = 0;
        }
        else {
            as_logger_trace(mod_lua.logger, "[CACHE] entry not found: %s", citem->key);
        }
    }
    else {
        as_logger_trace(mod_lua.logger, "[CACHE] is disabled.");
    }
    
    // l is not NULL
    // This means that it was not returned to the cache.
    // So, we free it up.
    if ( citem->state != NULL) {
        lua_close(citem->state);
        as_logger_trace(mod_lua.logger, "[CACHE] state closed: %s", citem->key);
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
static bool pushargs_foreach(as_val * val, void * context) {
    pushargs_data * data = (pushargs_data *) context;
    as_logger_trace(mod_lua.logger, "pusharg: %s", as_val_tostring(val));
    data->count += mod_lua_pushval(data->l, val);
    return true;
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
    as_logger_trace(mod_lua.logger, "pushargs: %d", data.count);
    return data.count;
}


static void panic_setjmp(void) {
    setjmp(panic_jmp);
}

static int handle_panic(lua_State * l) {
    const char * msg = luaL_optstring(l, 1, 0);
    as_logger_error(mod_lua.logger, "Lua Runtime Fault: %s", msg);
    longjmp(panic_jmp, 1);
    return 0;
}

static int handle_error(lua_State * l) {
    const char * msg = luaL_optstring(l, 1, 0);
    as_logger_error(mod_lua.logger, "Lua Runtime Error: %s", msg);
    // cf_warning(AS_SPROC, (char *) msg);
    return 0;
}

static int apply(lua_State * l, int err, int argc, as_result * res) {

    as_logger_trace(mod_lua.logger, "apply");

    // call apply_record(f, r, ...)
    as_logger_trace(mod_lua.logger, "call function");
    int rc = lua_pcall(l, argc, 1, err);

    as_logger_trace(mod_lua.logger, "rc = %d", rc);

    // Convert the return value from a lua type to a val type
    as_logger_trace(mod_lua.logger, "convert lua type to val");
    as_val * rv = mod_lua_retval(l);


    if ( rc == 0 ) {
        if ( res != NULL ) {
            as_result_setsuccess(res, rv);
        }
    }
    else {
        if ( res != NULL ) {
            as_result_setfailure(res, rv);
        }
    }

    // Pop the return value off the stack
    as_logger_trace(mod_lua.logger, "pop return value from the stack");
    lua_pop(l, -1);

    return 0;
}

static int verify_environment(context * ctx, as_aerospike * as) {
    int rc = 0;

    pthread_rwlock_rdlock(ctx->lock);
    if ( ctx->config.system_path[0] == '\0' ) {
        char * p = ctx->config.system_path;
        char msg[256] = {'\0'};
        strcpy(msg, "system-path is invalid: ");
        strncpy(msg+24, p+1, 230);
        as_aerospike_log(as, __FILE__, __LINE__, 1, msg);
        rc += 1;
    }

    if ( ctx->config.user_path[0] == '\0' ) {
        char * p = ctx->config.user_path;
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

    int         rc      = 0;
    context *   ctx     = (context *) m->source;    // mod-lua context
    lua_State * l       = (lua_State *) NULL;       // Lua State
    int         argc    = 0;                        // Number of arguments pushed onto the stack
    int         err     = 0;                        // Error handler
    
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
    
    as_logger_trace(mod_lua.logger, "apply_record: BEGIN");

    // lease a state
    as_logger_trace(mod_lua.logger, "apply_record: poll state");
    rc = poll_state(ctx, &citem);
    pthread_rwlock_unlock(ctx->lock);

    if ( rc != 0 ) {
        as_logger_trace(mod_lua.logger, "apply_record: Unable to poll a state");
        return rc;
    }

    l = citem.state;

    // push error handler
    // lua_pushcfunction(l, handle_error);
    // int err = lua_gettop(l);
    
    // push aerospike into the global scope
    as_logger_trace(mod_lua.logger, "apply_record: push aerospike into the global scope");
    mod_lua_pushaerospike(l, as);
    lua_setglobal(l, "aerospike");
    
    // push apply_record() onto the stack
    as_logger_trace(mod_lua.logger, "apply_record: push apply_record() onto the stack");
    lua_getglobal(l, "apply_record");
    
    // push function onto the stack
    as_logger_trace(mod_lua.logger, "apply_record: push function onto the stack");
    lua_getglobal(l, function);

    // push the record onto the stack
    as_logger_trace(mod_lua.logger, "apply_record: push the record onto the stack");
    mod_lua_pushrecord(l, r);

    // push each argument onto the stack
    as_logger_trace(mod_lua.logger, "apply_record: push each argument onto the stack");
    argc = pushargs(l, args);

    // function + record + arglist
    argc = argc + 2;
    
    // apply the function
    as_logger_trace(mod_lua.logger, "apply_record: apply the function");
    apply(l, err, argc, res);

    // return the state
    pthread_rwlock_rdlock(ctx->lock);
    as_logger_trace(mod_lua.logger, "apply_record: offer state");
    offer_state(ctx, &citem);
    pthread_rwlock_unlock(ctx->lock);
    
    as_logger_trace(mod_lua.logger, "apply_record: END");
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
    context *   ctx     = (context *) m->source;    // mod-lua context
    lua_State * l       = (lua_State *) NULL;   // Lua State
    int         argc    = 0;                    // Number of arguments pushed onto the stack
    int         err     = 0;                    // Error handler
    
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

    as_logger_trace(mod_lua.logger, "apply_stream: BEGIN");

    // lease a state
    as_logger_trace(mod_lua.logger, "apply_stream: poll state");
    rc = poll_state(ctx, &citem);
    pthread_rwlock_unlock(ctx->lock);

    if ( rc != 0 ) {
        as_logger_trace(mod_lua.logger, "apply_stream: Unable to poll a state");
        return rc;
    }

    l = citem.state;

    // push error handler
    lua_pushcfunction(l, handle_error);
    err = lua_gettop(l);
    
    // push aerospike into the global scope
    as_logger_trace(mod_lua.logger, "apply_stream: push aerospike into the global scope");
    mod_lua_pushaerospike(l, as);
    lua_setglobal(l, "aerospike");

    // push apply_stream() onto the stack
    as_logger_trace(mod_lua.logger, "apply_stream: push apply_stream() onto the stack");
    lua_getglobal(l, "apply_stream");
    
    // push function onto the stack
    as_logger_trace(mod_lua.logger, "apply_stream: push function onto the stack");
    lua_getglobal(l, function);

    // push the stream onto the stack
    // if server_mode == true then SCOPE_SERVER(1) else SCOPE_CLIENT(2)
    as_logger_trace(mod_lua.logger, "apply_stream: push scope onto the stack");
    lua_pushinteger(l, ctx->config.server_mode ? 1 : 2);

    // push the stream onto the stack
    as_logger_trace(mod_lua.logger, "apply_stream: push istream onto the stack");
    mod_lua_pushstream(l, istream);

    as_logger_trace(mod_lua.logger, "apply_stream: push ostream onto the stack");
    mod_lua_pushstream(l, ostream);

    // push each argument onto the stack
    as_logger_trace(mod_lua.logger, "apply_stream: push each argument onto the stack");
    argc = pushargs(l, args); 

    // function + scope + istream + ostream + arglist
    argc = 4 + argc;
    
    // call apply_stream(f, s, ...)
    as_logger_trace(mod_lua.logger, "apply_stream: apply the function");
    apply(l, err, argc, NULL);

    // release the context
    pthread_rwlock_rdlock(ctx->lock);
    as_logger_trace(mod_lua.logger, "apply_stream: lose the context");
    offer_state(ctx, &citem);
    pthread_rwlock_unlock(ctx->lock);

    as_logger_trace(mod_lua.logger, "apply_stream: END");
    return rc;
}


int mod_lua_rdlock(as_module * m) {
    context * c = (context *) ( m ? m->source : NULL );
    if ( c && c->lock ) {
        return pthread_rwlock_rdlock(c->lock);
    }
    return 1;
}

int mod_lua_wrlock(as_module * m) {
    context * c = (context *) ( m ? m->source : NULL );
    if ( c && c->lock ) {
        return pthread_rwlock_wrlock(c->lock);
    }
    return 1;
}

int mod_lua_unlock(as_module * m) {
    context * c = (context *) ( m ? m->source : NULL );
    if ( c && c->lock ) {
        return pthread_rwlock_unlock(c->lock);
    }
    return 1;
}


/**
 * Module Hooks
 */
static const as_module_hooks mod_lua_hooks = {
    .destroy        = NULL,
    .update         = update,
    .apply_record   = apply_record,
    .apply_stream   = apply_stream
};

/**
 * Module
 */
as_module mod_lua = {
    .source         = &mod_lua_source,
    .logger         = NULL,
    .hooks          = &mod_lua_hooks
};
