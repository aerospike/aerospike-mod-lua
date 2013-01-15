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

#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <setjmp.h>         // needed for gracefully handling lua panics

// #include <fault.h>

// LUA Shizzle
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>


#define LOG(m) \
    // printf("%s:%d  -- %s\n",__FILE__,__LINE__, m);

typedef struct mod_lua_context_s mod_lua_context;

static const as_module_hooks hooks;

static jmp_buf panic_jmp;

static int init(as_module *);
static int configure(as_module *, void *);
static int apply_record(as_module *, as_aerospike *, const char *, const char *, as_rec *, as_list *, as_result *);
static int apply_stream(as_module *, as_aerospike *, const char *, const char *, as_stream *, as_list *, as_result *);

static lua_State * create_state();
static lua_State * open_state(as_module *, const char *);
static int close_state(as_module *, const char *, lua_State *);

static void panic_setjmp(void);
static int handle_error(lua_State *);
static int handle_panic(lua_State *);

/**
 * Lua Module Specific Data
 * This will populate the module.source field
 */
static struct mod_lua_context_s {
    lua_State * lua_cache;
    bool        cache_enabled;
    char *      system_path;
    char *      user_path;
} lua = {
    .lua_cache      = NULL,
    .cache_enabled  = true,
    .system_path    = NULL,
    .user_path      = NULL
};

/**
 * Module Initializer.
 * This sets up the module before use. This is called only once on startup.
 *
 * @param m the module being initialized.
 * @return 0 on success, otherwhise 1
 */
static int init(as_module * m) {
    return 0;
}

/**
 * Module Configurator. 
 * This configures and reconfigures the module. This can be called an
 * arbitrary number of times during the lifetime of the server.
 *
 * @param m the module being configured.
 * @return 0 on success, otherwhise 1
 */
static int configure(as_module * m, void * config) {
    mod_lua_context *   ctx = (mod_lua_context *) m->source;
    mod_lua_config *    cfg = (mod_lua_config *) config;
    
    ctx->cache_enabled  = cfg->cache_enabled;
    ctx->system_path    = strdup(cfg->system_path);
    ctx->user_path      = strdup(cfg->user_path);

    if ( ctx->cache_enabled ) {
        
        if ( ctx->lua_cache ) {
            lua_close(ctx->lua_cache);
            ctx->lua_cache = NULL;
        }

        ctx->lua_cache = lua_open();

        DIR *           dir             = NULL;
        struct dirent * entry           = NULL;
        char            filename[128]   = {0};

        dir = opendir(ctx->user_path);
        
        if ( dir == 0 ) {
            return -1;
        }
        
        while ( (entry = readdir(dir)) && entry->d_name ) {

            char * name = entry->d_name;
            size_t len = strlen(name);

            if ( len < 4 ) continue;

            if ( strcmp(&name[len-4],".lua") != 0 ) continue;
            
            memcpy(filename, name, len-4); filename[len-4]=0; 

            lua_State * l = create_state(m, filename);

            lua_pushlightuserdata(ctx->lua_cache, l);
            lua_setglobal(ctx->lua_cache, filename);
        }

        closedir(dir);
        
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
static lua_State * create_state(as_module * m, const char * filename) {
    mod_lua_context *   ctx = (mod_lua_context *) m->source;
    lua_State *         l   = NULL;

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
 * @param fqn the fully qualified name of the function the context will be used for.
 * @return a lua_State to be used as the context.
 */
static lua_State * open_state(as_module * m, const char * filename) {

    mod_lua_context *   ctx = (mod_lua_context *) m->source;

    if ( ctx->cache_enabled == true && ctx->lua_cache != NULL ) {
        lua_getglobal(ctx->lua_cache, filename);
        lua_State * root = (lua_State *) lua_touserdata(ctx->lua_cache, -1);

        if ( root == NULL ) {
            root = create_state(m, filename);
            lua_pushlightuserdata(ctx->lua_cache, root);
            lua_setglobal(ctx->lua_cache, filename);
        }

        if ( root == NULL ) return NULL;

        lua_State * node = lua_newthread(root);
        lua_pop(ctx->lua_cache, -1);
        lua_pop(root, -1);

        return node;
    }
    else {
        return create_state(m, filename);
    }
}

/**
 * Release the context. 
 *
 * @param m the module from which the context was leased from.
 * @param fqn the fully qualified name of the function the context was leased to.
 * @param l the context being released
 * @return 0 on success, otherwise 1
 */
static int close_state(as_module * m, const char * fqn, lua_State * l) {
    mod_lua_context * ctx = (mod_lua_context *) m->source;

    if ( ctx->cache_enabled == true && ctx->lua_cache != NULL ) {
        lua_gc(l, LUA_GCCOLLECT, 0);
    }
    else {
        lua_gc(l, LUA_GCCOLLECT, 0);
        lua_close(l);
    }
    return 0;
}

/**
 * Pushes arguments from a list on to the stack
 *
 * @param l the lua_State to push arguments onto
 * @param args the list containing the arguments
 * @return the number of arguments pushed onto the stack.
 */
static int pushargs(lua_State * l, as_list * args) {
    LOG("pushargs()");
    int argc = 0;
    as_iterator * i = as_list_iterator(args);
    while( as_iterator_has_next(i) ) {
        argc += mod_lua_pushval(l, as_iterator_next(i));
    }
    as_iterator_free(i);
    return argc;
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

static int handle_error(lua_State * l) {
    // const char * msg = luaL_optstring(l, 1, 0);
    // cf_warning(AS_SPROC, (char *) msg);
    return 0;
}

static int apply(lua_State * l, int err, int argc, as_result * res) {

    // call apply_record(f, r, ...)
    LOG("call apply_record()");
    int rc = lua_pcall(l, argc, 1, err);

    // Convert the return value from a lua type to a val type
    LOG("convert lua type to val");
    as_val * rv = mod_lua_toval(l, -1);

    if ( rc == 0 ) {
        as_result_tosuccess(res, rv);
    }
    else {
        as_result_tofailure(res, rv);
    }

    // Pop the return value off the stack
    LOG("pop return value from the stack");
    lua_pop(l, -1);

    return 0;
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

    lua_State * l       = (lua_State *) NULL;   // Lua State
    int         argc    = 0;                    // Number of arguments pushed onto the stack
    int         err     = 0;                    // Error handler
    
    LOG("BEGIN")

    // lease a context
    LOG("open context")
    l = open_state(m, filename);

    // push error handler
    // lua_pushcfunction(l, handle_error);
    // int err = lua_gettop(l);
    
    // push aerospike into the global scope
    LOG("push aerospike into the global scope");
    mod_lua_pushaerospike(l, as);
    lua_setglobal(l, "aerospike");
    
    // push apply_record() onto the stack
    LOG("push apply_record() onto the stack");
    lua_getglobal(l, "apply_record");
    
    // push function onto the stack
    LOG("push function onto the stack");
    lua_getglobal(l, function);

    // push the record onto the stack
    LOG("push the record onto the stack");
    mod_lua_pushrecord(l, r);

    // push each argument onto the stack
    LOG("push each argument onto the stack");
    argc = pushargs(l, args);

    // function + stream + arglist
    argc = argc + 2;
    
    // apply the function
    apply(l, err, argc, res);

    // release the context
    LOG("close the context");
    close_state(m, filename, l);
    
    LOG("END");
    return 0;
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
static int apply_stream(as_module * m, as_aerospike * as, const char * filename, const char * function, as_stream * s, as_list * args, as_result * res) {

    lua_State * l       = (lua_State *) NULL;   // Lua State
    int         argc    = 0;                    // Number of arguments pushed onto the stack
    int         err     = 0;                    // Error handler

    LOG("apply_stream: BEGIN")

    // lease a context
    LOG("open context")
    l = open_state(m, filename);

    // push error handler
    // lua_pushcfunction(l, handle_error);
    // int err = lua_gettop(l);

    // push aerospike into the global scope
    LOG("push aerospike into the global scope");
    mod_lua_pushaerospike(l, as);
    lua_setglobal(l, "aerospike");

    // push apply_stream() onto the stack
    LOG("push apply_stream() onto the stack");
    lua_getglobal(l, "apply_stream");
    
    // push function onto the stack
    LOG("push function onto the stack");
    lua_getglobal(l, function);

    // push the stream onto the stack
    LOG("push the stream iterator onto the stack");
    mod_lua_pushstream(l, s);

    // push each argument onto the stack
    LOG("push each argument onto the stack");
    argc = pushargs(l, args); 

    // function + stream + arglist
    argc = argc + 2;
    
    // call apply_stream(f, s, ...)
    LOG("call apply_stream()");
    apply(l, err, argc, res);

    // release the context
    LOG("close the context");
    close_state(m, filename, l);

    LOG("END");
    return 0;
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
