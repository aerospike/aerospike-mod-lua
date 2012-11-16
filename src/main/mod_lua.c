#include "mod_lua.h"
#include "mod_lua_aerospike.h"
#include "mod_lua_record.h"
#include "mod_lua_iterator.h"
#include "mod_lua_stream.h"
#include "mod_lua_val.h"
#include "as_aerospike.h"
#include "as_types.h"

#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>

// LUA Shizzle
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

/**
 * Lua Module Specific Data
 * This will populate the module.source field
 */
struct mod_lua_context_s {
};

/**
 * Single Instance of the Lua Module Specific Data
 */
static mod_lua_context lua = {};

#define LOG(m) \
    // printf("%s:%d  -- %s\n",__FILE__,__LINE__, m);

static const as_module_hooks hooks;

static int init(as_module *);
static int configure(as_module *);
static int apply_record(as_module *, as_aerospike * as, const char *, as_rec *, as_list *, as_result *);
static int apply_stream(as_module *, as_aerospike * as, const char *, as_stream *, as_list *, as_result *);

static lua_State * create_state();
static lua_State * open_state(as_module * m, const char * f);
static int close_state(as_module * m, const char * f, lua_State * l);


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
static int configure(as_module * m) {
    return 0;
}

/**
 * Creates a new context (lua_State) populating it with default values.
 *
 * @return a new lua_State
 */
static lua_State * create_state() {

    lua_State * l = lua_open();
    luaL_openlibs(l);
    
    int error = luaL_dofile(l, "/home/chris/projects/misc/aerospike-lua/src/lua/aerospike.lua");
    if ( error ) {
        fprintf(stderr, "%s", lua_tostring(l, -1));
        lua_pop(l, 1);  // pop error message from the stack
    }

    mod_lua_aerospike_register(l);
    mod_lua_record_register(l);
    mod_lua_iterator_register(l);
    mod_lua_stream_register(l);

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
static lua_State * open_state(as_module * m, const char * fqn) {
    lua_State * l = create_state();
    // lua_State * L = ((mod_lua_context *)m->source)->root;
    // lua_State * l = lua_newthread(((mod_lua_context *)m->source)->root);
    // lua_pop(L, -1);
    return l;
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
    lua_gc(l, LUA_GCCOLLECT, 0);
    lua_close(l);
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
        const as_val * arg = as_iterator_next(i);
        as_integer * i = NULL;
        as_string * s  = NULL;
        switch ( as_val_type(arg) ) {
            case AS_INTEGER :
                i = as_integer_fromval(arg);
                lua_pushinteger(l, as_integer_toint(i));
                argc++;
                i = NULL;
                break;
            case AS_STRING :
                s = as_string_fromval(arg);
                lua_pushstring(l, as_string_tostring(s));
                s = NULL;
                argc++;
                break;
            default:
                break;
        }
    }
    return argc;
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
static int apply_record(as_module * m, as_aerospike * as, const char * fqn, as_rec * r, as_list * args, as_result * res) {

    lua_State *     l       = (lua_State *) NULL;     // Lua State
    int             argc    = 0;                      // Number of arguments pushed onto the stack
    as_val *        ret     = (as_val *) NULL;        // Return value from call

    LOG("BEGIN")

    // lease a context
    LOG("open context")
    l = open_state(m, fqn);
    
    // push aerospike into the global scope
    mod_lua_pushaerospike(l, as);
    lua_setglobal(l, "aerospike");
    
    // push apply_record() onto the stack
    LOG("push apply_record() onto the stack");
    lua_getglobal(l, "apply_record");
    
    // push the fully qualified name (fqn) of the function onto the stack
    LOG("push the fully qualified name (fqn) of the function onto the stack");
    lua_pushstring(l, fqn);

    // push the record onto the stack
    LOG("push the record onto the stack");
    mod_lua_pushrecord(l, r);

    // push each argument onto the stack
    LOG("push each argument onto the stack");
    argc = pushargs(l, args) + 2;
    
    // call apply_record(f, r, argc)
    LOG("call apply_record()");
    lua_pcall(l, argc, 1, 0);

    // Convert the return value from a lua type to a val type
    LOG("convert lua type to val");
    ret = mod_lua_toval(l, -1);

    // Make it a success
    // Note: This is too simplistic, as it is not catching errors.
    as_result_tosuccess(res, ret);

    // Pop the return value off the stack
    LOG("pop return value from the stack");
    lua_pop(l, -1);

    // release the context
    LOG("close the context");
    close_state(m, fqn, l);

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
static int apply_stream(as_module * m, as_aerospike * as, const char * fqn, as_stream * s, as_list * args, as_result * res) {

    lua_State * l = (lua_State *) NULL;     // Lua State
    int argc = 0;                           // Number of arguments pushed onto the stack
    as_val * ret = (as_val *) NULL;               // Return value from call

    LOG("apply_stream: BEGIN")

    // lease a context
    LOG("open context")
    l = open_state(m, fqn);

    // push aerospike into the global scope
    mod_lua_pushaerospike(l, as);
    lua_setglobal(l, "aerospike");

    // push apply_stream() onto the stack
    LOG("push apply_stream() onto the stack");
    lua_getglobal(l, "apply_stream");
    
    // push the fully qualified name (fqn) of the function onto the stack
    LOG("push the fully qualified name (fqn) of the function onto the stack");
    lua_pushstring(l, fqn);

    // push the stream onto the stack
    LOG("push the stream iterator onto the stack");
    mod_lua_pushstream(l, s);

    // push each argument onto the stack
    LOG("push each argument onto the stack");
    argc = pushargs(l, args) + 2;
    
    // call apply_stream(f, s, argc)
    LOG("call apply_stream()");
    lua_pcall(l, argc, 1, 0);

    // Convert the return value from a lua type to a val type
    LOG("convert lua type to val");
    ret = mod_lua_toval(l, -1);
    
    // Make it a success
    // Note: This is too simplistic, as it is not catching errors.
    as_result_tosuccess(res, ret);

    // Pop the return value off the stack
    LOG("pop return value from the stack");
    lua_pop(l, -1);

    // release the context
    LOG("close the context");
    close_state(m, fqn, l);

    LOG("apply_stream: END");
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
