/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/

#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <setjmp.h>         // needed for gracefully handling lua panics

// #include <fault.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <pthread.h>

#include <citrusleaf/cf_queue.h>
#include <citrusleaf/cf_rchash.h>

#include <citrusleaf/alloc.h>

#include <aerospike/as_aerospike.h>
#include <aerospike/as_types.h>

#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>
#include <aerospike/mod_lua_aerospike.h>
#include <aerospike/mod_lua_record.h>
#include <aerospike/mod_lua_iterator.h>
#include <aerospike/mod_lua_stream.h>
#include <aerospike/mod_lua_list.h>
#include <aerospike/mod_lua_map.h>
#include <aerospike/mod_lua_bytes.h>
#include <aerospike/mod_lua_val.h>

#include "internal.h"

pthread_rwlock_t g_cache_lock = PTHREAD_RWLOCK_INITIALIZER;
#define RDLOCK pthread_rwlock_rdlock(&g_cache_lock)
#define WRLOCK pthread_rwlock_wrlock(&g_cache_lock)
#define UNLOCK pthread_rwlock_unlock(&g_cache_lock)
/******************************************************************************
 * MACROS
 ******************************************************************************/

#define CACHE_TABLE_ENTRY_MAX 128
#define CACHE_ENTRY_KEY_MAX 128
#define CACHE_ENTRY_GEN_MAX 128
#define CACHE_ENTRY_STATE_MAX 128
#define CACHE_ENTRY_STATE_MIN 10

#define MOD_LUA_CONFIG_SYSPATH "/opt/aerospike/sys/udf/lua"
#define MOD_LUA_CONFIG_USRPATH "/opt/aerospike/usr/udf/lua"

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

static const as_module_hooks hooks;

/**
 * Lua Module Specific Data
 * This will populate the module.source field
 */
static context mod_lua_source = {
	.config = {
		.cache_enabled  = true,
		.system_path    = MOD_LUA_CONFIG_SYSPATH,
		.user_path      = MOD_LUA_CONFIG_USRPATH,
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
		if (l) cf_queue_push(centry->lua_state_q, &l);
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
	if ( !key || ( strlen(key) == 0 )) return 0;
	cache_entry     * centry = NULL;
	WRLOCK;
	if (CF_RCHASH_OK != cf_rchash_get(centry_hash, (void *)key, strlen(key), (void *)&centry)) {
		UNLOCK;
		return 0;
	}
	cf_rchash_delete(centry_hash, (void *)key, strlen(key));
	UNLOCK;
	cache_entry_cleanup(centry);
	cf_queue_destroy(centry->lua_state_q);
	cf_rc_releaseandfree(centry);
	centry = 0;
	return 0;
}

int cache_init(context * ctx, const char *key, const char * gen) {
	if (strlen(key) == 0) return 0;
	cache_entry     * centry = NULL;
	WRLOCK;
	if (CF_RCHASH_OK != cf_rchash_get(centry_hash, (void *)key, strlen(key), (void *)&centry)) {
		centry = cf_rc_alloc(sizeof(cache_entry)); 
		cf_atomic32_set(&centry->total, 0);
		cf_atomic32_set(&centry->cache_miss, 0);
		centry->max_cache_size = CACHE_ENTRY_STATE_MAX;
		centry->lua_state_q = cf_queue_create(sizeof(lua_State *), true);
		cache_entry_init(ctx, centry, key, gen);
		int retval = cf_rchash_put(centry_hash, (void *)key, strlen(key), (void *)centry);
		UNLOCK;
		if (retval != CF_RCHASH_OK) {
			// weird should not happen
			cf_queue_destroy(centry->lua_state_q);
			cf_rc_releaseandfree(centry);
			return 1;
		} else {
			as_logger_trace(mod_lua.logger, "[CACHE] Added [%s:%p]", key, centry);
		}
	} else { 
		UNLOCK;
		cache_entry_init(ctx, centry, key, gen);
		cf_rc_releaseandfree(centry);
		centry = 0;
	}
	return 0;
}

static int cache_remove_file(context * ctx, const char * filename) {
	char    key[CACHE_ENTRY_KEY_MAX]    = "";
	memcpy(key, filename, CACHE_ENTRY_KEY_MAX);
	if( rindex(key, '.') ) {
		*(rindex(key, '.')) = '\0';
	}
	cache_rm(ctx, key);
	return 0;
}

static int cache_add_file(context * ctx, const char * filename) {
	char    key[CACHE_ENTRY_KEY_MAX]    = "";
	char    gen[CACHE_ENTRY_GEN_MAX]    = "";
	memcpy(key, filename, CACHE_ENTRY_KEY_MAX);
	char *tmp_char = rindex(key, '.');
	if (  !tmp_char             // Filename without extension
	   || key == tmp_char       // '.' as first character
	   || strlen(tmp_char) <= 1) // '.' in filename , but no extension e.g. "abc."
	{
		as_logger_error(mod_lua.logger, "LUA registration failed : Invalid filename %s", filename);
		return -1;
	}
	*tmp_char = '\0';
	cache_init(ctx, key, gen);
	return 0;
}

static char * dropext(char * name, size_t name_len, const char * ext, size_t ext_len) {
	char * p = (name + name_len - ext_len);
	if ( strncmp(p, ext, ext_len) == 0 ) {
		*p = '\0';
		return name;
	}
	return NULL;
}

static int cache_scan_dir(context * ctx, const char * directory) {

	DIR *           dir     = NULL;
	struct dirent * dentry  = NULL;

	dir = opendir(directory);
	
	if ( dir == 0 ) return -1;

	while ( (dentry = readdir(dir)) && dentry->d_name ) {

		char    key[CACHE_ENTRY_KEY_MAX]    = "";
		char    gen[CACHE_ENTRY_GEN_MAX]    = "";

		memcpy(key, dentry->d_name, CACHE_ENTRY_KEY_MAX);

		char *  base    = NULL;
		size_t  len     = strlen(key);

		// If file ends with ".lua", then drop ".lua"
		base = dropext(key, len, ".lua", 4);
		if ( base != NULL ) {
			cache_init(ctx, key, gen);
			continue;
		}

		// If file ends with ".so", then drop ".so"
		base = dropext(key, len, ".so", 3);
		if ( base != NULL ) {
			cache_init(ctx, key, gen);
			continue;
		}
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
				// No Internal Lock
				int rc = cf_rchash_create(&centry_hash, filename_hash_fn, NULL, 0, 64, 0);
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
			
			// Attempt to open the directory.
			// If it opens, then set the ctx value.
			// Otherwise, we alert the user of the error when a UDF is called. (for now)
			if ( config->system_path[0] != '\0' ) {
				DIR * dir = opendir(config->system_path);
				if ( dir == 0 ) {
					ctx->config.system_path[0] = '\0';
					strncpy(ctx->config.system_path+1, config->system_path, 255);
				}
				else {
					strncpy(ctx->config.system_path, config->system_path, 256);
					closedir(dir);
				}
				dir = NULL;
			}

			// Attempt to open the directory.
			// If it opens, then set the ctx value.
			// Otherwise, we alert the user of the error when a UDF is called. (for now)
			if ( config->user_path[0] != '\0' ) {
				DIR * dir = opendir(config->user_path);
				if ( dir == 0 ) {
					ctx->config.user_path[0] = '\0';
					strncpy(ctx->config.user_path+1, config->user_path, 255);
				}
				else {
					strncpy(ctx->config.user_path, config->user_path, 256);
					closedir(dir);
				}
				dir = NULL;
			}

            if ( ctx->config.cache_enabled ) {
            	// Set up the USER path
            	cache_scan_dir(ctx, ctx->config.user_path);

            	// Set up the SYSTEM path.  Build a string for the new sub-dir "external".
            	size_t syslen = strlen(ctx->config.system_path);
            	if ( ctx->config.system_path[syslen-1] == '/' ) {
            		ctx->config.system_path[syslen-1] = '\0';
            		syslen--;
            	}
            	char external[265] = {0};
            	strncpy(external, ctx->config.system_path, 255);
            	strncpy(external + syslen, "/external", 9);

            	cache_scan_dir(ctx, external);
            }

			break;
		}
		case AS_MODULE_EVENT_FILE_SCAN: {
			if ( ctx->config.user_path[0] == '\0' ) return 2;
            if ( ctx->config.cache_enabled ) {
            	// Set up the USER path
            	cache_scan_dir(ctx, ctx->config.user_path);

            	// Set up the SYSTEM path.  Build a string for the new sub-dir "external".
            	size_t syslen = strlen(ctx->config.system_path);
            	if ( ctx->config.system_path[syslen-1] == '/' ) {
            		ctx->config.system_path[syslen-1] = '\0';
            		syslen--;
            	}
            	char external[265] = {0};
            	strncpy(external, ctx->config.system_path, 255);
            	strncpy(external + syslen, "/external", 9);

            	cache_scan_dir(ctx, external);
            }
			break;
		}
		case AS_MODULE_EVENT_FILE_ADD: {
			if ( e->data.filename == NULL ) return 2;
			if (ctx->config.cache_enabled) {
				if (cache_add_file(ctx, e->data.filename)) {
					return 4;    //Why 4? - No defined error codes, so returning distinct nonzero value.
				}
			}
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
	lua_pushstring(l, system_path);
	lua_pushstring(l, "/external/?.lua");
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
	lua_pushstring(l, system_path);
	lua_pushstring(l, "/external/?.so");
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
 * Checks whether a module is native (i.e., a ".so" file.)
 *
 * @return true if native, otherwise false
 */
static bool is_native_module(context * ctx, const char *filename)
{
	struct stat buf;
	char fn[1024];

	snprintf(fn, sizeof(fn), "%s/%s.so", ctx->config.user_path, filename);
	if (!stat(fn, &buf)) {
		return true;
	}

	snprintf(fn, sizeof(fn), "%s/%s.so", ctx->config.system_path, filename);
	if (!stat(fn, &buf)) {
		return true;
	}

	return false;
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
	int rc = lua_pcall(l, 1, 1, 0);
	if (rc) {
		as_logger_error(mod_lua.logger, "Lua Create Error: %s", lua_tostring(l, -1));
		lua_close(l);
		return NULL;
	}

	if (is_native_module(ctx, filename)) {
		as_logger_trace(mod_lua.logger, "Not requiring native module: %s", filename);
		return l;
	}

	lua_getglobal(l, "require");
	lua_pushstring(l, filename);
	rc = lua_pcall(l, 1, 1, 0);
	if (rc) {
		as_logger_error(mod_lua.logger, "Lua Create Error: %s", lua_tostring(l, -1));
		lua_close(l);
		return NULL;
	}
	return l;
}

/**
 * Leases a context (lua_State). This will attempt to reuse an 
 * existing context or create a new one as needed.
 *
 * @param m the module from which the context will be leased from.
 * @param filename name of the udf file
 * @return populate citem with lua_State to be used as the context.
 * @return 0 on success, otherwise 1
 */
static int poll_state(context * ctx, cache_item * citem) {
	uint32_t miss = 0;
	uint32_t total = 1;
	if ( ctx->config.cache_enabled == true ) {
		cache_entry     * centry = NULL;
		RDLOCK;
		int retval = cf_rchash_get(centry_hash, (void *)citem->key, strlen(citem->key), (void *)&centry);
		UNLOCK;
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
		if (!citem->state) {
			as_logger_trace(mod_lua.logger, "[CACHE] state create failed: %s", citem->key);
			return 1;
		} else { 
			as_logger_trace(mod_lua.logger, "[CACHE] state created: %s", citem->key);
		}
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
		RDLOCK;
		if (CF_RCHASH_OK == cf_rchash_get(centry_hash, (void *)citem->key, strlen(citem->key), (void *)&centry) ) {
			UNLOCK;
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
			UNLOCK;
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

	as_list_foreach(args, pushargs_foreach, &data);
	as_logger_trace(mod_lua.logger, "pushargs: %d", data.count);
	return data.count;
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


	if ( rc == 0 ) {
		if ( res != NULL ) {
			as_val * rv = mod_lua_retval(l);
			as_result_setsuccess(res, rv);
		}
	}
	else {
		if ( res != NULL ) {
			as_val * rv = mod_lua_retval(l);
			as_result_setfailure(res, rv);
		}
	}

	// Pop the return value off the stack
	as_logger_trace(mod_lua.logger, "pop return value from the stack");
	lua_pop(l, -1);

	if ( res == NULL ) {
		return rc;
	} else {
		return 0;
	}
}

// Returning negative number as positive number collide with lua return codes
// Used in udf_rw.c function to print the error message 
// NB: No protection in this function callers should have a multi threaded
//     protection
static int verify_environment(context * ctx, as_aerospike * as) {
	int rc = 0;

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

	return rc;
} 

char * as_module_err_string(int err_no) {
	char *rs;
	switch(err_no) {
		case -1: 
			rs = strdup("UDF: Mod-Lua system path not found");
			break;
		case -2:
			rs = strdup("UDF: Mod-Lua user path not found");
			break;
		case -3: 
			rs = strdup("UDF: Mod-Lua system and user path not found");
			break;
		default:
			rs = malloc(sizeof(char) * 128);
			sprintf(rs, "UDF: Excution Error - Check Logs %d", err_no);
			break;
	}
	return rs;
}

static void populate_error(lua_State * l, const char * filename, int rc, as_module_error * err) {

	const char * message = lua_tostring(l, -1);

	err->scope = 2; // UDF Module

	switch ( rc ) {
		case LUA_ERRSYNTAX : {
			err->code = 10;
			break;
		}
		case LUA_ERRRUN : {
			err->code = 11;
			break;
		}
		case LUA_ERRMEM : {
			err->code = 12;
			break;
		}
		case LUA_ERRERR : {
			err->code = 13;
			break;
		}
		default : {
			err->code = 0;
			break;
		}
	}

	if ( err->code == 10 || err->code == 11 ) {
		if ( message[0] == '[' ) {
			char * fileL = strchr(message,'"');
			if ( fileL ) {
				fileL++;
				char * fileR = strchr(fileL, '"');
				if ( fileR ) {
					memcpy(err->file, fileL, fileR-fileL);
					err->file[fileR-fileL] = '\0';
					char * lineL = strchr(fileR, ':');
					if ( lineL ) {
						lineL++;
						char * lineR = strchr(lineL, ':');
						if ( lineR ) {
							char line[11] = {0};
							memcpy(line, lineL, lineR-lineL);
							err->line = atoi(line);
							lineR += 2;
							memcpy(err->message, lineR, 1024);
							err->message[1023] = '\0';
						}
					}
				}
			}
		}
	}
	else {

		lua_Debug ar;
		lua_getfield(l, LUA_GLOBALSINDEX, "f");
		lua_getinfo(l, ">Snl", &ar);

		printf("## name = %s\n", ar.name);
		printf("## namewhat = %s\n", ar.namewhat);
		printf("## what = %s\n", ar.what);
		printf("## source = %s\n", ar.source);
		printf("## currentline = %d\n", ar.currentline);
		printf("## nups = %d\n", ar.nups);
		printf("## linedefined = %d\n", ar.linedefined);
		printf("## lastlinedefined = %d\n", ar.lastlinedefined);
		printf("## short_src = %s\n", ar.short_src);

		memcpy(err->message, message, 1024);
		err->message[1023] = '\0';
		memcpy(err->file, filename, 256);
		err->file[255] = '\0';
		err->line = ar.currentline;
		memcpy(err->func, ar.name, 256);
		err->func[255] = '\0';
	}
}

/**
 * Validates a UDF module
 */
static int validate(as_module * m, as_aerospike * as, const char * filename, const char * content, uint32_t size, as_module_error * err) {
	
	int rc = 0;

	err->scope = 0;
	err->code = 0;
	err->message[0] = '\0';
	err->file[0] = '\0';
	err->line = 0;
	err->func[0] = '\0';

	context * ctx = (context *) m->source;
	lua_State * l = NULL;
	
	l = lua_open();

	if ( l == NULL ) {
		err->scope = 1;
		err->code = 1;
		strcpy(err->message,"Unable to create a new Lua state");
		goto Cleanup;
	}

	luaL_openlibs(l);

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

	rc = lua_pcall(l, 1, 1, 0);
	if ( rc ) {
		populate_error(l, filename, rc, err);
		goto Cleanup;
	}

	rc = luaL_loadbuffer(l, content, size, filename);
	if ( rc ) {
		populate_error(l, filename, rc, err);
		goto Cleanup;
	}

	rc = lua_pcall(l, 0, 1, 0);
	if ( rc ) {
		populate_error(l, filename, rc, err);
		goto Cleanup;
	}

Cleanup:
	if ( err->code == 0 ) {
		as_logger_trace(mod_lua.logger, "Lua Validation Pass for '%s'", filename);
	}
	else {
		as_logger_debug(mod_lua.logger, "Lua Validation Fail for '%s': (%d) %s", filename, err->code, err->message);
	}
	
	if ( l != NULL ) {
		lua_close(l);
	}

	return err->code;
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
	rc = apply(l, err, argc, res);

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
	rc = apply(l, err, argc, NULL);

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
	.validate		= validate,
	.apply_record   = apply_record,
	.apply_stream   = apply_stream
};

/**
 * Module
 */
as_module mod_lua = {
	.source         = &mod_lua_source,
	.logger         = NULL,
	.memtracker     = NULL,
	.hooks          = &mod_lua_hooks
};
