/*
 * Copyright 2008-2018 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#include <aerospike/mod_lua.h>

#include <aerospike/as_aerospike.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_dir.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_types.h>
#include <aerospike/mod_lua_aerospike.h>
#include <aerospike/mod_lua_bytes.h>
#include <aerospike/mod_lua_config.h>
#include <aerospike/mod_lua_geojson.h>
#include <aerospike/mod_lua_iterator.h>
#include <aerospike/mod_lua_list.h>
#include <aerospike/mod_lua_map.h>
#include <aerospike/mod_lua_record.h>
#include <aerospike/mod_lua_stream.h>
#include <aerospike/mod_lua_val.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_hash_math.h>
#include <citrusleaf/cf_queue.h>

#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>
#include <pthread.h>
#include <setjmp.h>         // needed for gracefully handling lua panics
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "internal.h"

pthread_rwlock_t g_cache_lock = PTHREAD_RWLOCK_INITIALIZER;
#define RDLOCK pthread_rwlock_rdlock(&g_cache_lock)
#define WRLOCK pthread_rwlock_wrlock(&g_cache_lock)
#define UNLOCK pthread_rwlock_unlock(&g_cache_lock)
/******************************************************************************
 * MACROS
 ******************************************************************************/

#define CACHE_TABLE_ENTRY_MAX 128 // Doesn't appear to be used
#define CACHE_ENTRY_KEY_MAX 128
#define CACHE_ENTRY_GEN_MAX 128
#define CACHE_ENTRY_STATE_MAX 128
#define CACHE_ENTRY_STATE_MIN 10

#define LUA_PARAM_COUNT_THRESHOLD 20 // warn if a function call exceeds this

#define MOD_LUA_CONFIG_USRPATH "/opt/aerospike/usr/udf/lua"

/******************************************************************************
 * TYPES
 ******************************************************************************/

struct cache_entry_s;
typedef struct cache_entry_s cache_entry;

struct cache_item_s;
typedef struct cache_item_s cache_item;

struct cache_entry_s {
	char key[CACHE_ENTRY_KEY_MAX];
	char gen[CACHE_ENTRY_GEN_MAX];
	uint64_t cache_miss;
	uint64_t total;
	cf_queue* lua_state_q;
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

typedef struct lua_hash_ele_s {
	struct lua_hash_ele_s* next;
	cache_entry* value;
	char key[]; // key_size bytes of key
} lua_hash_ele;

typedef struct lua_hash_s {
	uint32_t ele_size;
	uint32_t n_rows;
	uint8_t* table;
} lua_hash;

/******************************************************************************
 * VARIABLES
 ******************************************************************************/

static pthread_rwlock_t lock;

static lua_hash* g_lua_hash = NULL;

/**
 * Lua Module Specific Data
 * This will populate the module.source field
 */
static context mod_lua_source = {
	.config = {
		.cache_enabled  = true,
		.user_path      = MOD_LUA_CONFIG_USRPATH,
		.server_mode    = true
	},
	.lock = NULL
};


/******************************************************************************
 * STATIC FORWARD DECLARATIONS
 ******************************************************************************/

static int update(as_module *, as_module_event *);
static int apply_record(as_module *, as_udf_context *, const char *, const char *, as_rec *, as_list *, as_result *);
static int apply_stream(as_module *, as_udf_context *, const char *, const char *, as_stream *, as_list *, as_stream *, as_result *);

static lua_State * create_state(context *, const char *filename);
static int poll_state(context *, cache_item *);
static int offer_state(context *, cache_item *);
static void destroy_cache_entry(cache_entry *centry);
static inline void lua_hash_call_cb_if(void (*cb)(cache_entry *), cache_entry *centry);
static inline lua_hash_ele* lua_hash_get_row_head(const lua_hash* h, const char* key);


/******************************************************************************
 * FORWARD DECLARATIONS
 *****************************************************************************/

void lua_hash_clear(lua_hash* h, void (*cb)(cache_entry *));
lua_hash* lua_hash_create(uint32_t key_size, uint32_t n_rows);
void lua_hash_destroy(lua_hash* h);
bool lua_hash_get(const lua_hash* h, const char* key, cache_entry** p_value);
cache_entry* lua_hash_put(lua_hash* h, const char* key, cache_entry* value);
cache_entry* lua_hash_remove(lua_hash* h, const char* key);


/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

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
	strncpy(centry->key, key, CACHE_ENTRY_KEY_MAX - 1);
	strncpy(centry->gen, gen, CACHE_ENTRY_GEN_MAX - 1);

	return 0;
}

int
cache_rm(context* ctx, const char *key)
{
	if (key == NULL || *key == '\0') {
		return 0;
	}

	WRLOCK;
	cache_entry* centry = lua_hash_remove(g_lua_hash, key);
	UNLOCK;

	if (centry != NULL) {
		destroy_cache_entry(centry);
	}

	return 0;
}

int cache_init(context * ctx, const char *key, const char * gen) {
	if (key == NULL || *key == '\0') {
		return 0;
	}

	cache_entry     * centry = NULL;
	WRLOCK;

	if (! lua_hash_get(g_lua_hash, key, &centry)) {
		centry = cf_malloc(sizeof(cache_entry));
		as_store_uint64(&centry->total, 0);
		as_store_uint64(&centry->cache_miss, 0);
		// Start Small and grow (as necessary) up to the max
		centry->lua_state_q = cf_queue_create(sizeof(lua_State *), true);
		cache_entry_init(ctx, centry, key, gen);
		lua_hash_put(g_lua_hash, key, centry);
		UNLOCK;
		as_log_trace("[CACHE] Added [%s:%p]", key, centry)
	} else {
		UNLOCK;
		cache_entry_init(ctx, centry, key, gen);
		centry = 0;
	}
	return 0;
}

static int cache_remove_file(context * ctx, const char * filename) {
	char key[CACHE_ENTRY_KEY_MAX];
	if (as_strncpy(key, filename, sizeof(key))) {
		as_log_error("LUA cache remove failed : filename truncated %s", key);
		return -1;
	}

	char* p = strrchr(key, '.');

	if (p) {
		*p = '\0';
	}
	cache_rm(ctx, key);
	return 0;
}

static int cache_add_file(context * ctx, const char * filename) {
	char key[CACHE_ENTRY_KEY_MAX];
	if (as_strncpy(key, filename, sizeof(key))) {
		as_log_error("LUA registration failed : filename truncated %s", key);
		return -1;
	}

	char *tmp_char = strrchr(key, '.');
	if (  !tmp_char             // Filename without extension
	   || key == tmp_char       // '.' as first character
	   || strlen(tmp_char) <= 1) // '.' in filename , but no extension e.g. "abc."
	{
		as_log_error("LUA registration failed : Invalid filename %s", filename);
		return -1;
	}
	*tmp_char = '\0';

	char gen[CACHE_ENTRY_GEN_MAX];
	gen[0] = 0;

	cache_init(ctx, key, gen);
	return 0;
}

static char * dropext(char * name, size_t name_len, const char * ext, size_t ext_len) {
	char * p = (name + name_len - ext_len);
	if (ext_len < name_len && strncmp(p, ext, ext_len) == 0) {
		*p = '\0';
		return name;
	}
	return NULL;
}

static bool hasext(const char * name, size_t name_len, const char * ext, size_t ext_len) {
	const char * p = (name + name_len - ext_len);
	if (ext_len < name_len && strncmp(p, ext, ext_len) == 0) {
		return true;
	}
	return false;
}

static int cache_scan_dir(context * ctx, const char * directory) {

	as_dir dir;
	const char* entry;

	if (!as_dir_open(&dir, directory)) {
		return -1;
	}

	while ( (entry = as_dir_read(&dir)) ) {

		char key[CACHE_ENTRY_KEY_MAX];
		if (as_strncpy(key, entry, sizeof(key))) {
			as_log_error("LUA cache dir scan skipping truncated entry %s", key);
			continue;
		}

		char gen[CACHE_ENTRY_GEN_MAX];
		gen[0] = 0;

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

	as_dir_close(&dir);
	return 0;
}

static void
destroy_cache_entry(cache_entry *centry)
{
	cache_entry_cleanup(centry);
	cf_queue_destroy(centry->lua_state_q);
	cf_free(centry);
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

			if (g_lua_hash == NULL && ctx->config.cache_enabled) {
				// No Internal Lock
				g_lua_hash = lua_hash_create(CACHE_ENTRY_KEY_MAX, 64);
			}

			if ( ctx->lock == NULL ) {
				ctx->lock = &lock;

#if defined(_MSC_VER)
				if (0 != pthread_rwlock_init(ctx->lock, NULL)) {
					return 3;
				}
#else
				pthread_rwlockattr_t rwattr;
				if (0 != pthread_rwlockattr_init(&rwattr)) {
					return 3;
				}

#if defined(__USE_UNIX98) || defined (__USE_XOPEN2K)
				if (0 != pthread_rwlockattr_setkind_np(&rwattr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP)) {
					return 3;
				}
#endif
				if (0 != pthread_rwlock_init(ctx->lock, &rwattr)) {
					return 3;
				}
#endif
			}

			// Attempt to open the directory.
			// If it opens, then set the ctx value.
			// Otherwise, we alert the user of the error when a UDF is called. (for now)
			if ( config->user_path[0] != '\0' ) {
				if (!as_dir_exists(config->user_path)) {
					ctx->config.user_path[0] = '\0';
					strncpy(ctx->config.user_path+1, config->user_path, 255);
				}
				else {
					strncpy(ctx->config.user_path, config->user_path, 256);
				}
			}

            if ( ctx->config.cache_enabled ) {
            	// Set up the USER path
            	cache_scan_dir(ctx, ctx->config.user_path);
            }

			break;
		}
		case AS_MODULE_EVENT_FILE_SCAN: {
			if ( ctx->config.user_path[0] == '\0' ) return 2;
            if ( ctx->config.cache_enabled ) {
            	// Set up the USER path
            	cache_scan_dir(ctx, ctx->config.user_path);
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
			if ( ctx->config.cache_enabled ) {
				if (cache_remove_file(ctx, e->data.filename) != 0) {
					return 2;
				}
			}
			break;
		}
		case AS_MODULE_EVENT_CLEAR_CACHE: {
			if (ctx->config.cache_enabled) {
				WRLOCK;
				lua_hash_clear(g_lua_hash, &destroy_cache_entry);
				UNLOCK;
			}
			break;
		}
	}

	return 0;
}

static void package_path_set(lua_State * l, char * user_path) {
	int stack = 0;

	lua_getglobal(l, "package");
	lua_getfield(l, -1, "path");
	stack += 1;

	lua_pushstring(l, ";");
	lua_pushstring(l, user_path);
	lua_pushstring(l, "/?.lua");
	stack += 3;

	lua_concat(l, stack);

	lua_setfield(l, -2, "path");
	lua_pop(l, 1);
}

static void package_cpath_set(lua_State * l, char * user_path) {
	int stack = 0;

	lua_getglobal(l, "package");
	lua_getfield(l, -1, "cpath");
	stack += 1;

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

	return false;
}

extern const char as_lua_as[];
extern const char as_lua_stream_ops[];
extern const char as_lua_aerospike[];

extern size_t as_lua_as_size;
extern size_t as_lua_stream_ops_size;
extern size_t as_lua_aerospike_size;

static bool load_buffer(lua_State* l, const char* script, size_t size, const char* name)
{
	if (luaL_loadbuffer(l, script, size - 1, name) || lua_pcall(l, 0, LUA_MULTRET, 0)) {
		as_log_error("Failed to load lua string: %s %d", name, (int)size);
		lua_close(l);
		return false;
	}
	return true;
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

	package_path_set(l, ctx->config.user_path);
	package_cpath_set(l, ctx->config.user_path);

	mod_lua_aerospike_register(l);
	mod_lua_record_register(l);
	mod_lua_iterator_register(l);
	mod_lua_stream_register(l);
	mod_lua_list_register(l);
	mod_lua_map_register(l);
	mod_lua_bytes_register(l);
	mod_lua_geojson_register(l);

	if (! load_buffer(l, as_lua_as, as_lua_as_size, "as.lua")) {
		return NULL;
	}

	if (! load_buffer(l, as_lua_stream_ops, as_lua_stream_ops_size, "stream_ops.lua")) {
		return NULL;
	}

	if (! load_buffer(l, as_lua_aerospike, as_lua_aerospike_size, "aerospike.lua")) {
		return NULL;
	}

	if (is_native_module(ctx, filename)) {
		as_log_trace("Not requiring native module: %s", filename);
		return l;
	}

	lua_getglobal(l, "require");
	lua_pushstring(l, filename);
	int rc = lua_pcall(l, 1, 1, 0);
	if (rc) {
		as_log_error("Lua Create Error: %s", lua_tostring(l, -1));
		lua_close(l);
		return NULL;
	}
	as_log_debug("Size of the lua state created for the file %s in KB %d",
			filename, lua_gc(l, LUA_GCCOUNT,0));
	return l;
}

/**
 * Leases a context (lua_State). This will attempt to reuse an
 * existing context or create a new one as needed.
 *
 * @param ctx mod lua context.
 * @param citem context cache item to be populated.
 * @return populate citem with lua_State to be used as the context.
 * @return 0 on success, otherwise 1
 */
static int poll_state(context * ctx, cache_item * citem) {
	if ( ctx->config.cache_enabled == true ) {
		cache_entry     * centry = NULL;
		RDLOCK;
		if (lua_hash_get(g_lua_hash, citem->key, &centry)) {
			uint64_t miss;
			if (cf_queue_pop(centry->lua_state_q, &citem->state, CF_QUEUE_NOWAIT) != CF_QUEUE_EMPTY) {
				strncpy(citem->key, centry->key, CACHE_ENTRY_KEY_MAX);
				strncpy(citem->gen, centry->gen, CACHE_ENTRY_GEN_MAX);
				as_log_trace("[CACHE] took state: %s", citem->key);
				miss = as_load_uint64(&centry->cache_miss);
			} else {
				as_log_trace("[CACHE] miss state: %s", citem->key);
				miss = as_aaf_uint64(&centry->cache_miss, 1);
				citem->state = NULL;
			}
			uint64_t total = as_aaf_uint64(&centry->total, 1);
			centry = 0;
			as_log_trace("[CACHE] Miss %lu : Total %lu", miss, total);
		} else {
			centry = NULL;
		}
		UNLOCK;
	}
	else {
		as_log_trace("[CACHE] is disabled.");
	}

	if ( citem->state == NULL ) {
		citem->gen[0] = '\0';
		pthread_rwlock_rdlock(ctx->lock);
		citem->state = create_state(ctx, citem->key);
		pthread_rwlock_unlock(ctx->lock);
		if (!citem->state) {
			as_log_trace("[CACHE] state create failed: %s", citem->key);
			return 1;
		} else {
			as_log_trace("[CACHE] state created: %s", citem->key);
		}
	}

	return 0;
}

// Soon we will make these defaults "overridable" via config so that someone
// who actually knew what they were doing could make adjustments.
// Experimentation showed that at least 40 steps were needed to clean up after
// a simple UDF (in our system), so for us, that is the "light threshold". (tjl)
#define LUA_KMEM_GC_THRESHOLD  1024*10 // Perform GC if we've used over 10Mb
#define LUA_GC_STEP_THRESHOLD  40 // Perform 40 "iterations" of STEP GC

// NB: When it comes to Lua garbage collection, it appears to be more art than
// science.  When I fully understand what's really going on with GC, I will
// document it more clearly (and fully). (tjl)
// For now, here's the story:
// Much has already been done with Lua GC measurement, and performance measurements
// showed that a "light GC" with step data=2 provided for good performance.
// However, once a customer build a pretty heavy-duty UDF (one that iteratively
// built a list that exceeded Bin limits), the server ran out of memory and was
// then killed by the "OOM-KILLER".  Initially, changing the light "Step 2" to
// a heavy "full GC" solved the OOM problem, but ran noticeably slower
// (about 1/2 to 2/3 the speed).  So, for now, we've decided to take the "tiered
// approach", where for low thresholds we delay GC, and then when we decide to
// perform GC, we try "light GC first", then "Heavy GC" if that fails.
//
// The current thresholds are part guess and part measurement.  The most trivial
// UDF seems to use 99k of Lua space in our environment.  That's probably because
// of the automatic cost of our existing system UDFs (including LDTs).  So, we
// set the "start GC" threshold at 200k, at which point we perform "light GC".
// Our measurements showed that we needed Step Data=40 to successfully perform
// a cycle with a relatively simple UDF (read a record bin, update it and write
// it back).  For heavier UDFs the simple step GC will not complete a cycle,
// and so we then enlist the FULL GC to clean up.
//
// We've tested this approach for (sufficiently) large runs of small and large
// UDFs, and we appear to get the best possible performance coupled with the
// safety of full GC when we need it. (May 9, 2014 tjl)

/**
 * Release the context.
 *
 * @param m the module from which the context was leased from.
 * @param filename name of the udf file
 * @param l the context being released
 * @return 0 on success, otherwise 1
 */
static int offer_state(context * ctx, cache_item * citem) {
	int rc;

	if ( ctx->config.cache_enabled == true ) {
		// For Lua GC, we take the tiered approach, where if mem-used is below
		// our threshold, we don't do anything (we wait until later).  If
		// we should do "something", then we try "GC Light" first and look at
		// the result. If that didn't work (didn't complete a cycle), then we
		// call in the heavy artillery, "GC Heavy". (tjl: May 2014).
		// Note that garbage collection should be done outside the spinlock.
		int kmem_used = lua_gc(citem->state, LUA_GCCOUNT, 0);
		if ( kmem_used > LUA_KMEM_GC_THRESHOLD ) {
			// We will want some sort of counter here, but can't use a
			// g_config-related stat because g_config is not visible here.
			// This comment is a placeholder that we need to add some sort
			// of bridge beween the AS world and the mod-lua world.
			// as_incr_uint64(&g_config.stat_lua_gc_step);
			if ((rc = lua_gc(citem->state, LUA_GCSTEP, 40)) != 1) {
				lua_gc(citem->state, LUA_GCCOLLECT, 200);
				// We will want some sort of counter here, but can't use g_config
				// as_incr_uint64(&g_config.stat_lua_gc_full);
			}
		}
		// else {
		// We will want some sort of counter here, but can't use g_config
		// as_incr_uint64(&g_config.stat_lua_gc_delay);
		// }

		cache_entry *centry = NULL;
		RDLOCK;
		if (lua_hash_get(g_lua_hash, citem->key, &centry) ) {
			as_log_trace("[CACHE] found entry: %s", citem->key);
			if (( cf_queue_sz(centry->lua_state_q) < CACHE_ENTRY_STATE_MAX )
				&& ( !strncmp(centry->gen, citem->gen, CACHE_ENTRY_GEN_MAX) )) {
				cf_queue_push(centry->lua_state_q, &citem->state);
				as_log_trace("[CACHE] returning state: %s", citem->key);
				citem->state = NULL;
			}
			centry = 0;
		}
		else {
			as_log_trace("[CACHE] entry not found: %s", citem->key);
		}
		UNLOCK;
	}
	else {
		as_log_trace("[CACHE] is disabled.");
	}

	// l is not NULL
	// This means that it was not returned to the cache.
	// So, we free it up.
	if ( citem->state != NULL) {
		lua_close(citem->state);
		as_log_trace("[CACHE] state closed: %s", citem->key);
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
	as_log_trace("pushargs: %d", data.count);
	return data.count;
}

static int handle_error(lua_State * l) {
	const char * msg = luaL_optstring(l, 1, 0);
	as_log_error("Lua Runtime Error: %s", msg);
	return 1;
}


#ifndef LUA_DEBUG_HOOK
static as_timer g_timer = {
	    .is_malloc = false,
	    .source = NULL,
	    .hooks = NULL
};

/**
 * Lua debug hook to check for a timeout.
 */
static void check_timer(lua_State *L, lua_Debug *ar)
{
	as_log_trace("%s %p", __func__, &g_timer);

	if (ar->event == LUA_HOOKCOUNT) {
		if (as_timer_timedout(&g_timer)) {
			luaL_error(L, "UDF Execution Timeout");
		}
	}
}
#endif

static int apply(lua_State * l, as_udf_context *udf_ctx, int err, int argc, as_result * res, bool is_stream) {

	as_log_trace("apply");

#ifndef LUA_DEBUG_HOOK
	if ( !g_timer.hooks && udf_ctx->timer ) {
		g_timer.hooks = udf_ctx->timer->hooks;
	}

	if ( udf_ctx->timer ) {
		uint64_t slice = as_timer_timeslice(udf_ctx->timer);
		as_log_trace("setting lua_debug hook (%p), count = %lu, thread ID = %lu", &check_timer, slice, pthread_self());
		lua_sethook(l, &check_timer, LUA_MASKCOUNT, (int)slice);
	}
#endif

	// call apply_record(f, r, ...)
	as_log_trace("call function");
	int rc = lua_pcall(l, argc, 1, err);

	as_log_trace("rc = %d", rc);

	// Convert the return value from a lua type to a val type
	as_log_trace("convert lua type to val");

	if ( rc == 0 ) { // indicates lua-execution success
		if ( (is_stream == false) && (res != NULL) ) { // if is_stream is true, no need to set result as success
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

#ifndef LUA_DEBUG_HOOK
	// Disable the hook.
	if ( udf_ctx->timer ) {
		lua_sethook(l, &check_timer, 0, 0);
	}
#endif

	// Pop the return value off the stack
	as_log_trace("pop return value from the stack");
	lua_pop(l, -1);

	if ( is_stream || (res == NULL) ) { //if is_stream is true then whether res is null or not rc should be returned
		return rc;
	} else {
		return 0;
	}
}


char * as_module_err_string(int err_no) {
	char *rs;
	switch(err_no) {
		case -1:
			rs = cf_strdup("UDF: Mod-Lua system path not found");
			break;
		case -2:
			rs = cf_strdup("UDF: Mod-Lua user path not found");
			break;
		case -3:
			rs = cf_strdup("UDF: Mod-Lua system and user path not found");
			break;
		default:
			rs = cf_malloc(sizeof(char) * 128);
			sprintf(rs, "UDF: Execution Error %d", err_no);
			break;
	}
	return rs;
}

static void populate_error(lua_State * l, const char * filename, int rc, as_module_error * err) {

	const char * message = lua_tostring(l, -1);
	size_t len = 0;

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

	if ( ! message ) {
		message = "(Null error message returned by lua)";
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
							len = strlen(lineR);
							if ( len > 1024 ) {
								len = 1024;
							}
							memcpy(err->message, lineR, len);
							err->message[len] = '\0';
						}
					}
				}
			}
		}
		else {
			char * c = strstr(message, "module 'aerospike' not found");
			if ( c ) {
				strcpy(err->message, "'aerospike' lua module not found, check mod-lua system-path");
			}
			else {
				// Unrecognized error message. Just return the first line, up
				// to 256 chars.
				c = strchr(message, '\n');
				if ( c ) {
					len = c - message;
				}
				else {
					len = strlen(message);
				}
				if ( len > 256 ) {
					len = 256;
				}
				memcpy(err->message, message, len);
				err->message[len] = '\0';
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

		len = strlen(message);
		if ( len > 1024 ) {
			len = 1024;
		}
		memcpy(err->message, message, len);
		err->message[len] = '\0';
		memcpy(err->file, filename, 256);
		err->file[255] = '\0';
		err->line = ar.currentline;
		memcpy(err->func, ar.name, 256);
		err->func[255] = '\0';
	}
}

static bool
load_buffer_validate(
	lua_State* l, const char* filename, const char* script, size_t size, const char* name,
	as_module_error* err
	)
{
	int rc = luaL_loadbuffer(l, script, size - 1, name);

	if (rc) {
		populate_error(l, filename, rc, err);
		return false;
	}

	rc = lua_pcall(l, 0, LUA_MULTRET, 0);

	if (rc) {
		populate_error(l, filename, rc, err);
		return false;
	}

	return true;
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

	package_path_set(l, ctx->config.user_path);
	package_cpath_set(l, ctx->config.user_path);

	mod_lua_aerospike_register(l);
	mod_lua_record_register(l);
	mod_lua_iterator_register(l);
	mod_lua_stream_register(l);
	mod_lua_list_register(l);
	mod_lua_map_register(l);
	mod_lua_bytes_register(l);
	mod_lua_geojson_register(l);

	if (! load_buffer_validate(l, filename, as_lua_as, as_lua_as_size, "as.lua", err)) {
		goto Cleanup;
	}

	if (! load_buffer_validate(l, filename, as_lua_stream_ops, as_lua_stream_ops_size, "stream_ops.lua", err)) {
		goto Cleanup;
	}

	if (! load_buffer_validate(l, filename, as_lua_aerospike, as_lua_aerospike_size, "aerospike.lua", err)) {
		goto Cleanup;
	}

	// No validation for .so file
	if (hasext(filename, strlen(filename), ".so", 3)) {
		as_log_trace("No validation required for native module: %s", filename);
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
		as_log_trace("Lua Validation Pass for '%s'", filename);
	}
	else {
		as_log_debug("Lua Validation Fail for '%s': (%d) %s", filename, err->code, err->message);
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
 * TODO: Remove redundancies between apply_record() and apply_stream()
 *
 * @param m module from which the fqn will be resolved.
 * @param udf_ctx udf execution context
 * @param function fully-qualified name of the function to invoke.
 * @param r record to apply to the function.
 * @param args list of arguments for the function represented as vals
 * @param res pointer to an as_result that will be populated with the result.
 * @return 0 on success, otherwise 1
 */
static int apply_record(as_module * m, as_udf_context * udf_ctx, const char * filename, const char * function, as_rec * r, as_list * args, as_result * res) {

	int         rc      = 0;
	context *   ctx     = (context *) m->source;    // mod-lua context
	lua_State * l       = (lua_State *) NULL;       // Lua State
	int         argc    = 0;                        // Number of arguments pushed onto the stack
	int         err     = 0;                        // Error handler
	as_aerospike *as    = udf_ctx->as;              // aerospike object

	cache_item  citem   = {
		.key    = "",
		.gen    = "",
		.state  = NULL
	};

	strncpy(citem.key, filename, CACHE_ENTRY_KEY_MAX - 1);

	as_log_trace("apply_record: BEGIN");

	// lease a state
	as_log_trace("apply_record: poll state");
	rc = poll_state(ctx, &citem);

	if ( rc != 0 ) {
		as_log_trace("apply_record: Unable to poll a state");
		return rc;
	}

	l = citem.state;

	// push error handler
	// lua_pushcfunction(l, handle_error);
	// int err = lua_gettop(l);

	// push aerospike into the global scope
	as_log_trace("apply_record: push aerospike into the global scope");
	mod_lua_pushaerospike(l, as);
	lua_setglobal(l, "aerospike");

	// push apply_record() onto the stack
	as_log_trace("apply_record: push apply_record() onto the stack");
	lua_getglobal(l, "apply_record");

	// push function onto the stack
	as_log_trace("apply_record: push function onto the stack");
	lua_getglobal(l, function);

	// push the record onto the stack
	as_log_trace("apply_record: push the record onto the stack");
	mod_lua_pushrecord(l, r);

	// push each argument onto the stack
	as_log_trace("apply_record: push each argument onto the stack");
	argc = pushargs(l, args);

	if (argc > LUA_PARAM_COUNT_THRESHOLD) {
		as_log_error("large number of Lua function arguments (%d)", argc);
	}

	// function + record + arglist
	argc = argc + 2;

	// apply the function
	as_log_trace("apply_record: apply the function %s.%s", filename, function);
	rc = apply(l, udf_ctx, err, argc, res, false);

	// return the state
	pthread_rwlock_rdlock(ctx->lock);
	as_log_trace("apply_record: offer state");
	offer_state(ctx, &citem);
	pthread_rwlock_unlock(ctx->lock);

	as_log_trace("apply_record: END");
	return rc;
}



/**
 * Applies function to a stream and set of arguments.
 *
 * Proxies to `m->hooks->apply_stream(m, ...)`
 *
 * TODO: Remove redundancies between apply_record() and apply_stream()
 *
 * @param m module from which the fqn will be resolved.
 * @param udf_ctx udf execution context
 * @param function fully-qualified name of the function to invoke.
 * @param istream stream to apply to the function.
 * @param args list of arguments for the function represented as vals
 * @param ostream output stream which will be populated by applying the function.
 * @param res pointer to an as_result that will be populated with the result.
 * @return 0 on success, otherwise 1
 */
static int apply_stream(as_module * m, as_udf_context *udf_ctx, const char * filename, const char * function, as_stream * istream, as_list * args, as_stream * ostream, as_result * res) {

	int         rc      = 0;
	context *   ctx     = (context *) m->source;    // mod-lua context
	lua_State * l       = (lua_State *) NULL;   // Lua State
	int         argc    = 0;                    // Number of arguments pushed onto the stack
	int         err     = 0;                    // Error handler
	as_aerospike *as    = udf_ctx->as;          // aerospike object

	cache_item  citem   = {
		.key    = "",
		.gen    = "",
		.state  = NULL
	};

	strncpy(citem.key, filename, CACHE_ENTRY_KEY_MAX - 1);

	as_log_trace("apply_stream: BEGIN");

	// lease a state
	as_log_trace("apply_stream: poll state");
	rc = poll_state(ctx, &citem);

	if ( rc != 0 ) {
		as_log_trace("apply_stream: Unable to poll a state");
		return rc;
	}

	l = citem.state;

	// push error handler
	lua_pushcfunction(l, handle_error);
	err = lua_gettop(l);

	// push aerospike into the global scope
	as_log_trace("apply_stream: push aerospike into the global scope");
	mod_lua_pushaerospike(l, as);
	lua_setglobal(l, "aerospike");

	// push apply_stream() onto the stack
	as_log_trace("apply_stream: push apply_stream() onto the stack");
	lua_getglobal(l, "apply_stream");

	// push function onto the stack
	as_log_trace("apply_stream: push function onto the stack");
	lua_getglobal(l, function);

	// push the stream onto the stack
	// if server_mode == true then SCOPE_SERVER(1) else SCOPE_CLIENT(2)
	as_log_trace("apply_stream: push scope onto the stack");
	lua_pushinteger(l, ctx->config.server_mode ? 1 : 2);

	// push the stream onto the stack
	as_log_trace("apply_stream: push istream onto the stack");
	mod_lua_pushstream(l, istream);

	as_log_trace("apply_stream: push ostream onto the stack");
	mod_lua_pushstream(l, ostream);

	// push each argument onto the stack
	as_log_trace("apply_stream: push each argument onto the stack");
	argc = pushargs(l, args);

	if (argc > LUA_PARAM_COUNT_THRESHOLD) {
		as_log_error("large number of Lua function arguments (%d)", argc);
	}

	// function + scope + istream + ostream + arglist
	argc = 4 + argc;

	// call apply_stream(f, s, ...)
	as_log_trace("apply_stream: apply the function %s.%s", filename, function);
	rc = apply(l, udf_ctx, err, argc, res, true);

	// release the context
	pthread_rwlock_rdlock(ctx->lock);
	as_log_trace("apply_stream: lose the context");
	offer_state(ctx, &citem);
	pthread_rwlock_unlock(ctx->lock);

	as_log_trace("apply_stream: END");
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
	.hooks          = &mod_lua_hooks
};

//==========================================================
// Simple hashmap for lua configuration.
// - keys are null terminated but fixed-size allocated
// - key parameters are assumed to be good
// - values are lua cache_entry struct pointers
//

static lua_hash_ele*
lua_hash_get_row_head(const lua_hash* h, const char* key)
{
	uint64_t hashed_key = cf_hash_fnv32((const uint8_t*)key, strlen(key));
	uint32_t row_i = (uint32_t)(hashed_key % h->n_rows);

	lua_hash_ele* e = (lua_hash_ele*)(h->table + (h->ele_size * row_i));

	return e;
}

lua_hash*
lua_hash_create(uint32_t key_size, uint32_t n_rows)
{
	lua_hash* h = (lua_hash*)cf_malloc(sizeof(lua_hash));

	h->ele_size = sizeof(lua_hash_ele) + key_size;
	h->n_rows = n_rows;

	size_t table_size = n_rows * h->ele_size;

	h->table = cf_malloc(table_size);

	memset((void*)h->table, 0, table_size);

	return h;
}

cache_entry*
lua_hash_remove(lua_hash* h, const char* key)
{
	lua_hash_ele* e = lua_hash_get_row_head(h, key);

	// Nobody in row yet so nothing to delete.
	if (e->value == NULL) {
		return NULL;
	}

	lua_hash_ele* e_head = e;
	lua_hash_ele* e_last = NULL;

	while (e != NULL) {
		if (strcmp(e->key, key) == 0) {
			cache_entry* ele_to_remove_val = e->value;

			// Special cases for removing first element in a row.
			if (e == e_head) {
				if (e->next) { // move the next element up to the head
					lua_hash_ele* e_next = e->next;
					e->next = e_next->next;
					e->value = e_next->value;
					strcpy(e->key, e_next->key);
					cf_free(e_next);
				}
				else { // remove only element in row
					e->next = NULL;
					e->value = NULL;
					e->key[0] = '\0';
				}
			}
			else {
				e_last->next = e->next;
				cf_free(e);
			}

			return ele_to_remove_val;
		}

		e_last = e;
		e = e->next;
	}

	return NULL;
}

static inline void
lua_hash_call_cb_if(void (*cb)(cache_entry*), cache_entry* centry)
{
	if (cb != NULL && centry != NULL) {
		(*cb)(centry);
	}
}

// Wipe out all entries but leave hash itself intact. This function cleans up
// the hash itself. The callback may be used to do any additional cleanup on the
// hash values.
void
lua_hash_clear(lua_hash* h, void (*cb)(cache_entry*))
{
	lua_hash_ele* e_table = (lua_hash_ele*)h->table;

	for (uint32_t i = 0; i < h->n_rows; i++) {
		lua_hash_call_cb_if(cb, e_table->value);

		if (e_table->next != NULL) {
			lua_hash_ele* e = e_table->next;

			while (e != NULL) {
				lua_hash_call_cb_if(cb, e->value);

				lua_hash_ele* t = e->next;
				cf_free(e);
				e = t;
			}
		}

		e_table->next = NULL;
		e_table->value = NULL;
		e_table->key[0] = '\0';
		e_table = (lua_hash_ele*)((uint8_t*)e_table + h->ele_size);
	}
}

// Wipe out entire hash (hash not usable after calling).
void
lua_hash_destroy(lua_hash* h)
{
	lua_hash_clear(h, NULL);
	cf_free(h->table);
	cf_free(h);
}

// Returns old cache_entry if key had value before NULL otherwise.
cache_entry*
lua_hash_put(lua_hash* h, const char* key, cache_entry* value)
{
	lua_hash_ele* e = lua_hash_get_row_head(h, key);

	// Nobody in row yet so just set that first element
	if (e->value == NULL) {
		strcpy(e->key, key);
		e->value = value;
		return NULL;
	}

	lua_hash_ele* e_head = e;
	cache_entry* overwritten_value = NULL;

	while (e) {
		if (strcmp(e->key, key) == 0) {
			overwritten_value = e->value;
			break;
		}

		e = e->next;
	}

	if (overwritten_value == NULL) {
		e = (lua_hash_ele*)cf_malloc(h->ele_size);
		strcpy(e->key, key);
		e->next = e_head->next;
		e_head->next = e;
	}

	e->value = value;

	return overwritten_value;
}

// Functions as a "has" if called with a null p_value.
bool
lua_hash_get(const lua_hash* h, const char* key, cache_entry** p_value)
{
	lua_hash_ele* e = lua_hash_get_row_head(h, key);

	if (e->value == NULL) {
		return false;
	}

	while (e) {
		if (strcmp(e->key, key) == 0) {
			if (p_value) {
				*p_value = e->value;
			}

			return true;
		}

		e = e->next;
	}

	return false;
}
