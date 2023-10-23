/*
 * Copyright 2008-2022 Aerospike, Inc.
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


//==========================================================
// Includes.
//

#include "aerospike/mod_lua.h"

#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>
#include <pthread.h>
#include <setjmp.h> // needed for gracefully handling lua panics
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <aerospike/as_aerospike.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_dir.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_types.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_hash_math.h>
#include <citrusleaf/cf_queue.h>

#include "aerospike/mod_lua_aerospike.h"
#include "aerospike/mod_lua_bytes.h"
#include "aerospike/mod_lua_config.h"
#include "aerospike/mod_lua_geojson.h"
#include "aerospike/mod_lua_iterator.h"
#include "aerospike/mod_lua_list.h"
#include "aerospike/mod_lua_map.h"
#include "aerospike/mod_lua_record.h"
#include "aerospike/mod_lua_stream.h"
#include "aerospike/mod_lua_val.h"

#include "internal.h"

// In mod_lua_system.c, there's no .h.

extern const char as_lua_as[];
extern const char as_lua_stream_ops[];
extern const char as_lua_aerospike[];

extern size_t as_lua_as_size;
extern size_t as_lua_stream_ops_size;
extern size_t as_lua_aerospike_size;


//==========================================================
// Typedefs & constants.
//

#define CACHE_ENTRY_KEY_MAX 128
#define CACHE_ENTRY_STATE_MAX 128
#define CACHE_ENTRY_STATE_MIN 10

#define LUA_PARAM_COUNT_THRESHOLD 20 // warn if a function call exceeds this

#define MOD_LUA_CONFIG_USRPATH "/opt/aerospike/usr/udf/lua"

typedef struct cache_entry_s {
	uint64_t cache_miss;
	uint64_t total;
	uint32_t id;
	cf_queue* lua_state_q;
} cache_entry;

typedef struct cache_item_s {
	uint32_t id;
	lua_State* state;
} cache_item;

typedef struct pushargs_data_s {
	lua_State* l;
	uint32_t count;
} pushargs_data;

typedef struct lua_hash_ele_s {
	char key[CACHE_ENTRY_KEY_MAX];
	cache_entry* value;
	struct lua_hash_ele_s* next;
} lua_hash_ele;

typedef struct lua_hash_s {
	uint32_t n_rows;
	lua_hash_ele* table;
} lua_hash;


//==========================================================
// Globals.
//

static uint32_t g_id = 0;

static pthread_rwlock_t g_lock =
#if defined(__USE_UNIX98) || defined (__USE_XOPEN2K)
		PTHREAD_RWLOCK_WRITER_NONRECURSIVE_INITIALIZER_NP;
#else
		PTHREAD_RWLOCK_INITIALIZER;
#endif

static lua_hash* g_lua_hash = NULL;
static pthread_rwlock_t g_cache_lock = PTHREAD_RWLOCK_INITIALIZER;

// Lua module specific configuration.
static mod_lua_config g_lua_cfg = {
		.server_mode = true,
		.cache_enabled = true,
		.user_path = MOD_LUA_CONFIG_USRPATH
};

static as_timer g_timer = { 0 };


//==========================================================
// Forward declarations.
//

static int update(as_module* m, as_module_event* e);
static int validate(as_module* m, as_aerospike* as, const char* filename, const char* content, uint32_t size, as_module_error* err);
static int apply_record(as_module* m, as_udf_context* udf_ctx, const char* filename, const char* function, as_rec* r, as_list* args, as_result* res);
static int apply_stream(as_module* m, as_udf_context* udf_ctx, const char* filename, const char* function, as_stream* istream, as_list* args, as_stream* ostream, as_result* res);

static int cache_scan_dir(const char* user_path);
static int cache_add_file(const char* user_path, const char* filename);
static int cache_remove_file(const char* filename);
static void cache_init(const char* user_path, const char* key);
static void cache_rm(const char* key);
static void destroy_cache_entry(cache_entry* centry);

static void package_path_set(lua_State* l, const char* user_path);
static void package_cpath_set(lua_State* l, const char* user_path);
static bool load_buffer_validate(lua_State* l, const char* filename, const char* script, size_t size, const char* name, as_module_error* err);
static void populate_error(lua_State* l, const char* filename, int rc, as_module_error* err);

static int get_state(const char* filename, cache_item* citem);
static int pushargs(lua_State* l, as_list* args);
static bool pushargs_foreach(as_val* val, void* context);
static int apply(lua_State* l, as_udf_context* udf_ctx, int err, int argc, as_result* res, bool is_stream);
static void release_state(const char* filename, cache_item* citem);
static lua_State* create_state(const char* user_path, const char* filename);
static bool load_buffer(lua_State* l, const char* script, size_t size, const char* name);
static bool is_native_module(const char* user_path, const char* filename);

static int handle_error(lua_State* l);
static void check_timer(lua_State* l, lua_Debug* ar);

lua_hash* lua_hash_create(uint32_t n_rows);
void lua_hash_destroy(lua_hash* h); // for unit test only
cache_entry* lua_hash_put(lua_hash* h, const char* key, cache_entry* value);
bool lua_hash_get(const lua_hash* h, const char* key, cache_entry** p_value);
cache_entry* lua_hash_remove(lua_hash* h, const char* key);
void lua_hash_clear(lua_hash* h, void (*cb)(cache_entry*));


//==========================================================
// Inlines & macros.
//

static inline bool
hasext(const char* name, size_t name_len, const char* ext, size_t ext_len)
{
	const char* p = name + name_len - ext_len;

	return ext_len < name_len && strncmp(p, ext, ext_len) == 0;
}

static inline bool
dropext(char* name, size_t name_len, const char* ext, size_t ext_len)
{
	char* p = name + name_len - ext_len;

	if (ext_len < name_len && strncmp(p, ext, ext_len) == 0) {
		*p = '\0';
		return true;
	}

	return false;
}

static inline void
cache_entry_cleanup(cache_entry* centry)
{
	lua_State* l;

	while(cf_queue_pop(centry->lua_state_q, &l, CF_QUEUE_NOWAIT) ==
			CF_QUEUE_OK) {
		lua_close(l);
	}
}

static inline void
cache_entry_populate(cache_entry* centry, const char* user_path,
		const char* filename)
{
	for (int i = 0; i < CACHE_ENTRY_STATE_MIN; i++) {
		lua_State* l = create_state(user_path, filename);

		if (l != NULL) {
			cf_queue_push(centry->lua_state_q, &l);
		}
	}
}

static inline void
cache_entry_init(cache_entry* centry, const char* user_path,
		const char* filename)
{
	centry->id = as_aaf_uint32(&g_id, 1);
	cache_entry_cleanup(centry);
	cache_entry_populate(centry, user_path, filename);
}


//==========================================================
// Public API - somehow, not hooks.
//

// Called by server only (udf_cask).
void
mod_lua_rdlock(as_module* m)
{
	(void)m;
	pthread_rwlock_rdlock(&g_lock);
}

// Called by server only (udf_cask).
void
mod_lua_wrlock(as_module* m)
{
	(void)m;
	pthread_rwlock_wrlock(&g_lock);
}

// Called by server only (udf_cask).
void
mod_lua_unlock(as_module* m)
{
	(void)m;
	pthread_rwlock_unlock(&g_lock);
}

char*
as_module_err_string(int err_no)
{
	char* rs;

	switch (err_no) {
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


//==========================================================
// Public API - hooks.
//

static const as_module_hooks mod_lua_hooks = {
		.destroy        = NULL,
		.update         = update,
		.validate       = validate,
		.apply_record   = apply_record,
		.apply_stream   = apply_stream
};

// 'extern' of this global in header file makes this a public API.
as_module mod_lua = {
		.source         = NULL, // context is all global
		.hooks          = &mod_lua_hooks
};


//==========================================================
// Public API - implementation of hooks.
//

static int
update(as_module* m, as_module_event* e)
{
	(void)m;

	switch (e->type) {
	// Server and client, only at startup:
	case AS_MODULE_EVENT_CONFIGURE: {
		mod_lua_config* config = (mod_lua_config*)e->data.config;

		g_lua_cfg.server_mode = config->server_mode;
		g_lua_cfg.cache_enabled = config->cache_enabled;

		if (g_lua_hash == NULL && g_lua_cfg.cache_enabled) {
			g_lua_hash = lua_hash_create(64);
		}

		if (config->user_path[0] != '\0') {
			// Attempt to open directory. If it opens, set the cfg value.
			// Otherwise, empty the path: either UDFs won't be found and will
			// fail when attempted, or may be found elsewhere by Lua.
			if (as_dir_exists(config->user_path)) {
				strcpy(g_lua_cfg.user_path, config->user_path);
			}
			else {
				g_lua_cfg.user_path[0] = '\0';
			}
		}

		// For the client. Pointless when the server calls, since immediately
		// after this, we clear the Lua directory and then re-add everything we
		// get from SMD, including clearing & re-populating cache.
		// TODO - remove this, and have client call FILE_SCAN after CONFIGURE?
		if (g_lua_cfg.cache_enabled) {
			// Set up the USER path.
			cache_scan_dir(g_lua_cfg.user_path);
		}

		break;
	}
	// For now, nobody uses this:
	case AS_MODULE_EVENT_FILE_SCAN:
		if (g_lua_cfg.cache_enabled &&
				// Set up the USER path.
				cache_scan_dir(g_lua_cfg.user_path) != 0) {
			return 3;
		}
		break;
	// Server only, after startup, caller should have a write g_lock:
	case AS_MODULE_EVENT_FILE_ADD:
		if (g_lua_cfg.cache_enabled &&
				cache_add_file(g_lua_cfg.user_path, e->data.filename) != 0) {
			return 2;
		}
		break;
	// Server only, after startup, caller should have a write g_lock:
	case AS_MODULE_EVENT_FILE_REMOVE:
		if (g_lua_cfg.cache_enabled &&
				cache_remove_file(e->data.filename) != 0) {
			return 2;
		}
		break;
	// Server only, after startup, caller should have a write g_lock:
	// TODO - does this really need to be under g_lock?
	case AS_MODULE_EVENT_CLEAR_CACHE:
		if (g_lua_cfg.cache_enabled) {
			pthread_rwlock_wrlock(&g_cache_lock);
			lua_hash_clear(g_lua_hash, &destroy_cache_entry);
			pthread_rwlock_unlock(&g_cache_lock);
		}
		break;
	default:
		as_log_error("bad module update event %d", e->type);
		return 1;
	}

	return 0;
}

static int
validate(as_module* m, as_aerospike* as, const char* filename,
		const char* content, uint32_t size, as_module_error* err)
{
	(void)m;
	(void)as;

	err->scope = 0;
	err->code = 0;
	err->message[0] = '\0';
	err->file[0] = '\0';
	err->line = 0;
	err->func[0] = '\0';

	lua_State* l = luaL_newstate();

	if (l == NULL) {
		err->scope = 1;
		err->code = 1;
		strcpy(err->message, "Unable to create a new Lua state");
		goto Cleanup;
	}

	luaL_openlibs(l);

	package_path_set(l, g_lua_cfg.user_path);
	package_cpath_set(l, g_lua_cfg.user_path);

	mod_lua_aerospike_register(l);
	mod_lua_record_register(l);
	mod_lua_iterator_register(l);
	mod_lua_stream_register(l);
	mod_lua_list_register(l);
	mod_lua_map_register(l);
	mod_lua_bytes_register(l);
	mod_lua_geojson_register(l);

	if (! load_buffer_validate(l, filename, as_lua_as, as_lua_as_size, "as.lua",
			err)) {
		goto Cleanup;
	}

	if (! load_buffer_validate(l, filename, as_lua_stream_ops,
			as_lua_stream_ops_size, "stream_ops.lua", err)) {
		goto Cleanup;
	}

	if (! load_buffer_validate(l, filename, as_lua_aerospike,
			as_lua_aerospike_size, "aerospike.lua", err)) {
		goto Cleanup;
	}

	// No validation for .so file.
	if (hasext(filename, strlen(filename), ".so", 3)) {
		goto Cleanup;
	}

	int rc = luaL_loadbuffer(l, content, size, filename);

	if (rc != 0) {
		populate_error(l, filename, rc, err);
		goto Cleanup;
	}

	rc = lua_pcall(l, 0, 1, 0);

	if (rc != 0) {
		populate_error(l, filename, rc, err);
		goto Cleanup;
	}

Cleanup:

	if (l != NULL) {
		lua_close(l);
	}

	return err->code;
}

// Apply a record UDF. Relevant only to server.
static int
apply_record(as_module* m, as_udf_context* udf_ctx, const char* filename,
		const char* function, as_rec* r, as_list* args, as_result* res)
{
	(void)m;

	cache_item citem = { 0 };

	// Get a state.
	int rc = get_state(filename, &citem);

	if (rc != 0) {
		return rc;
	}

	lua_State* l = citem.state;

	// Push error handler.
	// lua_pushcfunction(l, handle_error);

	int err = 0; // lua_gettop(l);

	// Push as_aerospike object into the global scope.
	mod_lua_pushaerospike(l, udf_ctx->as);
	lua_setglobal(l, "aerospike");

	// Push apply_record() onto the stack.
	lua_getglobal(l, "apply_record");

	// Push function onto the stack.
	lua_getglobal(l, function);

	// Push the record onto the stack.
	mod_lua_pushrecord(l, r);

	// Push each argument onto the stack.
	int argc = pushargs(l, args);

	if (argc < 0) {
		release_state(filename, &citem);
		return 2;
	}

	if (argc > LUA_PARAM_COUNT_THRESHOLD) {
		as_log_error("large number of lua function arguments (%d)", argc);
	}

	argc = argc + 2; // function + record + arglist

	// Apply the function.
	apply(l, udf_ctx, err, argc, res, false); // here, return value is always 0

	// Release the state.
	release_state(filename, &citem);

	return 0;
}

// Apply a stream UDF. Relevant to client and server.
static int
apply_stream(as_module* m, as_udf_context* udf_ctx, const char* filename,
		const char* function, as_stream* istream, as_list* args,
		as_stream* ostream, as_result* res)
{
	(void)m;

	cache_item citem = { 0 };

	// Get a state.
	int rc = get_state(filename, &citem);

	if (rc != 0) {
		return rc;
	}

	lua_State* l = citem.state;

	// Push error handler.
	lua_pushcfunction(l, handle_error);

	int err = lua_gettop(l);

	// Push as_aerospike object into the global scope.
	mod_lua_pushaerospike(l, udf_ctx->as);
	lua_setglobal(l, "aerospike");

	// Push apply_stream() onto the stack.
	lua_getglobal(l, "apply_stream");

	// Push function onto the stack.
	lua_getglobal(l, function);

	// Push the scope onto the stack.
	lua_pushinteger(l, g_lua_cfg.server_mode ? 1 : 2);

	// Push the istream onto the stack.
	mod_lua_pushstream(l, istream);

	// Push the ostream onto the stack.
	mod_lua_pushstream(l, ostream);

	// Push each argument onto the stack.
	int argc = pushargs(l, args);

	if (argc < 0) {
		release_state(filename, &citem);
		return 2;
	}

	if (argc > LUA_PARAM_COUNT_THRESHOLD) {
		as_log_error("large number of lua function arguments (%d)", argc);
	}

	argc = 4 + argc; // function + scope + istream + ostream + arglist

	// Call apply_stream(f, s, ...).
	rc = apply(l, udf_ctx, err, argc, res, true);

	// Release the state.
	release_state(filename, &citem);

	return rc;
}


//==========================================================
// Local helpers - cache related.
//

static int
cache_scan_dir(const char* user_path)
{
	as_dir dir;

	if (! as_dir_open(&dir, user_path)) {
		return -1;
	}

	const char* entry;

	while ((entry = as_dir_read(&dir)) != NULL) {
		char key[CACHE_ENTRY_KEY_MAX];

		if (as_strncpy(key, entry, sizeof(key))) {
			as_log_error("lua dir scan: filename too long %s", entry);
			continue;
		}

		size_t len = strlen(key);

		// TODO - if we have both "foo.lua" and "foo.so", it's not clear if/how
		// both can be used, since the cache key ignores extension. So, what?

		// If file ends with ".lua", then drop ".lua".
		if (dropext(key, len, ".lua", 4)) {
			cache_init(user_path, key);
		}
		// If file ends with ".so", then drop ".so"
		else if (dropext(key, len, ".so", 3)) {
			cache_init(user_path, key);
		}
	}

	as_dir_close(&dir);

	return 0;
}

static int
cache_add_file(const char* user_path, const char* filename)
{
	char key[CACHE_ENTRY_KEY_MAX];

	if (as_strncpy(key, filename, sizeof(key))) {
		as_log_error("lua registration: filename too long %s...", key);
		return -1;
	}

	char* p = strrchr(key, '.');

	// TODO - the criteria for extensions here is just: at least one character
	// for name and two characters for extension, like "n.xx". Shouldn't this
	// only let in .lua and .so like for FILE_SCAN? Seems no other files could
	// get used.

	// No extension, '.' is first, '.' is last or 1-char extension.
	if (p == NULL || p == key || strlen(p) <= 1) {
		as_log_error("lua registration: invalid filename %s", key);
		return -1;
	}

	*p = '\0';

	cache_init(user_path, key);

	return 0;
}

static int
cache_remove_file(const char* filename)
{
	char key[CACHE_ENTRY_KEY_MAX];

	if (as_strncpy(key, filename, sizeof(key))) {
		as_log_error("lua cache remove: filename too long %s...", key);
		return -1;
	}

	char* p = strrchr(key, '.');

	if (p != NULL) {
		*p = '\0';
	}

	cache_rm(key);

	return 0;
}

static void
cache_init(const char* user_path, const char* key)
{
	pthread_rwlock_wrlock(&g_cache_lock);

	cache_entry* centry;

	if (lua_hash_get(g_lua_hash, key, &centry)) {
		pthread_rwlock_unlock(&g_cache_lock);

		cache_entry_init(centry, user_path, key);
	}
	else {
		centry = cf_malloc(sizeof(cache_entry));

		*centry = (cache_entry){
				.lua_state_q = cf_queue_create(sizeof(lua_State*), true)
		};

		cache_entry_init(centry, user_path, key);

		lua_hash_put(g_lua_hash, key, centry);

		as_log_trace("[CACHE] added [%s:%p]", key, centry);

		pthread_rwlock_unlock(&g_cache_lock);
	}
}

static void
cache_rm(const char* key)
{
	if (key == NULL || *key == '\0') {
		return;
	}

	pthread_rwlock_wrlock(&g_cache_lock);

	cache_entry* centry = lua_hash_remove(g_lua_hash, key);

	pthread_rwlock_unlock(&g_cache_lock);

	if (centry != NULL) {
		destroy_cache_entry(centry);
	}
}

static void
destroy_cache_entry(cache_entry* centry)
{
	cache_entry_cleanup(centry);
	cf_queue_destroy(centry->lua_state_q);
	cf_free(centry);
}


//==========================================================
// Local helpers - validation.
//

static void
package_path_set(lua_State* l, const char* user_path)
{
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

static void
package_cpath_set(lua_State* l, const char* user_path)
{
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

static bool
load_buffer_validate(lua_State* l, const char* filename, const char* script,
		size_t size, const char* name, as_module_error* err)
{
	int rc = luaL_loadbuffer(l, script, size - 1, name);

	if (rc != 0) {
		populate_error(l, filename, rc, err);
		return false;
	}

	rc = lua_pcall(l, 0, LUA_MULTRET, 0);

	if (rc != 0) {
		populate_error(l, filename, rc, err);
		return false;
	}

	return true;
}

static void
populate_error(lua_State* l, const char* filename, int rc, as_module_error* err)
{
	err->scope = 2; // UDF module

	switch (rc) {
	case LUA_ERRSYNTAX:
		err->code = 10;
		break;
	case LUA_ERRRUN:
		err->code = 11;
		break;
	case LUA_ERRMEM:
		err->code = 12;
		break;
	case LUA_ERRERR:
		err->code = 13;
		break;
	default:
		err->code = 0;
		break;
	}

	const char* message = lua_tostring(l, -1);

	if (message == NULL) {
		message = "(Null error message returned by lua)";
	}

	size_t len = 0;

	if (err->code == 10 || err->code == 11) {
		if (message[0] == '[') {
			char* fileL = strchr(message,'"');

			if (fileL != NULL) {
				fileL++;

				char* fileR = strchr(fileL, '"');

				if (fileR != NULL) {
					memcpy(err->file, fileL, fileR - fileL);
					err->file[fileR - fileL] = '\0';

					char* lineL = strchr(fileR, ':');

					if (lineL != NULL) {
						lineL++;

						char* lineR = strchr(lineL, ':');

						if (lineR != NULL) {
							char line[11] = { 0 };

							memcpy(line, lineL, lineR - lineL);
							err->line = atoi(line);
							as_strncpy(err->message, lineR + 2,
									sizeof(err->message));
						}
					}
				}
			}
		}
		else {
			char* c = strstr(message, "module 'aerospike' not found");

			if (c != NULL) {
				strcpy(err->message, "'aerospike' lua module not found, check mod-lua system-path");
			}
			else {
				// Unrecognized message. Return first line, up to 256 chars.
				c = strchr(message, '\n');

				if (c != NULL) {
					len = c - message;
				}
				else {
					len = strlen(message);
				}

				if (len > 256) {
					len = 256;
				}

				memcpy(err->message, message, len);
				err->message[len] = '\0';
			}
		}
	}
	else {
		lua_Debug ar;
		lua_rawgeti(l, LUA_REGISTRYINDEX, LUA_RIDX_GLOBALS);
		lua_getinfo(l, ">Snl", &ar);

		// TODO - really? This goes to stdout?
		printf("## name = %s\n", ar.name);
		printf("## namewhat = %s\n", ar.namewhat);
		printf("## what = %s\n", ar.what);
		printf("## source = %s\n", ar.source);
		printf("## currentline = %d\n", ar.currentline);
		printf("## nups = %d\n", ar.nups);
		printf("## linedefined = %d\n", ar.linedefined);
		printf("## lastlinedefined = %d\n", ar.lastlinedefined);
		printf("## short_src = %s\n", ar.short_src);

		as_strncpy(err->message, message, sizeof(err->message));
		strcpy(err->file, filename);
		err->line = ar.currentline;
		as_strncpy(err->func, ar.name, sizeof(err->func));
	}
}


//==========================================================
// Local helpers - apply record or stream UDF.
//

// Get a lua_State. Re-uses an existing state if possible.
static int
get_state(const char* filename, cache_item* citem)
{
	if (g_lua_cfg.cache_enabled) {
		pthread_rwlock_rdlock(&g_cache_lock);

		cache_entry* centry;

		if (lua_hash_get(g_lua_hash, filename, &centry)) {
			uint64_t miss;

			citem->id = centry->id;

			if (cf_queue_pop(centry->lua_state_q, &citem->state,
					CF_QUEUE_NOWAIT) != CF_QUEUE_EMPTY) {
				as_log_trace("[CACHE] took state (id %u): %s", citem->id,
						filename);

				miss = centry->cache_miss;
			}
			else {
				as_log_trace("[CACHE] miss state (id %u): %s", citem->id,
						filename);

				miss = as_aaf_uint64(&centry->cache_miss, 1);
			}

			uint64_t total = as_aaf_uint64(&centry->total, 1);

			as_log_debug("[CACHE] miss %lu : total %lu", miss, total);
		}
		else {
			as_log_trace("[CACHE] not found: %s", filename);
		}

		pthread_rwlock_unlock(&g_cache_lock);
	}

	if (citem->state == NULL) {
		pthread_rwlock_rdlock(&g_lock);
		citem->state = create_state(g_lua_cfg.user_path, filename);
		pthread_rwlock_unlock(&g_lock);

		if (citem->state == NULL) {
			as_log_trace("[CACHE] state create failed: %s", filename);
			return 1;
		}

		as_log_trace("[CACHE] state created (id %u): %s", citem->id, filename);
	}

	return 0;
}

// Pushes arguments from a list on to the stack.
static int
pushargs(lua_State* l, as_list* args)
{
	// Grow the stack if necessary. (Return value 0 is a failure.)
	if (lua_checkstack(l, as_list_size(args) + LUA_MINSTACK) == 0) {
		as_log_error("failed to push %u lua args", as_list_size(args));
		return -1;
	}

	pushargs_data data = { .l = l };

	as_list_foreach(args, pushargs_foreach, &data);

	return (int)data.count;
}

// Pushes arguments into the Lua stack. We scope the arguments to Lua, so Lua is
// responsible for releasing them.
static bool
pushargs_foreach(as_val* val, void* context)
{
	pushargs_data* data = (pushargs_data*)context;

	data->count += mod_lua_pushval(data->l, val);

	return true;
}

static int
apply(lua_State* l, as_udf_context* udf_ctx, int err, int argc, as_result* res,
		bool is_stream)
{
	if (udf_ctx->timer != NULL) {
		// Lazily set timer hooks (which never change) on first ever UDF call.
		if (g_timer.hooks == NULL) {
			g_timer.hooks = udf_ctx->timer->hooks;
		}

		lua_sethook(l, &check_timer, LUA_MASKCOUNT,
				(int)as_timer_timeslice(udf_ctx->timer));
	}

	// Call the lua function.
	int rc = lua_pcall(l, argc, 1, err);

	// Convert the return value from a lua type to an as_val type.

	if (res != NULL) {
		if (is_stream) {
			if (rc != 0) {
				as_val* rv = mod_lua_retval(l);

				as_result_setfailure(res, rv);
			}
			// else - don't set success result.

			// Stream UDF always returns original rc.
		}
		else { // record UDF, server only
			as_val* rv = mod_lua_retval(l);

			if (rc == 0) {
				as_result_setsuccess(res, rv);
			}
			else {
				as_result_setfailure(res, rv);
				rc = 0; // record UDFs just rely on as_result
			}
		}
	}
	// else - return original rc. (Note - record UDFs never have NULL res.)

	// Disable the hook.
	if (udf_ctx->timer != NULL) {
		lua_sethook(l, &check_timer, 0, 0);
	}

	// Pop the return value off the stack.
	lua_pop(l, -1);

	return rc;
}

static void
release_state(const char* filename, cache_item* citem)
{
	pthread_rwlock_rdlock(&g_lock);

	if (g_lua_cfg.cache_enabled) {
		pthread_rwlock_rdlock(&g_cache_lock);

		cache_entry* centry;

		if (lua_hash_get(g_lua_hash, filename, &centry)) {
			if (centry->id == citem->id) {
				if (cf_queue_sz(centry->lua_state_q) < CACHE_ENTRY_STATE_MAX) {
					as_log_trace("[CACHE] re-caching state (id %u): %s",
							citem->id, filename);

					cf_queue_push(centry->lua_state_q, &citem->state);
					citem->state = NULL;
				}
				else {
					as_log_trace("[CACHE] excess state (id %u): %s", citem->id,
							filename);
				}
			}
			else {
				as_log_trace("[CACHE] stale state (id %u cached id %u): %s",
						citem->id, centry->id, filename);
			}
		}
		else {
			as_log_trace("[CACHE] not found: %s", filename);
		}

		pthread_rwlock_unlock(&g_cache_lock);
	}

	// l not NULL - it was not returned to the cache, free it.
	if (citem->state != NULL) {
		lua_close(citem->state);
		as_log_trace("[CACHE] state closed (id %u): %s", citem->id, filename);
	}

	pthread_rwlock_unlock(&g_lock);
}

// Creates a new context (lua_State) and populates it with default values.
static lua_State*
create_state(const char* user_path, const char* filename)
{
	lua_State* l = luaL_newstate();

	luaL_openlibs(l);

	lua_gc(l, LUA_GCGEN, 0, 0);

	package_path_set(l, user_path);
	package_cpath_set(l, user_path);

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

	if (! load_buffer(l, as_lua_stream_ops, as_lua_stream_ops_size,
			"stream_ops.lua")) {
		return NULL;
	}

	if (! load_buffer(l, as_lua_aerospike, as_lua_aerospike_size,
			"aerospike.lua")) {
		return NULL;
	}

	if (is_native_module(user_path, filename)) {
		return l;
	}

	lua_getglobal(l, "require");
	lua_pushstring(l, filename);

	if (lua_pcall(l, 1, 1, 0) != 0) {
		as_log_error("lua create error: %s", lua_tostring(l, -1));
		lua_close(l);
		return NULL;
	}

	as_log_debug("lua state created for %s is %d kbytes", filename,
			lua_gc(l, LUA_GCCOUNT, 0));

	return l;
}

static bool
load_buffer(lua_State* l, const char* script, size_t size, const char* name)
{
	if (luaL_loadbuffer(l, script, size - 1, name) != 0 ||
			lua_pcall(l, 0, LUA_MULTRET, 0) != 0) {
		as_log_error("failed to load lua string: %s %zu", name, size);
		lua_close(l);
		return false;
	}

	return true;
}

static bool
is_native_module(const char* user_path, const char* filename)
{
	struct stat buf;
	char full_path[512]; // >= 255 + 1 + 127 + 1 + 2 + 1

	sprintf(full_path, "%s/%s.so", user_path, filename);

	return stat(full_path, &buf) == 0;
}


//==========================================================
// Local helpers - miscellaneous.
//

static int
handle_error(lua_State* l)
{
	const char* msg = luaL_optstring(l, 1, 0);

	as_log_error("lua runtime error: %s", msg);

	return 1;
}

// Lua debug hook to check for a timeout.
static void
check_timer(lua_State* l, lua_Debug* ar)
{
	if (ar->event == LUA_HOOKCOUNT) {
		if (as_timer_timedout(&g_timer)) {
			luaL_error(l, "UDF Execution Timeout");
		}
	}
}


//==========================================================
// Simple hashmap for lua configuration.
// - keys are null terminated but fixed-size allocated
// - key parameters are assumed to be good
// - values are lua cache_entry struct pointers
//

static inline lua_hash_ele*
lua_hash_get_row_head(const lua_hash* h, const char* key)
{
	uint32_t row_i = cf_wyhash32((const uint8_t*)key, strlen(key)) % h->n_rows;

	return &h->table[row_i];
}

static inline void
lua_hash_call_cb_if(void (*cb)(cache_entry*), cache_entry* centry)
{
	if (cb != NULL && centry != NULL) {
		(*cb)(centry);
	}
}

lua_hash*
lua_hash_create(uint32_t n_rows)
{
	lua_hash* h = (lua_hash*)cf_malloc(sizeof(lua_hash));

	*h = (lua_hash){
			.n_rows = n_rows,
			.table = (lua_hash_ele*)cf_calloc(n_rows, sizeof(lua_hash_ele))
	};

	return h;
}

// Note - This is only here for a unit test, not used by client or server.
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

	// Nothing in row yet so just set first element.
	if (e->value == NULL) {
		strcpy(e->key, key);
		e->value = value;
		return NULL;
	}

	lua_hash_ele* e_head = e;
	cache_entry* overwritten_value = NULL;

	while (e != NULL) {
		if (strcmp(e->key, key) == 0) {
			overwritten_value = e->value;
			break;
		}

		e = e->next;
	}

	if (overwritten_value == NULL) {
		e = (lua_hash_ele*)cf_malloc(sizeof(lua_hash_ele));

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

	while (e != NULL) {
		if (strcmp(e->key, key) == 0) {
			if (p_value != NULL) {
				*p_value = e->value;
			}

			return true;
		}

		e = e->next;
	}

	return false;
}

cache_entry*
lua_hash_remove(lua_hash* h, const char* key)
{
	lua_hash_ele* e = lua_hash_get_row_head(h, key);

	// Nothing in row yet so nothing to delete.
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
				if (e->next != NULL) { // move the next element up to the head
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

// Wipe out all entries but leave hash itself intact. This function cleans up
// the hash itself. The callback may be used to do any additional cleanup on the
// hash values.
void
lua_hash_clear(lua_hash* h, void (*cb)(cache_entry*))
{
	lua_hash_ele* e_table = h->table;

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
		e_table++;
	}
}
