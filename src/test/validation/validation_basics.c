/*
 * Copyright 2008-2016 Aerospike, Inc.
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

#include <errno.h>
#include <limits.h>
#include <stdlib.h>

#include <aerospike/as_arraylist.h>
#include <aerospike/as_list.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_stream.h>
#include <aerospike/as_string.h>
#include <aerospike/as_rec.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_module.h>
#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>


#include "../test.h"
#include "../util/test_aerospike.h"
#include "../util/test_logger.h"
#include "../util/map_rec.h"

/******************************************************************************
 * VARIABLES
 *****************************************************************************/

typedef struct {
	char *	filename;
	char * 	description;
	bool 	is_valid;
} validation_entry;

#define SCRIPT_LEN_MAX 1048576

static as_aerospike as;

static int readfile(const char * filename, char ** content, uint32_t * size) {
	
	uint8_t * content_v = content ? (uint8_t *) *content : NULL;
	uint32_t size_v = size ? (uint32_t) *size : 0;

	FILE * file = fopen(filename,"r"); 

	if ( !file ) { 
		error("cannot open script file %s : %s", filename, strerror(errno));  
		return -1; 
	} 

	if ( content_v == NULL ) {
		content_v = (uint8_t *) cf_malloc(SCRIPT_LEN_MAX);
		if ( content_v == NULL ) { 
			error("cf_malloc failed");
			fclose(file);
			return -2;
		}
	}

	int size_b = 0; 

	uint8_t * buff = content_v; 
	int read = (int)fread(buff, 1, 512, file);
	while ( read ) { 
		size_b += read; 
		buff += read; 
		read = (int)fread(buff, 1, 512, file);
		if ( size_b >= size_v-1 ) {
			break;
		}
	}           
	fclose(file);

	content_v[size_b] = '\0';

	*content = (char *) content_v;
	*size = size_b;

	return 0;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( validation_basics_1, "validation: src/test/lua/validate_*.lua" )
{
	validation_entry entries[] = {
		{ "src/test/lua/validate_1.lua", "single file local variable", true },
		{ "src/test/lua/validate_2.lua", "invalid function in module scope", false },
		{ "src/test/lua/validate_3.lua", "invalid statement in function scope", false },
		{ "src/test/lua/validate_4.lua", "index a global variable in module scope", false },
		{ "src/test/lua/validate_5.lua", "index a global variable in function scope", true },
		{ "src/test/lua/validate_6.lua", "create closure on a global variable in function scope", true },
		{ NULL }
	};

	for( validation_entry * entry = entries; entry != NULL && entry->filename != NULL; entry++ ) {

		info("validating %s [%s] - %s", entry->filename, entry->is_valid ? "VALID" : "INVALID", entry->description);

		as_module_error err;
		int rc = 0;

		char * buff = NULL;
		uint32_t size = 0;

		rc = readfile(entry->filename, &buff, &size);
		assert(rc == 0);

		rc = as_module_validate(&mod_lua, &as, entry->filename, buff, size, &err);
		cf_free(buff);
		
		if ( rc != 0 && entry->is_valid ) {
			info("error = {");
			info("  scope   = %d", err.scope);
			info("  code    = %d", err.code);
			info("  message = %s", err.message);
			info("  file    = %s", err.file);
			info("  line    = %d", err.line);
			info("  func    = %s", err.func);
			info("}");
			assert(rc == 0 && entry->is_valid);
		}
		
		if ( rc == 0 && !entry->is_valid ) {
			assert(rc != 0 && !entry->is_valid);
		}
	}
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite) {

	test_aerospike_init(&as);

    mod_lua_config config = {
        .server_mode    = true,
        .cache_enabled  = false,
        .system_path    = {'\0'},
        .user_path      = "src/test/lua"
    };

    char * system_path = getenv("AS_SYSTEM_LUA");
    if ( system_path != NULL ) {
	    strncpy(config.system_path, system_path, 255);
	    config.system_path[255] = '\0';
    }
    else {
    	error("environment variable 'AS_SYSTEM_LUA' should be set to point to the directory containing system lua files.")
    	return false;
    }

	as_lua_log_init();

	int rc = as_module_configure(&mod_lua, &config);

	if ( rc != 0 ) {
		error("as_module_configure failed: %d", rc);
		return false;
	}
 
	return true;
}

SUITE( validation_basics, "record basics" )
{
	suite_before( before );
	
	suite_add( validation_basics_1 );
}
