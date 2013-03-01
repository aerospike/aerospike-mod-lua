#include "test.h"
#include "util/test_logger.h"

PLAN( mod_lua_test ) {

    test_logger.level = AS_LOG_INFO;

    /**
     * types - tests types
     */
    plan_add( types_string );
    plan_add( types_integer );
    plan_add( types_bytes );
    plan_add( types_arraylist );
    plan_add( types_linkedlist );
    plan_add( types_hashmap );

    /**
     * stream - stream tests
     */
    plan_add( stream_basics );
    plan_add( stream_udf );

    /**
     * record - record tests
     */
    // plan_add( record_basics );
    plan_add( record_udf );
	// plan_add( bytes_udf );
}
