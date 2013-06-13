#include "test.h"
#include "util/test_logger.h"

PLAN( mod_lua_test ) {

    test_logger.level = AS_LOGGER_LEVEL_INFO;
    
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
	plan_add( bytes_udf );
}
