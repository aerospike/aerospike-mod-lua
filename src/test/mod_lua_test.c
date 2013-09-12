#include "test.h"
#include "util/test_logger.h"

PLAN( mod_lua_test ) {

    test_logger.level = AS_LOGGER_LEVEL_INFO;
    
    /**
     * stream - stream tests
     */
    plan_add( stream_udf );

    /**
     * record - record tests
     */
    plan_add( record_udf );

    /**
     * validation
     */
    plan_add( validation_basics );
}
