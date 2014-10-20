#include "test.h"

PLAN( mod_lua_test )
{
    /**
     * record - record tests
     */
    plan_add( record_udf );

    /**
     * stream - stream tests
     */
    plan_add( stream_udf );

    /**
     * validation
     */
    plan_add( validation_basics );
}
