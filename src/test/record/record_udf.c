
#include "../test.h"
#include <aerospike/as_stream.h>
#include <aerospike/as_types.h>
#include <limits.h>
#include <stdlib.h>

#include <aerospike/as_module.h>
#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>

#include "../util/test_aerospike.h"
#include "../util/test_logger.h"
#include "../util/map_rec.h"

/******************************************************************************
 * VARIABLES
 *****************************************************************************/

static as_aerospike as;

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( record_udf_1, "echo bin a of {a = 123, b = 456 }" ) {

    as_rec * rec = map_rec_new();
    as_rec_set(rec, "a", (as_val *) as_integer_new(123));

    as_list * arglist = (as_list *) as_arraylist_new(1,0);
    as_list_append_str(arglist, "a");

    as_result * res = as_success_new(NULL);

    int rc = as_module_apply_record(&mod_lua, &as, "records", "getbin", rec, arglist, res);

    assert_int_eq( rc, 0 );
    assert_true( res->is_success );
    assert_not_null( res->value );
    assert_int_eq( as_integer_toint((as_integer *) res->value), 123 );

    as_rec_destroy(rec);
    as_list_destroy(arglist);
    as_result_destroy(res);
}

TEST( record_udf_2, "concat bins a and b of {a = 'abc', b = 'def' }" ) {

    as_rec * rec = map_rec_new();
    as_rec_set(rec, "a", (as_val *) as_string_new("abc",false));
    as_rec_set(rec, "b", (as_val *) as_string_new("def",false));

    as_list * arglist = (as_list *) as_arraylist_new(2,0);
    as_list_append_str(arglist, "a");
    as_list_append_str(arglist, "b");

    as_result * res = as_success_new(NULL);

    int rc = as_module_apply_record(&mod_lua, &as, "records", "cat", rec, arglist, res);

    assert_int_eq( rc, 0);
    assert_true( res->is_success );
    assert_not_null( res->value );
    assert_string_eq( as_string_tostring((as_string *)res->value), "abcdef");

    as_rec_destroy(rec);
    as_list_destroy(arglist);
    as_result_destroy(res);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite) {

    test_aerospike_init(&as);

    mod_lua_config config = {
        .server_mode    = true,
        .cache_enabled  = true,
        .system_path    = "src/lua",
        .user_path      = "src/test/lua"
    };

    if ( mod_lua.logger == NULL ) {
        mod_lua.logger = test_logger_new();
    }
        
    int rc = as_module_configure(&mod_lua, &config);

    if ( rc != 0 ) {
        error("as_module_configure failed: %d", rc);
        return false;
    }
 
    return true;
}

static bool after(atf_suite * suite) {
    return true;
}

SUITE( record_udf, "stream udf tests" ) {
    suite_before( before );
    suite_after( after );
    
    suite_add( record_udf_1 );
    suite_add( record_udf_2 );
}