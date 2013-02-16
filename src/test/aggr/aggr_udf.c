
#include "../test.h"
#include <as_stream.h>
#include <as_types.h>
#include <limits.h>
#include <stdlib.h>

#include <as_module.h>
#include <mod_lua.h>
#include <mod_lua_config.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_aerospike as;

extern as_rec * map_rec_new();
extern as_stream * rec_stream_new();
extern as_stream * integer_stream_new(uint32_t start, uint32_t end);
extern as_stream * list_stream_new(as_list * l);
extern uint32_t stream_pipe(as_stream * istream, as_stream * ostream);

static int test_log(const as_aerospike * as, const char * file, const int line, const int level, const char * msg) {
    char l[10] = {'\0'};
    switch(level) {
        case 1:
            strncpy(l,"WARN",10);
            break;
        case 2:
            strncpy(l,"INFO",10);
            break;
        case 3:
            strncpy(l,"DEBUG",10);
            break;
        default:
            strncpy(l,"TRACE",10);
            break;
    }
    atf_log_line(stderr, l, ATF_LOG_PREFIX, file, line, msg);
    return 0;
}

static const as_aerospike_hooks test_aerospike_hooks = {
    .destroy = NULL,
    .rec_create = NULL,
    .rec_update = NULL,
    .rec_remove = NULL,
    .rec_exists = NULL,
    .log = test_log,
};

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( aggr_udf_1, "filter even numbers from range (1-10)" ) {
 
    as_stream * istream = integer_stream_new(1,10);     

    as_list * l = as_arraylist_new(100,0);
    as_stream * ostream = list_stream_new(l);

    as_list * arglist = as_arraylist_new(100,0);

    int rc = as_module_apply_stream(&mod_lua, &as, "aggr", "even", istream, arglist, ostream);

    assert_int_eq( rc, 0);
    assert_int_eq( as_list_size(l), 5);


    as_val * v = as_list_head(l);
    assert_int_eq(as_integer_toint((as_integer *) v), 2);
    
    as_iterator * it = as_list_iterator_new(l);
    while ( as_iterator_has_next(it) ) {
        const as_val * v = as_iterator_next(it);
        info("result: %s",as_val_tostring(v));
    }
}

TEST( aggr_udf_2, "increment range (1-10)" ) {
 
    as_stream * istream = integer_stream_new(1,10);     

    as_list * l = as_arraylist_new(100,0);
    as_stream * ostream = list_stream_new(l);

    as_list * arglist = as_arraylist_new(100,0);

    int rc = as_module_apply_stream(&mod_lua, &as, "aggr", "increment", istream, arglist, ostream);

    assert_int_eq( rc, 0);
    assert_int_eq( as_list_size(l), 10);


    as_val * v = as_list_head(l);
    assert_int_eq(as_integer_toint((as_integer *) v), 2);
    
    as_iterator * it = as_list_iterator_new(l);
    while ( as_iterator_has_next(it) ) {
        const as_val * v = as_iterator_next(it);
        info("result: %s",as_val_tostring(v));
    }
}

TEST( aggr_udf_3, "sum range (1-1,000,000)" ) {

    as_stream * istream = integer_stream_new(1,1000*1000);

    as_list * l = as_arraylist_new(1,1);
    as_stream * ostream = list_stream_new(l);

    as_list * arglist = as_arraylist_new(100,0);

    int rc = as_module_apply_stream(&mod_lua, &as, "aggr", "sum", istream, arglist, ostream);

    assert_int_eq( rc, 0);

    assert_int_eq( as_list_size(l), 1);

    as_val * v = as_list_head(l);
    assert_int_eq(as_integer_toint((as_integer *) v), 500000500000);

    as_iterator * it = as_list_iterator_new(l);
    while ( as_iterator_has_next(it) ) {
        const as_val * v = as_iterator_next(it);
        info("result: %s",as_val_tostring(v));
    }
}

TEST( aggr_udf_4, "product range (1-10)" ) {
 
    as_stream * istream = integer_stream_new(1,10);     

    as_list * l = as_arraylist_new(100,0);
    as_stream * ostream = list_stream_new(l);

    as_list * arglist = as_arraylist_new(100,0);

    int rc = as_module_apply_stream(&mod_lua, &as, "aggr", "product", istream, arglist, ostream);

    assert_int_eq( rc, 0);
    assert_int_eq( as_list_size(l), 1);


    as_val * v = as_list_head(l);
    assert_int_eq(as_integer_toint((as_integer *) v), 3628800);

    as_iterator * it = as_list_iterator_new(l);
    while ( as_iterator_has_next(it) ) {
        const as_val * v = as_iterator_next(it);
        info("result: %s",as_val_tostring(v));
    }
}

TEST( aggr_udf_5, "campaign rollup w/ map & reduce" ) {
 
    as_stream * istream = rec_stream_new(100);  

    as_list * l = as_arraylist_new(100,0);
    as_stream * ostream = list_stream_new(l);

    as_list * arglist = as_arraylist_new(100,100);

    int rc = as_module_apply_stream(&mod_lua, &as, "aggr", "rollup", istream, arglist, ostream);

    assert_int_eq( rc, 0);
    assert_int_eq( as_list_size(l), 1);


    as_map * v = (as_map *) as_list_head(l);
    assert_int_eq(as_map_size(v), 10);
    
    as_integer i;

    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 0))), 5450);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 1))), 4740);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 2))), 4930);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 3))), 5120);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 4))), 4310);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 5))), 5500);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 6))), 4690);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 7))), 4880);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 8))), 5070);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 9))), 5260);

    as_iterator * it = as_list_iterator_new(l);
    while ( as_iterator_has_next(it) ) {
        const as_val * v = as_iterator_next(it);
        info("result: %s",as_val_tostring(v));
    }
}


TEST( aggr_udf_6, "campaign rollup w/ aggregate" ) {
 
    as_stream * istream = rec_stream_new(100);  

    as_list * l = as_arraylist_new(100,0);
    as_stream * ostream = list_stream_new(l);

    as_list * arglist = as_arraylist_new(100,100);

    int rc = as_module_apply_stream(&mod_lua, &as, "aggr", "rollup2", istream, arglist, ostream);

    assert_int_eq( rc, 0);
    assert_int_eq( as_list_size(l), 1);


    as_map * v = (as_map *) as_list_head(l);
    assert_int_eq(as_map_size(v), 10);
    
    as_integer i;

    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 0))), 5450);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 1))), 4740);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 2))), 4930);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 3))), 5120);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 4))), 4310);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 5))), 5500);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 6))), 4690);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 7))), 4880);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 8))), 5070);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(v, (as_val *) as_integer_init(&i, 9))), 5260);

    info("result: %s",as_val_tostring(v));
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite) {
    as_aerospike_init(&as, NULL, &test_aerospike_hooks);

    mod_lua_config_op conf_op = {
        .optype     = MOD_LUA_CONFIG_OP_INIT,
        .arg        = NULL,
        .config     = mod_lua_config_client(true, "src/lua", "src/test/lua")
    }; 

    as_module_init(&mod_lua);
    as_module_configure(&mod_lua, &conf_op);
 
    return true;
}

static bool after(atf_suite * suite) {
    return true;
}

SUITE( aggr_udf, "aggregate udf" ) {
    suite_before( before );
    suite_after( after );
    
    suite_add( aggr_udf_1 );
    suite_add( aggr_udf_2 );
    suite_add( aggr_udf_3 );
    suite_add( aggr_udf_4 );
    suite_add( aggr_udf_5 );
    suite_add( aggr_udf_6 );
}