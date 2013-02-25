
#include "../test.h"
#include <as_stream.h>
#include <as_types.h>
#include <limits.h>
#include <stdlib.h>

#include <as_module.h>
#include <mod_lua.h>
#include <mod_lua_config.h>


#include "../util/test_aerospike.h"
#include "../util/map_rec.h"
#include "../util/producer_stream.h"
#include "../util/consumer_stream.h"

/******************************************************************************
 * VARIABLES
 *****************************************************************************/

static as_aerospike as;

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( stream_udf_1, "filter even numbers from range (1-10)" ) {
 
    uint32_t limit = 10;
    uint32_t produced = 0;
    uint32_t consumed = 0;
    
    as_val * produce() {
        if ( produced >= limit ) return AS_STREAM_END;
        produced++;
        return (as_val *) as_integer_new(produced);
    }

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) consumed++;
        as_val_destroy(v);
        return AS_STREAM_OK;
    }

    as_stream * istream = producer_stream_new(produce);
    as_stream * ostream = consumer_stream_new(consume);
    as_list *   arglist = NULL;

    int rc = as_module_apply_stream(&mod_lua, &as, "aggr", "even", istream, arglist, ostream);

    assert_int_eq( rc, 0);
    assert_int_eq( produced, limit);
    assert_int_eq( consumed, produced / 2);

    as_stream_destroy(istream);
    as_stream_destroy(ostream);
}

TEST( stream_udf_2, "increment range (1-10)" ) {

    uint32_t limit = 10;
    uint32_t produced = 0;
    uint32_t consumed = 0;
    
    as_val * produce() {
        if ( produced >= limit ) return AS_STREAM_END;
        produced++;
        return (as_val *) as_integer_new(produced);
    }

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) consumed++;
        as_val_destroy(v);
        return AS_STREAM_OK;
    }

    as_stream * istream = producer_stream_new(produce);
    as_stream * ostream = consumer_stream_new(consume);
    as_list *   arglist = NULL;

    int rc = as_module_apply_stream(&mod_lua, &as, "aggr", "increment", istream, arglist, ostream);

    assert_int_eq( rc, 0);
    assert_int_eq( produced, limit);
    assert_int_eq( consumed, produced);

    as_stream_destroy(istream);
    as_stream_destroy(ostream);
}

TEST( stream_udf_3, "sum range (1-1,000,000)" ) {

    uint32_t limit = 1000*1000;
    uint32_t produced = 0;
    uint32_t consumed = 0;

    as_integer * result = NULL;
    
    as_val * produce() {
        if ( produced >= limit ) return AS_STREAM_END;
        produced++;
        return (as_val *) as_integer_new(produced);
    }

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) consumed++;
        result = (as_integer *) v;
        return AS_STREAM_OK;
    }

    as_stream * istream = producer_stream_new(produce);
    as_stream * ostream = consumer_stream_new(consume);
    as_list *   arglist = NULL;

    int rc = as_module_apply_stream(&mod_lua, &as, "aggr", "sum", istream, arglist, ostream);

    assert_int_eq( rc, 0);
    assert_int_eq( produced, limit);
    assert_int_eq( consumed, 1);
    assert_int_eq( as_integer_toint(result), 500000500000);

    as_integer_destroy(result);
    as_stream_destroy(istream);
    as_stream_destroy(ostream);
}

TEST( stream_udf_4, "product range (1-10)" ) {
 
    uint32_t limit = 10;
    uint32_t produced = 0;
    uint32_t consumed = 0;

    as_integer * result = NULL;
    
    as_val * produce() {
        if ( produced >= limit ) return AS_STREAM_END;
        produced++;
        return (as_val *) as_integer_new(produced);
    }

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) consumed++;
        result = (as_integer *) v;
        return AS_STREAM_OK;
    }

    as_stream * istream = producer_stream_new(produce);
    as_stream * ostream = consumer_stream_new(consume);
    as_list *   arglist = NULL;

    int rc = as_module_apply_stream(&mod_lua, &as, "aggr", "product", istream, arglist, ostream);

    assert_int_eq( rc, 0);
    assert_int_eq( produced, limit);
    assert_int_eq( consumed, 1);
    assert_int_eq( as_integer_toint(result), 3628800);

    as_integer_destroy(result);
    as_stream_destroy(istream);
    as_stream_destroy(ostream);
}

TEST( stream_udf_5, "campaign rollup w/ map & reduce" ) {

    uint32_t limit = 100;
    uint32_t produced = 0;
    uint32_t consumed = 0;

    as_map * result = NULL;
    
    as_val * produce() {
        if ( produced >= limit ) return AS_STREAM_END;
        produced++;

        as_rec * rec = map_rec_new();
        as_rec_set(rec, "id", (as_val *) as_integer_new(produced));
        as_rec_set(rec, "campaign", (as_val *) as_integer_new(produced % 10));
        as_rec_set(rec, "views", (as_val *) as_integer_new(produced * 2919 % 1000));

        return (as_val *) rec;
    }

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) consumed++;
        result = (as_map *) v;
        return AS_STREAM_OK;
    }

    as_stream * istream = producer_stream_new(produce);
    as_stream * ostream = consumer_stream_new(consume);
    as_list *   arglist = as_arraylist_new(0,0);

    int rc = as_module_apply_stream(&mod_lua, &as, "aggr", "rollup", istream, arglist, ostream);

    assert_int_eq( rc, 0);
    assert_int_eq( produced, limit);
    assert_int_eq( consumed, 1);

    as_integer i;

    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 0))), 5450);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 1))), 4740);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 2))), 4930);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 3))), 5120);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 4))), 4310);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 5))), 5500);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 6))), 4690);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 7))), 4880);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 8))), 5070);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 9))), 5260);


    as_map_destroy(result);
    as_stream_destroy(istream);
    as_stream_destroy(ostream);
}


TEST( stream_udf_6, "campaign rollup w/ aggregate" ) {
 
    uint32_t limit = 100;
    uint32_t produced = 0;
    uint32_t consumed = 0;

    as_map * result = NULL;
    
    as_val * produce() {
        if ( produced >= limit ) return AS_STREAM_END;
        produced++;
        
        as_rec * rec = map_rec_new();
        as_rec_set(rec, "id", (as_val *) as_integer_new(produced));
        as_rec_set(rec, "campaign", (as_val *) as_integer_new(produced % 10));
        as_rec_set(rec, "views", (as_val *) as_integer_new(produced * 2919 % 1000));

        return (as_val *) rec;
    }

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) consumed++;
        result = (as_map *) v;
        return AS_STREAM_OK;
    }

    as_stream * istream = producer_stream_new(produce);
    as_stream * ostream = consumer_stream_new(consume);
    as_list *   arglist = as_arraylist_new(0,0);

    int rc = as_module_apply_stream(&mod_lua, &as, "aggr", "rollup2", istream, arglist, ostream);

    assert_int_eq( rc, 0);
    assert_int_eq( produced, limit);
    assert_int_eq( consumed, 1);

    as_integer i;

    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 0))), 5450);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 1))), 4740);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 2))), 4930);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 3))), 5120);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 4))), 4310);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 5))), 5500);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 6))), 4690);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 7))), 4880);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 8))), 5070);
    assert_int_eq(as_integer_toint((as_integer *) as_map_get(result, (as_val *) as_integer_init(&i, 9))), 5260);

    as_map_destroy(result);
    as_stream_destroy(istream);
    as_stream_destroy(ostream);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite) {
    test_aerospike_init(&as);

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

SUITE( stream_udf, "stream udf tests" ) {
    suite_before( before );
    suite_after( after );
    
    suite_add( stream_udf_1 );
    suite_add( stream_udf_2 );
    suite_add( stream_udf_3 );
    suite_add( stream_udf_4 );
    suite_add( stream_udf_5 );
    suite_add( stream_udf_6 );
}