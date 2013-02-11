#include "test.h"

#include "mod_lua.h"
#include "mod_lua_config.h"
#include "as_result.h"
#include "as_stream.h"
#include "as_types.h"
#include "as_pair.h"

#include <time.h>

#define LIMIT 1000000


/******************************************************************************
 * AS_REC -- HASHMAP-BACKED RECORD
 *****************************************************************************/

static as_val * hashmap_record_get(const as_rec * r, const char * name) {
    as_hashmap * m = (as_hashmap *) as_rec_source(r);
    as_val * k = (as_val *) as_string_new((char *)name);
    return as_hashmap_get(m,k);
}

static int hashmap_record_set(const as_rec * r, const char * name, const as_val * value) {
    as_hashmap * m = (as_hashmap *) as_rec_source(r);
    as_val * k = (as_val *) as_string_new((char *)name);
    return as_hashmap_set(m, k, (as_val *) value);
}

static int hashmap_record_remove(const as_rec * r, const char * name) {
    return 0;
}

static int hashmap_record_free(as_rec * r) {
    as_hashmap * m = (as_hashmap *) as_rec_source(r);
    as_hashmap_free(m);
    return 0;
}

static uint32_t hashmap_record_hash(as_rec * r) {
    return as_val_hash((as_val *) as_rec_source(r));
}

static const as_rec_hooks hashmap_record = {
    .get    = hashmap_record_get,
    .set    = hashmap_record_set,
    .remove = hashmap_record_remove,
    .free   = hashmap_record_free,
    .hash   = hashmap_record_hash
};

/******************************************************************************
 * AS_AEROSPIKE -- Test specific instance
 *****************************************************************************/

static int aslog(const as_aerospike * as, const char * file, const int line, const int level, const char * m) {
    LOG("[%s:%d] %s", file, line, m);
    return 0;
}

static const as_aerospike_hooks test_aerospike = {
    .log = aslog
};

/******************************************************************************
 * MAIN
 *****************************************************************************/

as_linkedlist * arglist(int argc, char ** argv) {
    if ( argc == 0 || argv == NULL ) return as_linkedlist_new(NULL,NULL);
    char * str = argv[0];
    char * invalid = NULL;
    long num  = strtol(str, &invalid, 10);
    if ( invalid == NULL ) {
        return as_linkedlist_new((as_val *)as_string_new(str), arglist(argc-1, argv+1));
    }
    else {
        return as_linkedlist_new((as_val *)as_integer_new(num), arglist(argc-1, argv+1));
    }
}


static void show_result(as_result * res) {
    if ( res->is_success ) {
        as_val * v = res->value;
        switch( as_val_type(v) ) {
            case AS_UNKNOWN:
                printf("SUCCESS: <unknown>\n");
                break;
            default:
                printf("SUCCESS: %s\n", as_val_tostring(v));
        }
    }
    else {
        as_val * v = res->value;
        as_string * s = as_string_fromval(v);
        printf("FAILURE: %s\n", as_string_tostring(s));
    }
}


int main ( int argc, char ** argv ) {

    if ( argc < 4 ) {
        printf("Usage: record_udf_test <iterations> <filename> <function> [args ...]\n");
        return 1;
    }

    char * iterstr = argv[1];
    char * filename = argv[2];
    char * function = argv[3];

    char * itererr = NULL;
    long iterations = strtol(iterstr, &itererr, 10);

    if ( itererr == NULL ) {
        printf("Usage: record_udf_test <iterations> <filename> <function> [args ...]\n");
        return 1;
    }
    
    mod_lua_config_op config = {
        .optype = MOD_LUA_CONFIG_OP_INIT,
        .arg = NULL,
        .config = mod_lua_config_new(true, "src/lua", "src/test/lua")
    };

    as_module_init(&mod_lua);    
    as_module_configure(&mod_lua, &config);


    as_aerospike as;
    as_aerospike_init(&as, NULL, &test_aerospike);

    as_rec rec;
    as_rec_init(&rec, as_hashmap_new(32), &hashmap_record);
    as_rec_set(&rec, "s", (as_val *) as_string_new("hello"));
    as_rec_set(&rec, "b", (as_val *) as_boolean_new(true));
    as_rec_set(&rec, "i", (as_val *) as_integer_new(12345));
    as_rec_set(&rec, "p", (as_val *) pair(as_string_new("five"),as_integer_new(5)));


    as_linkedlist * args = argc > 4 ? arglist(argc-4, argv+4) : arglist(0,NULL);
    as_list arglist;
    as_list_init(&arglist, args, &as_linkedlist_list);

    as_result res = { true, NULL };
    
    time_t t0, t1;
    time(&t0);
    for ( int i = 0; i < iterations; i ++ ) {
        as_module_apply_record(&mod_lua, &as, filename, function, &rec, &arglist, &res);
        // show_result(&res);
    }
    time(&t1);

    LOG("STATS: %f seconds for %d calls", difftime(t1,t0), iterations);
    LOG("TIME: %f cps", iterations/difftime(t1,t0));
    

    return 0;
}
