#include "test.h"

#include "mod_lua.h"
#include "mod_lua_config.h"
#include "as_result.h"
#include "as_stream.h"
#include "as_types.h"
#include "as_string.h"
#include "as_rec.h"

#define LIMIT 1


static as_rec * maprecord_create() ;
static as_val * maprecord_get(const as_rec *, const char *);
static int maprecord_set(const as_rec *, const char *, const as_val *);
static int maprecord_free(as_rec *);
static uint32_t maprecord_hash(as_rec *);

static const as_rec_hooks maprecord_hooks;

static as_rec * maprecord_create() {
    return as_rec_new(as_hashmap_new(8), &maprecord_hooks);
}

static as_val * maprecord_get(const as_rec * r, const char * name) {
    as_map * m = (as_map *) as_rec_source(r);
    as_val * k = (as_val *) as_string_new((char *)name);
    return as_map_get(m,k);
}

static int maprecord_set(const as_rec * r, const char * name, const as_val * value) {
    as_map * m = (as_map *) as_rec_source(r);
    as_val * k = (as_val *) as_string_new((char *)name);
    return as_map_set(m, k, (as_val *) value);
}

static int maprecord_remove(const as_rec * r, const char * name) {
    // as_map * m = (as_map *) as_rec_source(r);
    return 0;
}

static int maprecord_free(as_rec * r) {
    as_map * m = (as_map *) as_rec_source(r);
    as_map_free(m);
    // free(r);
    return 0;
}

static uint32_t maprecord_hash(as_rec * r) {
    return as_val_hash((as_val *) as_rec_source(r));
}

static const as_rec_hooks maprecord_hooks = {
    .get    = maprecord_get,
    .set    = maprecord_set,
    .remove = maprecord_remove,
    .free   = maprecord_free,
    .hash   = maprecord_hash
};





as_list * arglist(int argc, char ** argv) {
    if ( argc == 0 || argv == NULL ) return cons(NULL,NULL);
    log("arg: %s",argv[0]);
    return cons((as_val *) as_string_new(argv[0]), arglist(argc-1, argv+1));
}


static void show_result(as_result * res) {
    log("SHOW RESULTS!");
    if ( res->is_success ) {
        as_val * v = res->value;
        as_integer * i;
        as_string * s;
        switch( as_val_type(v) ) {
            case AS_INTEGER:
                i = as_integer_fromval(v);
                printf("SUCCESS: %lu\n", as_integer_toint(i) );
                break;
            case AS_STRING:
                s = as_string_fromval(v);
                printf("SUCCESS: %s\n", as_string_tostring(s) );
                break;
            default:
                printf("SUCCESS: <unknown>\n");
        }
    }
    else {
        as_val * v = res->value;
        as_string * s = as_string_fromval(v);
        printf("FAILURE: %s\n", as_string_tostring(s));
    }
}

static int aslog(as_aerospike * as, const char * file, int line, int level, const char * m) {
    log_append(file,line,"[%d] - %s",level,m);
    return 0;
}

static void run_record(const char * filename, const char * function, as_list * args, as_result * res) {

    as_aerospike_hooks  as_hooks    = { .log = aslog };
    as_aerospike *      as          = as_aerospike_new(NULL, &as_hooks);
    as_rec *            rec         = maprecord_create();

    as_rec_set(rec, "a", (as_val *) as_string_new("x"));
    as_rec_set(rec, "b", (as_val *) as_string_new("y"));
    as_rec_set(rec, "c", (as_val *) as_string_new("z"));

    // as_module_apply_record(&mod_lua, as, filename, function, rec, args, res);
    as_module_apply_record(&mod_lua, as, filename, function, rec, as_linkedlist_new((as_val*)args,NULL), res);
}

int main ( int argc, char ** argv ) {

    if ( argc < 3 ) {
        printf("Usage: record_udf_test <filename> <function> [args ...]\n");
        return 1;
    }

    mod_lua_config config = {
        .cache_enabled  = false,
        .system_path    = "src/lua", 
        .user_path      = "src/test/lua"
    };

    as_module_init(&mod_lua);
    as_module_configure(&mod_lua, &config);

    char * filename = argv[1];
    char * function = argv[2];

    as_list * args = argc > 3 ? arglist(argc-3, argv+3) : as_linkedlist_new(NULL,NULL);

    int i;
    for ( i = 0; i < LIMIT; i++ ) {
        as_result res = { false, NULL };
        run_record(filename, function, args, &res);
        show_result(&res);
    }

    // as_list_free(args);
    
    return 0;
}