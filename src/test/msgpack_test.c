#include "test.h"

#include "as_val.h"

#include "as_msgpack.h"
#include "as_types.h"
#include "as_hashmap.h"
#include "as_map.h"
#include "as_arraylist.h"

// forward declaration
as_list *make_list(int level);

as_map * make_map(int level) {

    as_map * m = as_hashmap_new(32);
    // if ( !level ) return m;
    as_map_set(m, 
        (as_val *) as_string_new("b",false), 
        (as_val *) as_boolean_new(true));
    as_map_set(m, 
        (as_val *) as_string_new("s", false), 
        (as_val *) as_string_new("word", false));
    as_map_set(m, 
        (as_val *) as_string_new("i", false), 
        (as_val *) as_integer_new(1234));
    if ( level > 0 ) 
        as_map_set(m, 
            (as_val *) as_string_new("l", false), 
            (as_val *) make_list(level-1));
    if ( level > 0 ) 
        as_map_set(m, 
            (as_val *) as_string_new("m", false), 
            (as_val *) make_map(level-1));
    as_map_set(m, 
        (as_val *) as_boolean_new(true), (as_val *) as_boolean_new(false));
    as_map_set(m, 
        (as_val *) as_integer_new(20), (as_val *) as_integer_new(200));
    // as_map_set(m, (as_val *) make_list(level-1), (as_val *) make_list(level-1));
    // as_map_set(m, (as_val *) make_map(level-1), (as_val *) make_map(level-1));

    printf("created map: %d\n", as_map_size(m));
    return m;
}

as_list * make_list(int level) {

    as_list * l = as_arraylist_new(10,10);
    // if ( !level ) return l;
    as_list_append(l, (as_val *) as_boolean_new(true));
    as_list_append(l, (as_val *) as_boolean_new(false));
    as_list_append(l, (as_val *) as_string_new("a", false));
    as_list_append(l, (as_val *) as_string_new("b", false));
    as_list_append(l, (as_val *) as_string_new("c", false));
    as_list_append(l, (as_val *) as_string_new("d", false));
    as_list_append(l, (as_val *) as_integer_new(1234));
    // if ( level > 0 ) as_list_append(l, (as_val *) make_map(level-1));
    // if ( level > 0 ) as_list_append(l, (as_val *) make_list(level-1));

    printf("created list: %d\n", as_list_size(l));
    return l;
}

int print_buffer(as_buffer * buff) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    sbuf.data = buff->data;
    sbuf.size = buff->size;
    sbuf.alloc = buff->capacity;

    msgpack_zone mempool;
    msgpack_zone_init(&mempool, 2048);

    msgpack_object deserialized;
    msgpack_unpack(sbuf.data, sbuf.size, NULL, &mempool, &deserialized);
    
    printf("b: ");
    msgpack_object_print(stdout, deserialized);
    puts("");

    msgpack_zone_destroy(&mempool);
    return 0;
}

int serialize(as_val * v) {

    as_buffer b;
    as_buffer_init(&b);
    
    as_serializer s;
    as_msgpack_init(&s);

    char *ac = as_val_tostring(v);
    printf("t: %d\n",as_val_type(v));
    printf("a: %s\n",ac);
    free(ac);

    as_serializer_serialize(&s, v, &b);

    print_buffer(&b);

    as_val * out = NULL;
    as_serializer_deserialize(&s, &b, &out);

    printf("c: %s\n",as_val_tostring(out));
    printf("\n");

    // watch it go!!!!
    // serialize(out);

    as_val_destroy(out);

    as_serializer_destroy(&s);
    as_buffer_destroy(&b);

    return 0;
}


int main(int argc, char ** argv) {
    // serialize((as_val *) as_boolean_new(true));
    // serialize((as_val *) as_boolean_new(false));
    // serialize((as_val *) as_integer_new(1));
    // serialize((as_val *) as_string_new("hello world"));
    // serialize((as_val *) make_list(0));
    serialize((as_val *) make_map(1));

    printf("<end>\n");
    return 0;
}
