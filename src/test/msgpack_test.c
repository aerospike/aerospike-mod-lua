#include "test.h"
#include "as_msgpack.h"
#include "as_types.h"
#include "as_hashmap.h"
#include "as_arraylist.h"

as_map * make_map();
as_list * make_list();

as_map * make_map(int level) {

    as_map * m = as_map_new(as_hashmap_new(32), &as_hashmap_map);
    // if ( !level ) return m;
    as_map_set(m, (as_val *) as_string_new("b"), (as_val *) as_boolean_new(true));
    as_map_set(m, (as_val *) as_string_new("s"), (as_val *) as_string_new("word"));
    as_map_set(m, (as_val *) as_string_new("i"), (as_val *) as_integer_new(1234));
    if ( level > 0 ) as_map_set(m, (as_val *) as_string_new("l"), (as_val *) make_list(level-1));
    if ( level > 0 ) as_map_set(m, (as_val *) as_string_new("m"), (as_val *) make_map(level-1));
    as_map_set(m, (as_val *) as_boolean_new(true), (as_val *) as_boolean_new(false));
    as_map_set(m, (as_val *) as_integer_new(20), (as_val *) as_integer_new(200));
    // as_map_set(m, (as_val *) make_list(level-1), (as_val *) make_list(level-1));
    // as_map_set(m, (as_val *) make_map(level-1), (as_val *) make_map(level-1));


    printf("created map: %d\n", as_map_size(m));
    return m;
}

as_list * make_list(int level) {

    as_list * l = as_list_new(as_arraylist_new(10,10), &as_arraylist_list);
    // if ( !level ) return l;
    as_list_append(l, (as_val *) as_boolean_new(true));
    as_list_append(l, (as_val *) as_boolean_new(false));
    as_list_append(l, (as_val *) as_string_new("a"));
    as_list_append(l, (as_val *) as_string_new("b"));
    as_list_append(l, (as_val *) as_string_new("c"));
    as_list_append(l, (as_val *) as_string_new("d"));
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

    printf("t: %d\n",as_val_type(v));
    printf("a: %s\n",as_val_tostring(v));

    as_serializer_serialize(&s, v, &b);

    print_buffer(&b);

    as_val * out = NULL;
    as_serializer_deserialize(&s, &b, &out);

    printf("c: %s\n",as_val_tostring(out));
    printf("\n");

    // watch it go!!!!
    // serialize(out);

    as_val_free(out);

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
