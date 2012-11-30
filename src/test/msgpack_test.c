#include "test.h"
#include "as_msgpack.h"
#include "as_hashmap.h"
#include "as_arraylist.h"

as_map * make_map();
as_list * make_list();

typedef int (*handle_msgpack)(msgpack_sbuffer * sbuf);

as_map * make_map(int level) {
    as_map * m = as_hashmap_new(32);
    if ( !level ) return m;
    as_map_set(m, (as_val *) as_string_new("b"), (as_val *) as_boolean_new(true));
    as_map_set(m, (as_val *) as_string_new("s"), (as_val *) as_string_new("word"));
    as_map_set(m, (as_val *) as_string_new("i"), (as_val *) as_integer_new(1234));
    as_map_set(m, (as_val *) as_string_new("l"), (as_val *) make_list(level-1));
    as_map_set(m, (as_val *) as_string_new("m"), (as_val *) make_map(level-1));
    as_map_set(m, (as_val *) as_boolean_new(true), (as_val *) as_boolean_new(false));
    as_map_set(m, (as_val *) as_integer_new(20), (as_val *) as_integer_new(200));
    as_map_set(m, (as_val *) make_list(level-1), (as_val *) make_list(level-1));
    as_map_set(m, (as_val *) make_map(level-1), (as_val *) make_map(level-1));
    return m;
}

as_list * make_list(int level) {
    as_list * l = as_arraylist_new(10,10);
    if ( !level ) return l;
    as_list_append(l, (as_val *) as_boolean_new(true));
    as_list_append(l, (as_val *) as_boolean_new(false));
    as_list_append(l, (as_val *) as_string_new("word"));
    as_list_append(l, (as_val *) as_integer_new(1234));
    as_list_append(l, (as_val *) make_map(level-1));
    as_list_append(l, (as_val *) make_list(level-1));
    return l;
}

int print_msgpack(msgpack_sbuffer * sbuf) {
    msgpack_zone mempool;
    msgpack_zone_init(&mempool, 2048);

    msgpack_object deserialized;
    msgpack_unpack(sbuf->data, sbuf->size, NULL, &mempool, &deserialized);
    
    msgpack_object_print(stdout, deserialized);
    puts("");

    msgpack_zone_destroy(&mempool);
    return 0;
}

int pack_map(as_map * m, handle_msgpack handler) {

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    as_msgpack_pack_map(&pk, m);

    handler(&sbuf);

    msgpack_sbuffer_destroy(&sbuf);
    return 0;
}

int pack_list(as_list * l, handle_msgpack handler) {

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    as_msgpack_pack_list(&pk, l);

    handler(&sbuf);

    msgpack_sbuffer_destroy(&sbuf);
    return 0;
}


int main(int argc, char ** argv) {
    
    pack_list(make_list(5), print_msgpack);
    pack_map(make_map(5), print_msgpack);

    return 0;
}
