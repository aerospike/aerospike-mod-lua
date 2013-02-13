
#include "../test.h"
#include <as_arraylist.h>
#include <as_linkedlist.h>
#include <as_list.h>
#include <as_integer.h>
#include <as_stream.h>
#include <as_string.h>
#include <as_rec.h>
#include <as_map.h>
#include <as_hashmap.h>
#include <limits.h>
#include <stdlib.h>


/******************************************************************************
 * TYPES
 *****************************************************************************/



as_val * map_rec_get(const as_rec * r, const char * name) {
    as_map * m = (as_map *) as_rec_source(r);
    as_string s;
    as_string_init(&s, (char *) name, false);
    as_val  * v = as_map_get(m, (as_val *) &s);
    as_string_destroy(&s);
    return v;
}

int map_rec_set(const as_rec * r, const char * name, const as_val * value) {
    as_map * m = (as_map *) as_rec_source(r);
    return as_map_set(m, (as_val *) as_string_new(strdup(name),true), (as_val *) value);
}

const as_rec_hooks map_rec_hooks = {
    .get        = map_rec_get,
    .set        = map_rec_set,
    .destroy    = NULL,
    .remove     = NULL,
    .ttl        = NULL,
    .gen        = NULL,
    .hash       = NULL
};

as_rec * map_rec_new() {
    return as_rec_new(as_hashmap_new(32), &map_rec_hooks);
}



uint32_t rec_stream_seed = 0;
uint32_t rec_stream_count = 0;
const uint32_t rec_stream_max = 1000 * 100;

as_val * rec_stream_read(const as_stream * s) {

    if ( rec_stream_count == rec_stream_max ) {
        return AS_STREAM_END;
    }

    rec_stream_count++;

    int i = rand_r(&rec_stream_seed);
    as_rec * r = map_rec_new();

    as_rec_set(r, "campaign", (as_val *) as_integer_new(i % 10));
    as_rec_set(r, "views", (as_val *) as_integer_new(i % 1000));

    return (as_val *) r;
}

const as_stream_hooks rec_stream_hooks = {
    .read   = rec_stream_read,
    .write  = NULL
};

as_stream * rec_stream_new() {
    return as_stream_new(NULL, &rec_stream_hooks);
}


const uint32_t integer_stream_max = 1000;

as_val * integer_stream_read(const as_stream * s) {
    uint32_t * i = (uint32_t *) as_stream_source(s);

    if ( integer_stream_max == *i ) {
        return AS_STREAM_END;
    }

    return (as_val *) as_integer_new((*i)++);
}

const as_stream_hooks integer_stream_hooks = {
    .read   = integer_stream_read,
    .write  = NULL
};

as_stream * integer_stream_new(uint32_t * i) {
    return as_stream_new(i, &integer_stream_hooks);
}




as_stream_status list_stream_write(const as_stream * s, const as_val * v) {
    as_list * l = (as_list *) as_stream_source(s);
    as_list_append(l, (as_val *) v);
    return AS_STREAM_OK;
}

const as_stream_hooks list_stream_hooks = {
    .read   = NULL,
    .write  = list_stream_write
};

as_stream * list_stream_new(as_list * l) {
    return as_stream_new(l, &list_stream_hooks);
}




/******************************************************************************
 * TEST CASES
 *****************************************************************************/

uint32_t stream_pipe(as_stream * istream, as_stream * ostream) {
    as_val * v = as_stream_read(istream);
    if ( v != AS_STREAM_END ) {
        as_stream_write(ostream, v);
        return stream_pipe(istream, ostream) + 1;
    }
    else {
        return 0;
    }
}

TEST( aggr_streams_ints, "piping ints from stream a to stream b" ) {

    uint32_t i = 0;
    as_stream * istream = integer_stream_new(&i);

    as_list * l = as_linkedlist_new(NULL,NULL);
    as_stream * ostream = list_stream_new(l);

    uint32_t count = stream_pipe(istream, ostream);

    assert_int_eq( count, integer_stream_max);
    assert_int_eq( as_list_size(l), count);
}

TEST( aggr_streams_recs, "piping recs from stream a to stream b" ) {

    as_stream * istream = rec_stream_new();

    as_list * l = as_arraylist_new(rec_stream_max,100);
    as_stream * ostream = list_stream_new(l);

    uint32_t count = stream_pipe(istream, ostream);

    assert_int_eq( count, rec_stream_max);
    assert_int_eq( as_list_size(l), count);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( aggr_streams, "as_stream" ) {
    suite_add( aggr_streams_ints );
    suite_add( aggr_streams_recs );
}