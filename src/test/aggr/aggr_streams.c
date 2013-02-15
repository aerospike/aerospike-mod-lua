
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
    // as_map * m = (as_map *) as_rec_source(r);
    // as_string s;
    // as_string_init(&s, (char *) name, false);
    // as_val  * v = as_map_get(m, (as_val *) &s);
    // as_string_destroy(&s);
    // return v;
    return (as_val *) as_integer_new(1);
}

int map_rec_set(const as_rec * r, const char * name, const as_val * value) {
    // as_map * m = (as_map *) as_rec_source(r);
    // return as_map_set(m, (as_val *) as_string_new(strdup(name),true), (as_val *) value);
    return 0;
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
    return as_rec_new(NULL, &map_rec_hooks);
}


typedef struct {
    uint32_t pos;
    uint32_t end;
} range;


uint32_t rec_stream_seed = 0;
uint32_t rec_stream_count = 0;
uint32_t const rec_stream_max = 1000 * 100;

as_val * rec_stream_read(const as_stream * s) {
    range * r = (range *) as_stream_source(s);

    if ( r->pos > r->end ) {
        return AS_STREAM_END;
    }

    as_rec * rec = map_rec_new();
    // as_rec_set(rec, "campaign", (as_val *) as_integer_new(r->pos % 10));
    // as_rec_set(rec, "views", (as_val *) as_integer_new(r->pos * 123 % 1000));

    return (as_val *) rec;
}

const as_stream_hooks rec_stream_hooks = {
    .read   = rec_stream_read,
    .write  = NULL
};

as_stream * rec_stream_new(uint32_t end) {
    range * r = (range *) malloc(sizeof(range));
    r->pos = 1;
    r->end = end;
    return as_stream_new(r, &rec_stream_hooks);
}



as_val * integer_stream_read(const as_stream * s) {
    range * r = (range *) as_stream_source(s);

    if ( r->pos > r->end ) {
        return AS_STREAM_END;
    }

    return (as_val *) as_integer_new(r->pos++);
}

const as_stream_hooks integer_stream_hooks = {
    .read   = integer_stream_read,
    .write  = NULL
};

as_stream * integer_stream_new(uint32_t start, uint32_t end) {
    range * r = (range *) malloc(sizeof(range));
    r->pos = start;
    r->end = end;
    return as_stream_new(r, &integer_stream_hooks);
}




as_stream_status list_stream_write(const as_stream * s, const as_val * v) {
    as_list * l = (as_list *) as_stream_source(s);
    if ( v != NULL ) {
        as_list_append(l, (as_val *) v);
    }
    return AS_STREAM_OK;
}

const as_stream_hooks list_stream_hooks = {
    .read   = NULL,
    .write  = list_stream_write
};

as_stream * list_stream_new(as_list * l) {
    return as_stream_new(l, &list_stream_hooks);
}


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


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( aggr_streams_ints, "piping ints from stream a to stream b" ) {

    as_stream * istream = integer_stream_new(1,100);

    as_list * l = as_linkedlist_new(NULL,NULL);
    as_stream * ostream = list_stream_new(l);

    uint32_t count = stream_pipe(istream, ostream);

    assert_int_eq( count, 100);
    assert_int_eq( as_list_size(l), count);
}

TEST( aggr_streams_recs, "piping recs from stream a to stream b" ) {

    as_stream * istream = rec_stream_new(10);

    as_list * l = as_arraylist_new(10,100);
    as_stream * ostream = list_stream_new(l);

    uint32_t count = stream_pipe(istream, ostream);

    assert_int_eq( count, rec_stream_max);
    assert_int_eq( as_list_size(l), count);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite) {
    return true;
}

static bool after(atf_suite * suite) {
    return true;
}

SUITE( aggr_streams, "as_stream" ) {
    suite_before( before );
    suite_after( after );
    
    suite_add( aggr_streams_ints );
    suite_add( aggr_streams_recs );
}