
#include "../test.h"
#include <aerospike/as_arraylist.h>
#include <aerospike/as_linkedlist.h>
#include <aerospike/as_list.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_stream.h>
#include <aerospike/as_string.h>
#include <aerospike/as_rec.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <limits.h>
#include <stdlib.h>


#include "../util/producer_stream.h"
#include "../util/consumer_stream.h"

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static uint32_t stream_pipe(as_stream * istream, as_stream * ostream) {
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

TEST( stream_basics_ints, "piping ints from stream a to stream b" ) {

    uint32_t limit = 100;
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

    uint32_t count = stream_pipe(istream, ostream);

    assert_int_eq( produced, consumed);
    assert_int_eq( limit, produced);
    assert_int_eq( count, consumed);

    as_stream_destroy(istream);
    as_stream_destroy(ostream);
}

TEST( stream_basics_recs, "piping recs from stream a to stream b" ) {

    uint32_t limit = 100;
    uint32_t produced = 0;
    uint32_t consumed = 0;

    as_val * produce() {
        if ( produced >= limit ) return AS_STREAM_END;
        produced++;

        as_map * m = (as_map *) as_hashmap_new(4);
        as_map_set(m, (as_val *) as_string_new(strdup("id"),true), (as_val *) as_integer_new(produced));
        as_map_set(m, (as_val *) as_string_new(strdup("campaign"),true), (as_val *) as_integer_new(produced % 10));
        as_map_set(m, (as_val *) as_string_new(strdup("views"),true), (as_val *) as_integer_new(produced * 2919 % 1000));

        return (as_val *) map_rec_new(m);
    }

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) consumed++;
        as_val_destroy(v);
        return AS_STREAM_OK;
    }

    as_stream * istream = producer_stream_new(produce);
    as_stream * ostream = consumer_stream_new(consume);

    uint32_t count = stream_pipe(istream, ostream);

    assert_int_eq( produced, consumed);
    assert_int_eq( limit, produced);
    assert_int_eq( count, consumed);

    as_stream_destroy(istream);
    as_stream_destroy(ostream);
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

SUITE( stream_basics, "stream tests" ) {
    suite_before( before );
    suite_after( after );
    
    suite_add( stream_basics_ints );
    suite_add( stream_basics_recs );
}