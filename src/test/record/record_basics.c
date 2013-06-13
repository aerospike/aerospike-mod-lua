
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

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( record_basics_1, "echo" ) {

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

SUITE( record_basics, "record basics" ) {
    suite_before( before );
    suite_after( after );
    
    suite_add( record_basics_1 );
}