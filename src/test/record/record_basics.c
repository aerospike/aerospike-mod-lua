
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