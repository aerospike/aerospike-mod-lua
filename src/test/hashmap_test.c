#include "test.h"
#include "as_hashmap.h"
#include "as_string.h"


int main ( int argc, char ** argv ) {

    log("BEGIN!");

    as_map *    m   = as_hashmap_new(64);
    as_string * a   = as_string_new("a");
    as_string * b   = as_string_new("b");
    as_string * c   = as_string_new("c");
    as_val *    v   = NULL;

    as_map_set(m, as_string_toval(a), as_string_toval(b));
    as_map_set(m, as_string_toval(b), as_string_toval(c));
    as_map_set(m, as_string_toval(c), as_string_toval(a));
    
    log("a == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) a )));
    log("b == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) b )));
    log("c == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) c )));

    log("test...")
    
    log("a == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) as_string_new("a") )));
    log("b == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) as_string_new("b") )));
    log("c == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) as_string_new("c") )));

    log("update...")

    as_map_set(m, as_string_toval(a), as_string_toval(c));
    
    log("a == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) a )));
    log("b == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) b )));
    log("c == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) c )));

    log("END");
    return 0;
}