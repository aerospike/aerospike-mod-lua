#include "test.h"
#include "as_hashmap.h"
#include "as_string.h"
#include "as_pair.h"


int main ( int argc, char ** argv ) {

    LOG("BEGIN!");

    as_map *    m   = as_hashmap_new(3);
    as_string * a   = as_string_new("a");
    as_string * b   = as_string_new("b");
    as_string * c   = as_string_new("c");
    as_val *    v   = NULL;

    as_map_set(m, as_string_toval(a), as_string_toval(b));
    as_map_set(m, as_string_toval(b), as_string_toval(c));
    as_map_set(m, as_string_toval(c), as_string_toval(a));
    
    LOG("a == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) a )));
    LOG("b == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) b )));
    LOG("c == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) c )));

    LOG("test...");
    
    LOG("a == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) as_string_new("a") )));
    LOG("b == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) as_string_new("b") )));
    LOG("c == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) as_string_new("c") )));

    LOG("update...");

    as_map_set(m, as_string_toval(a), as_string_toval(c));
    
    LOG("a == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) a )));
    LOG("b == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) b )));
    LOG("c == %s", as_string_tostring((as_string *) as_map_get(m, (as_val*) c )));

    LOG("iterate...");

    as_iterator * i  = as_map_iterator(m);
    while ( as_iterator_has_next(i) ) {
        as_val * v = as_iterator_next(i);

        LOG("%s",as_val_tostring(v));
    }
    as_iterator_free(i);

    LOG("print map");

    LOG("%s", as_val_tostring((as_val *) m));

    LOG("END");
    return 0;
}