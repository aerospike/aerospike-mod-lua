#include "as_val.h"

#include "as_hashmap.h"
#include "as_string.h"
#include "as_integer.h"

#include "test.h" // this redefines LOG so it becomes printf, keep it last

void print_val(char *msg, as_val *v) {
    char *c = as_val_tostring(v);
    LOG("%s: val: %s", msg, c);
    free(c);
}

int main ( int argc, char ** argv ) {

    LOG("BEGIN!");

    as_map *    m   = as_hashmap_new(3);
    as_string * a   = as_string_new("a", false);
    as_string * b   = as_string_new("b", false);
    as_string * c   = as_string_new("c", false);
    as_val *    v   = NULL;
    char * s = NULL;

    as_val_reserve(a);
    as_val_reserve(b);
    as_map_set(m, as_string_toval(a), as_string_toval(b));
    as_val_reserve(b);
    as_val_reserve(c);
    as_map_set(m, as_string_toval(b), as_string_toval(c));
    as_val_reserve(c);
    as_val_reserve(a);
    as_map_set(m, as_string_toval(c), as_string_toval(a));
    
    print_val("a == ", as_map_get(m, (as_val *) a));
    print_val("b == ", as_map_get(m, (as_val *) b));
    print_val("c == ", as_map_get(m, (as_val *) c));

    LOG("test...");
    
    // Get and print the value at a
    as_string a2;
    as_string_init(&a2, "a", false);
    v = as_map_get(m, (as_val *) &a2);
    print_val("a == ", v);
    as_val_destroy(&a2);

    // get and print the value at b
    as_string b2;
    as_string_init(&b2, "b", false);
    v = as_map_get(m, (as_val *) &b2);
    print_val("b == ", v);
    as_val_destroy(&b2);

    // get and print the value at c
    as_string c2;
    as_string_init(&c2, "a", false);
    v = as_map_get(m, (as_val *) &c2);
    print_val("c == ", v);
    as_val_destroy(&c2);

    LOG("update...");
    as_val_reserve(a);
    as_val_reserve(c);
    as_map_set(m, as_string_toval(a), as_string_toval(c));
    
    // Get and print the value at a
    v = as_map_get(m, (as_val *) a);
    print_val("a == ",v);

    // get and print the value at b
    v = as_map_get(m, (as_val *) b);
    print_val("b == ",v);

    // get and print the value at c
    v = as_map_get(m, (as_val *) c);
    print_val("c == ",v);

    // get a non-existant value
    as_integer ii;
    as_integer_init(&ii, 55);
    v = as_map_get(m, (as_val *) &ii);
    print_val("NULL = ",v);

    LOG("iterate...");

    as_iterator i;
    as_map_iterator_init(&i, m);
    while ( as_iterator_has_next(&i) ) {
        v = as_iterator_next(&i);
        print_val("iterating: ",v);
    }
    as_iterator_destroy(&i);

    print_val("map: ", m);

    // should be able to destroy even though there's an element in the table
    as_val_destroy(a);
    as_val_destroy(b);

    as_val_destroy(m);

    // and some destroyed after
    as_val_destroy(c);

    LOG("END");
    return 0;
}
