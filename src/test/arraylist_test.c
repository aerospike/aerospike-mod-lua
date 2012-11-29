#include "test.h"
#include "as_arraylist.h"
#include "as_string.h"


int main ( int argc, char ** argv ) {

    log("BEGIN");

    as_list * l = as_arraylist_new(1,1);

    log("size %d",as_list_size(l));

    as_list_append(l, (as_val *) as_string_new("a"));

    log("size %d",as_list_size(l));

    as_list_append(l, (as_val *) as_string_new("b"));

    log("size %d",as_list_size(l));

    as_list_append(l, (as_val *) as_string_new("c"));

    log("size %d",as_list_size(l));

    as_list_prepend(l, (as_val *) as_string_new("z"));

    log("size %d",as_list_size(l));

    as_list_set(l, 1, (as_val *) as_string_new("y"));

    log("size %d",as_list_size(l));

    as_iterator * i = as_list_iterator(l);

    while ( as_iterator_has_next(i) ) {
        as_val * v = as_iterator_next(i);
        log("val %s", as_string_tostring((as_string *) v));
    }

    log("get 1 = %s", as_string_tostring((as_string *) as_list_get(l,1)));
    log("get 3 = %s", as_string_tostring((as_string *) as_list_get(l,3)));
    log("get 5 = %s", as_string_tostring((as_string *) as_list_get(l,5)));

    log("END");
    return 0;
}