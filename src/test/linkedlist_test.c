#include "test.h"
#include "as_linkedlist.h"
#include "as_string.h"


int main ( int argc, char ** argv ) {

    LOG("BEGIN");

    as_linkedlist * ll = as_linkedlist_new(NULL,NULL);
    as_list * l = as_list_new(ll, &as_linkedlist_list);

    LOG("size %d",as_list_size(l));

    as_list_append(l, (as_val *) as_string_new("a"));

    LOG("size %d",as_list_size(l));

    as_list_append(l, (as_val *) as_string_new("b"));

    LOG("size %d",as_list_size(l));

    as_list_append(l, (as_val *) as_string_new("c"));

    LOG("size %d",as_list_size(l));

    as_list_prepend(l, (as_val *) as_string_new("z"));

    LOG("size %d",as_list_size(l));

    as_list_set(l, 1, (as_val *) as_string_new("y"));

    LOG("size %d",as_list_size(l));

    as_iterator * i = as_list_iterator(l);

    while ( as_iterator_has_next(i) ) {
        const as_val * v = as_iterator_next(i);
        LOG("val %s", as_string_tostring((as_string *) v));
    }
    as_iterator_free(i);
    i = NULL;

    LOG("get 1 = %s", as_string_tostring((as_string *) as_list_get(l,1)));
    LOG("get 3 = %s", as_string_tostring((as_string *) as_list_get(l,3)));
    LOG("get 5 = %s", as_string_tostring((as_string *) as_list_get(l,5)));

    LOG("");
    LOG("Iterate empty list");

    i = as_list_iterator(as_list_new(as_linkedlist_new(NULL,NULL),&as_linkedlist_list));
    while ( as_iterator_has_next(i) ) {
        const as_val * v = as_iterator_next(i);
        LOG("val %s", as_string_tostring((as_string *) v));
    }
    as_iterator_free(i);
    i = NULL;

    LOG("");
    LOG("Iterate take(2) list");

    as_list * sub = NULL;

    sub = as_list_take(l, 2);
    i = as_list_iterator(sub);
    while ( as_iterator_has_next(i) ) {
        const as_val * v = as_iterator_next(i);
        LOG("val %s", as_string_tostring((as_string *) v));
    }
    as_iterator_free(i);
    i = NULL;

    LOG("");
    LOG("Iterate drop(2) list");

    sub = as_list_drop(l, 2);
    i = as_list_iterator(sub);
    while ( as_iterator_has_next(i) ) {
        const as_val * v = as_iterator_next(i);
        LOG("val %s", as_string_tostring((as_string *) v));
    }
    as_iterator_free(i);
    i = NULL;

    LOG("END");



    return 0;
}