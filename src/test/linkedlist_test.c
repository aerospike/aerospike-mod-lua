
#include "as_val.h"

#include "as_linkedlist.h"
#include "as_string.h"

#include "test.h"

void print_val(const char *msg, const as_val *v) {
    char *c = as_val_tostring(v);
    LOG("%s: val: %s", msg, c);
    free(c);
}

int main ( int argc, char ** argv ) {

    LOG("BEGIN");

    as_list * l = as_linkedlist_new(NULL,NULL);

    LOG("size %d",as_list_size(l));

    as_list_append(l, (as_val *) as_string_new("a", false));

    LOG("size %d",as_list_size(l));

    as_list_append(l, (as_val *) as_string_new("b", false));

    LOG("size %d",as_list_size(l));

    as_list_append(l, (as_val *) as_string_new("c", false));

    LOG("size %d",as_list_size(l));

    as_list_prepend(l, (as_val *) as_string_new("z", false));

    LOG("size %d",as_list_size(l));

    as_list_set(l, 1, (as_val *) as_string_new("y", false));

    LOG("size %d",as_list_size(l));

    as_iterator i;
    as_list_iterator_init(&i, l);
    while ( as_iterator_has_next(&i) ) {
        const as_val * v = as_iterator_next(&i);
        print_val("iterate: ",v);
    }
    as_iterator_destroy(&i);

    print_val("get 1 =",as_list_get(l,1));
    print_val("get 3 =",as_list_get(l,3));
    print_val("get 5 =",as_list_get(l,5));

    LOG("");
    LOG("Iterate empty list");
    
    as_list empty_list;
    as_linkedlist_init(&empty_list,NULL,NULL);
    as_iterator *ii = as_list_iterator_new(&empty_list);
    while ( as_iterator_has_next(ii) ) {
        const as_val * v = as_iterator_next(ii);
        print_val("empty iterate:",v);
    }
    as_iterator_destroy(ii);
    ii = 0;
    as_val_destroy((as_val *) &empty_list);

    LOG("");
    LOG("Iterate take(2) list");

    as_list * sub = as_list_take(l, 2);
    ii = as_list_iterator_new(sub);
    while ( as_iterator_has_next(ii) ) {
        const as_val * v = as_iterator_next(ii);
        print_val("take iterate:", v);
    }
    as_iterator_destroy(ii);
    ii = 0;
    as_val_destroy((as_val *) sub);

    LOG("");
    LOG("Iterate drop(2) list");

    sub = as_list_drop(l, 2);
    ii = as_list_iterator_new(sub);
    while ( as_iterator_has_next(ii) ) {
        const as_val * v = as_iterator_next(ii);
        print_val("drop iterate:",v);
    }
    as_iterator_destroy(ii);
    ii = NULL;
    as_val_destroy(sub);

    print_val("list print:",l);

    as_val_destroy(l);

    LOG("END");

    return 0;
}
