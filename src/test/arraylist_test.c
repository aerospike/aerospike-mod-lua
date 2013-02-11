
#include "as_arraylist.h"
#include "as_string.h"
#include "as_val.h"

#include "test.h"

void print_val(char *msg, as_val *v) {
    if (v == 0) {
        LOG("%s: val: NULL",msg);
    } else {
        char *c = as_val_tostring(v);
        LOG("%s: val: %s", msg, c);
        free(c);
    }   
}

int main ( int argc, char ** argv ) {

    char *c;

    LOG("BEGIN");

    as_list * l = as_arraylist_new(1,1);

    LOG("size %d",as_list_size(l));

    as_list_append(l, (as_val *) as_string_new(strdup("a"), true));

    LOG("size %d",as_list_size(l));
    print_val("should be a", as_list_get(l,0));

    as_list_append(l, (as_val *) as_string_new("b", false));

    LOG("size %d",as_list_size(l));
    print_val("should be b", as_list_get(l,0));

    as_list_append(l, (as_val *) as_string_new(strdup("c"), true));

    LOG("size %d",as_list_size(l));
    print_val("should be c", as_list_get(l,0));

    as_list_prepend(l, (as_val *) as_string_new("z", false));

    LOG("size %d",as_list_size(l));
    print_val("should be z", as_list_get(l,0));

    as_list_set(l, 1, (as_val *) as_string_new(strdup("y"), true));
    print_val("should be y", as_list_get(l,0));

    LOG("size %d",as_list_size(l));

    as_iterator * i = as_list_iterator_new(l);
    while ( as_iterator_has_next(i) ) {
        const as_val * v = as_iterator_next(i);
        print_val("iterator should be y", as_list_get(l,1));
    }
    as_iterator_destroy(i);

    c = as_val_tostring(as_list_get(l,0));
    LOG("get 1 = %s", c);
    free(c);

    c = as_val_tostring(as_list_get(l,3));
    LOG("get 3 = %s", c);
    free(c);

    c = as_val_tostring(as_list_get(l,5));
    LOG("get 5= %s", c);
    free(c);

    as_val_destroy(l);

    LOG("END");
    return 0;
}