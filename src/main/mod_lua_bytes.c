#include "mod_lua_val.h"
#include "mod_lua_bytes.h"
#include "mod_lua_iterator.h"
#include "mod_lua_reg.h"

#include "as_val.h"
#include "internal.h"

#include <arpa/inet.h> // byteswap
#include <endian.h>


/*******************************************************************************
 * MACROS
 ******************************************************************************/

#define OBJECT_NAME "bytes"
#define CLASS_NAME  "Bytes"

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

as_bytes * mod_lua_tobytes(lua_State * l, int index) {
    mod_lua_box * box = mod_lua_tobox(l, index, CLASS_NAME);
    return (as_bytes *) mod_lua_box_value(box);
}

as_bytes * mod_lua_pushbytes(lua_State * l, as_bytes * b) {
    mod_lua_box * box = mod_lua_pushbox(l, MOD_LUA_SCOPE_LUA, b, CLASS_NAME);
    return (as_bytes *) mod_lua_box_value(box);
}

static as_bytes * mod_lua_checkbytes(lua_State * l, int index) {
    mod_lua_box * box = mod_lua_checkbox(l, index, CLASS_NAME);
    return (as_bytes *) mod_lua_box_value(box);
}

static int mod_lua_bytes_gc(lua_State * l) {
    mod_lua_freebox(l, 1, CLASS_NAME);
    return 0;
}


static int mod_lua_bytes_len(lua_State * l) {

    as_bytes *  b     = mod_lua_checkbytes(l, 1);
    uint32_t    size    = as_bytes_len(b);
    lua_pushinteger(l, size);
    return 1;
}

static int mod_lua_bytes_new(lua_State * l) {

    as_bytes * b = 0;

    int n_args = lua_gettop(l); // number of elements passed

    if ( n_args == 1) {

        b = as_bytes_empty_new(0 /*len*/);

        mod_lua_pushbytes(l, b);
    }

    else if ( n_args == 2 ) {

        if (lua_type(l, 2) == LUA_TNUMBER) {
            lua_Integer n = luaL_optinteger(l, 2, 0);
            b = as_bytes_empty_new(n);

        }
        else {
            // fprintf(stderr, "+=+=+= mod_lua_bytes_new: arg is type %d\n",lua_type(l, 2));
        }

    }

    if (!b) {
        // failure, nothing created
        lua_pushnil(l);
        return(0);
    }

    mod_lua_pushbytes(l, b);
    return(1);
}

static int mod_lua_bytes_tostring(lua_State * l) {

    mod_lua_box *   box     = mod_lua_checkbox(l, 1, CLASS_NAME);
    as_val *        val     = mod_lua_box_value(box);
    char *          str     = NULL;

    if ( val ) {
        str = as_val_tostring(val);
    }

    if ( str ) {
        lua_pushstring(l, str);
        free(str);
    }
    else {
        lua_pushstring(l, "Bytes()");
    }

    return 1;
}

// get an index value

static int mod_lua_bytes_index(lua_State * l) {

    as_bytes *  b     = mod_lua_checkbytes(l, 1);

    int offset = (int) luaL_optinteger(l, 2, 0); 

    uint8_t buf = 0;
    as_bytes_get(b, offset, &buf, sizeof(buf));

    lua_pushinteger(l, buf);
    return 1;
}

// set a new index value

static int mod_lua_bytes_newindex(lua_State * l) {

    as_bytes * b = mod_lua_checkbytes(l, 1);

    int offset = (int) luaL_optinteger(l, 2, 0); 

    int value = (int) luaL_optinteger(l, 3, 0);

    uint8_t buf = (uint8_t) value;

    if (0 != as_bytes_set(b, offset, &buf, 1)) {
        lua_pushnil(l);
        return(0);
    }

    // ???
    return(0);
}



// put: offset, int

static int mod_lua_bytes_put_int16(lua_State * l) {

    as_bytes * b = mod_lua_checkbytes(l, 1);

    int n_args = lua_gettop(l); 

    if (n_args != 3) {
        lua_pushnil(l);
        return(0);
    }

    int offset = (int) luaL_optinteger(l, 2, 0); 
    int value = (int) luaL_optinteger(l, 3, 0); 

    uint16_t buf = htons(value);

    if (0 != as_bytes_set(b, offset, (uint8_t *) &buf, 2)) {
        lua_pushnil(l);
        return(0);
    }

    return 1;
}

static int mod_lua_bytes_put_int32(lua_State * l) {

    as_bytes * b = mod_lua_checkbytes(l, 1);

    int n_args = lua_gettop(l); 

    if (n_args != 3) {
        lua_pushnil(l);
        return(0);
    }

    int offset = (int) luaL_optinteger(l, 2, 0); 
    int value = (int) luaL_optinteger(l, 3, 0); 

    uint32_t buf = htonl(value);

    if (0 != as_bytes_set(b, offset, (uint8_t *) &buf, sizeof(buf))) {
        lua_pushnil(l);
        return(0);
    }

    return 1;
}

static int mod_lua_bytes_put_int64(lua_State * l) {

    as_bytes * b = mod_lua_checkbytes(l, 1);

    int n_args = lua_gettop(l); 

    if (n_args != 3) {
        lua_pushnil(l);
        return(0);
    }

    int offset = (int) luaL_optinteger(l, 2, 0); 
    uint64_t value = (uint64_t) luaL_optinteger(l, 3, 0); 

    uint64_t buf = be64toh(value);

    if (0 != as_bytes_set(b, offset, (uint8_t *) &buf, sizeof(buf))) {
        lua_pushnil(l);
        return(0);
    }

    return 1;
}

static int mod_lua_bytes_put_string(lua_State * l) {

    as_bytes * b = mod_lua_checkbytes(l, 1);

    int n_args = lua_gettop(l); 

    if (n_args != 3) {
        lua_pushnil(l);
        return(0);
    }

    int offset = (int) luaL_optinteger(l, 2, 0); 
    const char *    value = luaL_optstring(l, 3, NULL);
    int     value_len = strlen(value);

    if (0 != as_bytes_set(b, offset, (uint8_t *) value, value_len)) {
        lua_pushnil(l);
        return(0);
    }

    return 1;
}

static int mod_lua_bytes_put_bytes(lua_State * l) {

    as_bytes * b = mod_lua_checkbytes(l, 1);

    int n_args = lua_gettop(l); 

    if (n_args != 3) {
        lua_pushnil(l);
        return(0);
    }

    int offset = (int) luaL_optinteger(l, 2, 0); 

    as_bytes * v = mod_lua_checkbytes(l, 3);

    uint8_t *buf = as_bytes_tobytes(v);
    int buf_len = as_bytes_len(v);

    if (0 != as_bytes_set(b, offset, (uint8_t *) &buf, buf_len)) {
        lua_pushnil(l);
        return(0);
    }

    return 1;
}

// get: offset, int

static int mod_lua_bytes_get_int16(lua_State * l) {

    as_bytes * b = mod_lua_checkbytes(l, 1);

    int n_args = lua_gettop(l); 
    if (n_args != 2) {
        lua_pushnil(l);
        return(0);
    }

    int offset = (int) luaL_optinteger(l, 2, 0); 

    uint16_t buf;

    if (0 != as_bytes_get(b, offset, (uint8_t *) &buf, sizeof(buf))) {
        lua_pushnil(l);
        return(0);
    }

    int result = ntohs(buf);
    lua_pushinteger(l, result);
    return 1;
}

static int mod_lua_bytes_get_int32(lua_State * l) {

    as_bytes * b = mod_lua_checkbytes(l, 1);

    int n_args = lua_gettop(l); 
    if (n_args != 2) {
        lua_pushnil(l);
        return(0);
    }

    int offset = (int) luaL_optinteger(l, 2, 0); 

    uint32_t buf;

    if (0 != as_bytes_get(b, offset, (uint8_t *) &buf, sizeof(buf))) {
        lua_pushnil(l);
        return(0);
    }

    int result = ntohl(buf);
    lua_pushinteger(l, result);
    return 1;
}

static int mod_lua_bytes_get_int64(lua_State * l) {

    as_bytes * b = mod_lua_checkbytes(l, 1);

    int n_args = lua_gettop(l); 
    if (n_args != 2) {
        lua_pushnil(l);
        return(0);
    }

    int offset = (int) luaL_optinteger(l, 2, 0); 

    uint64_t buf;

    if (0 != as_bytes_get(b, offset, (uint8_t *) &buf, sizeof(buf))) {
        lua_pushnil(l);
        return(0);
    }

    int result = (int) be64toh(buf);
    lua_pushinteger(l, result);
    return 1;
}

static int mod_lua_bytes_get_string(lua_State * l) {

    as_bytes * b = mod_lua_checkbytes(l, 1);

    int n_args = lua_gettop(l); 
    if (n_args != 3) {
        lua_pushnil(l);
        return(0);
    }

    int offset = (int) luaL_optinteger(l, 2, 0); 
    int len = (int) luaL_optinteger(l, 3, 0); 

    uint8_t * buf = (uint8_t *) malloc(len+1);


    if (0 != as_bytes_get(b, offset, (uint8_t *) buf, len)) {
        lua_pushnil(l);
        return(0);
    }
    buf[len] = 0;

    lua_pushlstring(l, buf, len);
    return 1;
}

static int mod_lua_bytes_get_bytes(lua_State * l) {

    as_bytes * b = mod_lua_checkbytes(l, 1);

    int n_args = lua_gettop(l); 
    if (n_args != 2) {
        lua_pushnil(l);
        return(0);
    }

    int offset = (int) luaL_optinteger(l, 2, 0); 
    int len = (int) luaL_optinteger(l, 3, 0); 

    // slice is in offset / offset
    as_bytes *slice = as_bytes_slice_new( b , offset, offset+len);

    if (!slice) {
        lua_pushnil(l);
        return(0);
    }

    mod_lua_pushbytes(l, slice);
    return 1;
}

/******************************************************************************
 * OBJECT TABLE
 *****************************************************************************/

static const luaL_reg bytes_object_table[] = {
    {"size",            mod_lua_bytes_len},
    {"tostring",        mod_lua_bytes_tostring},

    {"put_int16",       mod_lua_bytes_put_int16},
    {"put_int32",       mod_lua_bytes_put_int32},
    {"put_int64",       mod_lua_bytes_put_int64},
    {"put_string",      mod_lua_bytes_put_string},
    {"put_bytes",       mod_lua_bytes_put_bytes},

    {"get_int16",       mod_lua_bytes_get_int16},
    {"get_int32",       mod_lua_bytes_get_int32},
    {"get_int64",       mod_lua_bytes_get_int64},
    {"get_string",      mod_lua_bytes_get_string},
    {"get_bytes",       mod_lua_bytes_get_bytes},

//    {"type",            mod_lua_bytes_get_type},
//    {"set_type",        mod_lua_bytes_set_type},
//    {"get_type",        mod_lua_bytes_set_type},

//    {"append",          mod_lua_bytes_append},
//    {"delete",          mod_lua_bytes_delete}

    {0, 0}
};

static const luaL_reg bytes_object_metatable[] = {
    {"__call",          mod_lua_bytes_new},
    {0, 0}
};

/******************************************************************************
 * CLASS TABLE
 *****************************************************************************/

static const luaL_reg bytes_class_table[] = {
    {"putX",            mod_lua_bytes_tostring},
    {0, 0}
};

static const luaL_reg bytes_class_metatable[] = {
    {"__index",         mod_lua_bytes_index},
    {"__newindex",      mod_lua_bytes_newindex},
    {"__len",           mod_lua_bytes_len},
    {"__tostring",      mod_lua_bytes_tostring},
    {"__gc",            mod_lua_bytes_gc},
    {0, 0}
};

/******************************************************************************
 * REGISTER
 *****************************************************************************/

int mod_lua_bytes_register(lua_State * l) {
    mod_lua_reg_object(l, OBJECT_NAME, bytes_object_table, bytes_object_metatable);
    mod_lua_reg_class(l, CLASS_NAME, NULL, bytes_class_metatable);
    return 1;
}
