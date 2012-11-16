include project/build.makefile

CFLAGS = -std=c99 -Wall 
CFLAGS = -std=gnu99 -Wall -fPIC -fno-strict-aliasing
#CFLAGS = -g -O3 -fno-common -fno-strict-aliasing -rdynamic  -Wall -D_FILE_OFFSET_BITS=64 -std=gnu99 -D_REENTRANT -D_GNU_SOURCE  -D MARCH_x86_64 -march=nocona  -DMEM_COUNT -MMD 
LDFLAGS = -fPIC


as_types = 	as_val.o
as_types += as_boolean.o
as_types += as_integer.o
as_types += as_list.o
as_types += as_map.o
as_types += as_string.o

as_types += as_rec.o
as_types += as_iterator.o
as_types += as_stream.o
as_types += as_result.o

as_types += as_aerospike.o

as_module = as_module.o

mod_lua =  mod_lua.o
mod_lua += mod_lua_aerospike.o
mod_lua += mod_lua_record.o
mod_lua += mod_lua_iterator.o
mod_lua += mod_lua_stream.o
mod_lua += mod_lua_val.o


test_o =  test.o
test_o += $(as_types) $(as_module) $(mod_lua)

val_test_o =  val_test.o
val_test_o += $(as_types)

test: $(call objects, $(test_o)) | $(TARGET_BIN) libmod_lua.so
	$(call build, $(empty), target/Linux-x86_64/lib,  mod_lua lua, $(empty))

libmod_lua.so: $(call objects, $(as_types) $(as_module) $(mod_lua)) | $(TARGET_LIB)
	$(call library, $(empty), $(empty), lua, $(empty))

mod_lua.a: $(call objects, $(as_types) $(as_module) $(mod_lua)) | $(TARGET_LIB)
	$(call archive, $(empty), $(empty), lua, $(empty))

all: app
