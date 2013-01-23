include project/build.makefile

ifndef MSGPACK_PATH
MSGPACK_PATH = modules/msgpack
endif

CFLAGS 	= -g -O3 -std=gnu99 -Wall -fPIC -fno-common -fno-strict-aliasing -finline-functions -Winline -march=nocona -DMARCH_$(ARCH) -DMEM_COUNT=1
LDFLAGS = -Wall -Winline -rdynamic 

INC_PATH += modules/common/$(TARGET_INCL)
INC_PATH += $(MSGPACK_PATH)/src

as_types =
as_types += as_module.o
as_types += as_buffer.o
as_types += as_nil.o
as_types += as_boolean.o
as_types += as_integer.o
as_types += as_string.o
as_types += as_list.o
as_types += as_map.o
as_types += as_rec.o
as_types += as_pair.o

as_types += as_linkedlist.o
as_types += as_arraylist.o
as_types += as_hashmap.o

as_types += as_iterator.o
as_types += as_stream.o
as_types += as_result.o
as_types += as_aerospike.o
as_types += as_serializer.o

as_types += as_msgpack.o

mod_lua =
mod_lua += mod_lua.o
mod_lua += mod_lua_reg.o
mod_lua += mod_lua_aerospike.o
mod_lua += mod_lua_record.o
mod_lua += mod_lua_iterator.o
mod_lua += mod_lua_list.o
mod_lua += mod_lua_map.o
mod_lua += mod_lua_stream.o
mod_lua += mod_lua_val.o
mod_lua += mod_lua_config.o

test_o =  test.o
test_o += $(as_types) $(as_module) $(mod_lua)

val_test_o =  val_test.o
val_test_o += $(as_types)

##
## MAIN
##

all: libmod_lua.a libmod_lua.so

libtypes.o: | common $(TARGET_OBJ) $(call objects, $(as_types))

libmod_lua.o: $(call objects, $(as_types) $(mod_lua))

libmod_lua.so: | common msgpack libmod_lua.o $(TARGET_LIB) 
	$(call library, $(empty), $(empty), lua, $(empty), $(TARGET_OBJ)/*.o)

libmod_lua.a: | common msgpack libmod_lua.o $(TARGET_LIB) 
	$(call archive, $(empty), $(empty), $(empty), $(empty), $(TARGET_OBJ)/*.o)

##
## SUB-MODULES
##

common: 
	make -C modules/common prepare MEM_COUNT=1

common-lib: 
	make -C modules/common all MEM_COUNT=1

modules/msgpack/Makefile: 
	cd $(MSGPACK_PATH) && ./configure

msgpack: modules/msgpack/Makefile

msgpack-lib: modules/msgpack/Makefile
	cd $(MSGPACK_PATH) && make
##
## TEST
##

test_flags = $(TARGET_LIB)/libmod_lua.a  modules/common/$(TARGET_LIB)/libcf-shared.a modules/common/$(TARGET_LIB)/libcf-client.a $(MSGPACK_PATH)/src/.libs/libmsgpack.a 

record_udf: $(SOURCE_TEST)/record_udf.c | $(TARGET_BIN) libmod_lua.a common-lib msgpack-lib
	$(call executable, $(empty), $(empty), lua, $(empty), $(test_flags))

hashmap_test: $(SOURCE_TEST)/hashmap_test.c | $(TARGET_BIN) libmod_lua.a common-lib msgpack-lib
	$(call executable, $(empty), $(empty), $(empty), $(empty), $(test_flags))

linkedlist_test: $(SOURCE_TEST)/linkedlist_test.c | $(TARGET_BIN) libmod_lua.a common-lib msgpack-lib
	$(call executable, $(empty), $(empty), $(empty), $(empty), $(test_flags))

arraylist_test: $(SOURCE_TEST)/arraylist_test.c | $(TARGET_BIN) libmod_lua.a common-lib msgpack-lib
	$(call executable, $(empty), $(empty), $(empty), $(empty), $(test_flags))

msgpack_test: $(SOURCE_TEST)/msgpack_test.c | $(TARGET_BIN) libmod_lua.a common-lib msgpack-lib
	$(call executable, $(empty), $(empty), $(empty), $(empty), $(test_flags))

test: msgpack_test hashmap_test linkedlist_test arraylist_test record_udf
