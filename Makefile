include project/build.makefile

###############################################################################
##  SETTING                                                                  ##
###############################################################################

ifndef MSGPACK_PATH
MSGPACK_PATH = modules/msgpack
endif

CFLAGS = -O3

CC_FLAGS = $(CFLAGS) -g -std=gnu99 -Wall -Winline -fPIC 
CC_FLAGS += -fno-common -fno-strict-aliasing -finline-functions 
CC_FLAGS += -march=nocona -DMARCH_$(ARCH) -DMEM_COUNT

LD_FLAGS = -Wall -Winline -rdynamic 

INC_PATH += modules/common/$(TARGET_INCL)
INC_PATH += $(MSGPACK_PATH)/src

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

AS_TYPES =
AS_TYPES += as_val.o
AS_TYPES += as_module.o
AS_TYPES += as_buffer.o
AS_TYPES += as_nil.o
AS_TYPES += as_boolean.o
AS_TYPES += as_integer.o
AS_TYPES += as_string.o
AS_TYPES += as_list.o
AS_TYPES += as_map.o
AS_TYPES += as_rec.o
AS_TYPES += as_pair.o
AS_TYPES += as_linkedlist.o
AS_TYPES += as_arraylist.o
AS_TYPES += as_hashmap.o
AS_TYPES += as_iterator.o
AS_TYPES += as_stream.o
AS_TYPES += as_result.o
AS_TYPES += as_aerospike.o
AS_TYPES += as_serializer.o
AS_TYPES += as_msgpack.o
AS_TYPES += internal.o


MOD_LUA =
MOD_LUA += mod_lua.o
MOD_LUA += mod_lua_reg.o
MOD_LUA += mod_lua_aerospike.o
MOD_LUA += mod_lua_record.o
MOD_LUA += mod_lua_iterator.o
MOD_LUA += mod_lua_list.o
MOD_LUA += mod_lua_map.o
MOD_LUA += mod_lua_stream.o
MOD_LUA += mod_lua_val.o
MOD_LUA += mod_lua_config.o


TEST =  test.o
TEST += $(as_types) $(as_module) $(mod_lua)


VAL_TEST = val_test.o
VAL_TEST += $(as_types)

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: modules build prepare

.PHONY: modules
modules: modules/common modules/msgpack

.PHONY: build 
build: libmod_lua.a libmod_lua.so libas_types.a libas_types.so

.PHONY: prepare
prepare: $(TARGET_INCL)

.PHONY: libas_types.so libas_types.a libmod_lua.so libmod_lua.a
libas_types.so: $(TARGET_LIB)/libas_types.so
libas_types.a: $(TARGET_LIB)/libas_types.a
libmod_lua.so: $(TARGET_LIB)/libmod_lua.so
libmod_lua.a: $(TARGET_LIB)/libmod_lua.a

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_LIB)/libas_types.a: $(AS_TYPES:%=$(TARGET_OBJ)/%) | $(TARGET_LIB) modules
	$(archive)

$(TARGET_LIB)/libas_types.so: $(AS_TYPES:%=$(TARGET_OBJ)/%) | $(TARGET_LIB) modules
	$(library)

$(TARGET_LIB)/libmod_lua.a: $(MOD_LUA:%=$(TARGET_OBJ)/%) $(AS_TYPES:%=$(TARGET_OBJ)/%) | $(TARGET_LIB) modules
	$(archive)

$(TARGET_LIB)/libmod_lua.so: $(MOD_LUA:%=$(TARGET_OBJ)/%) $(AS_TYPES:%=$(TARGET_OBJ)/%) | $(TARGET_LIB) modules
	$(library)

$(TARGET_INCL):
	mkdir -p $(TARGET_INCL)
	cp -p $(SOURCE_INCL)/*.h $(TARGET_INCL)/.

###############################################################################
##  SUB-MODULES TARGETS                                                      ##
###############################################################################

.PHONY: modules/common
modules/common:
	$(MAKE) -C $@ prepare

modules/msgpack/Makefile: 
	cd $(MSGPACK_PATH) && ./configure

modules/msgpack/src/.libs/libmsgpackc.a: modules/msgpack/Makefile
	cd $(MSGPACK_PATH) && make

.PHONY: modules/msgpack
modules/msgpack: modules/msgpack/src/.libs/libmsgpackc.a

###############################################################################
##  TEST TARGETS                                                      		 ##
###############################################################################

TEST_FLAGS = $(TARGET_LIB)/libmod_lua.a  modules/common/$(TARGET_LIB)/libcf-shared.a modules/common/$(TARGET_LIB)/libcf-client.a $(MSGPACK_PATH)/src/.libs/libmsgpack.a 

record_udf: LDFLAGS += $(TEST_FLAGS)
record_udf: $(SOURCE_TEST)/record_udf.c | $(TARGET_BIN) libmod_lua.a common-lib msgpack-lib
	$(executable)

hashmap_test: LDFLAGS += $(TEST_FLAGS)
hashmap_test: $(SOURCE_TEST)/hashmap_test.c | $(TARGET_BIN) libmod_lua.a common-lib msgpack-lib
	$(executable)

linkedlist_test: LDFLAGS += $(TEST_FLAGS)
linkedlist_test: $(SOURCE_TEST)/linkedlist_test.c | $(TARGET_BIN) libmod_lua.a common-lib msgpack-lib
	$(executable)

arraylist_test: LDFLAGS += $(TEST_FLAGS)
arraylist_test: $(SOURCE_TEST)/arraylist_test.c | $(TARGET_BIN) libmod_lua.a common-lib msgpack-lib
	$(executable)

msgpack_test: LDFLAGS += $(TEST_FLAGS)
msgpack_test: $(SOURCE_TEST)/msgpack_test.c | $(TARGET_BIN) libmod_lua.a common-lib msgpack-lib
	$(executable)

test: msgpack_test hashmap_test linkedlist_test arraylist_test record_udf
