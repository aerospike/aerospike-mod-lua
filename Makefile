###############################################################################
include project/settings.makefile
###############################################################################

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

all: build prepare

.PHONY: prepare
prepare: $(TARGET_INCL)

.PHONY: build 
build: libmod_lua libas_types

.PHONY: build-clean
build-clean:
	@rm -rf $(TARGET_BIN)
	@rm -rf $(TARGET_LIB)

.PHONY: libas_types libas_types.a libas_types.so
libas_types: libas_types.a libas_types.so
libas_types.a: $(TARGET_LIB)/libas_types.a
libas_types.so: $(TARGET_LIB)/libas_types.so

.PHONY: libmod_lua libmod_lua.a libmod_lua.so
libmod_lua: libmod_lua.a libmod_lua.so
libmod_lua.a: $(TARGET_LIB)/libmod_lua.a
libmod_lua.so: $(TARGET_LIB)/libmod_lua.so

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_LIB)/libas_types.a $(TARGET_LIB)/libas_types.so: $(AS_TYPES:%=$(TARGET_OBJ)/%) | modules/common/$(TARGET_INCL)/*.h

$(TARGET_LIB)/libmod_lua.a $(TARGET_LIB)/libmod_lua.so: $(MOD_LUA:%=$(TARGET_OBJ)/%) $(AS_TYPES:%=$(TARGET_OBJ)/%) | modules/common/$(TARGET_INCL)/*.h

$(TARGET_INCL): $(SOURCE_INCL)/*.h 
	mkdir -p $(TARGET_INCL)
	cp -p $(SOURCE_INCL)/*.h $(TARGET_INCL)/.

###############################################################################
##  SUB-MODULES TARGETS                                                      ##
###############################################################################

.PHONY: modules
modules: modules/common modules/msgpack

.PHONY: modules-prepare
modules-prepare: modules/common/$(TARGET_INCL)/*.h

.PHONY: modules-clean
modules-clean: 
	$(MAKE) -e -C modules/common clean

##
## SUBMODULE: common
## 

modules/common/$(TARGET_LIB)/libcf-shared.a:
	$(MAKE) -e -C modules/common libcf-shared.a MEM_COUNT=1

modules/common/$(TARGET_LIB)/libcf-server.a:
	$(MAKE) -e -C modules/common libcf-server.a MEM_COUNT=1

modules/common/$(TARGET_INCL)/*.h:
	$(MAKE) -e -C modules/common prepare MEM_COUNT=1

.PHONY: modules/common
modules/common: modules/common/$(TARGET_LIB)/libcf-shared.a modules/common/$(TARGET_LIB)/libcf-server.a modules/common/$(TARGET_INCL)/*.h

##
## SUBMODULE: msgpack
## 

modules/msgpack/Makefile: 
	cd $(MSGPACK_PATH) && ./configure

modules/msgpack/src/.libs/libmsgpackc.a: modules/msgpack/Makefile
	cd $(MSGPACK_PATH) && make

.PHONY: modules/msgpack
modules/msgpack: modules/msgpack/src/.libs/libmsgpackc.a

###############################################################################
##  TEST TARGETS                                                      		 ##
###############################################################################

TEST_DEPS = $(TARGET_LIB)/libmod_lua.a modules/common/$(TARGET_LIB)/libcf-shared.a modules/common/$(TARGET_LIB)/libcf-client.a $(MSGPACK_PATH)/src/.libs/libmsgpack.a 

.PHONY: record_udf
record_udf: $(SOURCE_TEST)/record_udf.c $(TEST_DEPS)
	$(executable)

.PHONY: hashmap_test
hashmap_test: $(SOURCE_TEST)/hashmap_test.c $(TEST_DEPS)
	$(executable)

.PHONY: linkedlist_test
linkedlist_test: $(SOURCE_TEST)/linkedlist_test.c $(TEST_DEPS)
	$(executable)

.PHONY: arraylist_test
arraylist_test: $(SOURCE_TEST)/arraylist_test.c $(TEST_DEPS)
	$(executable)

.PHONY: msgpack_test
msgpack_test: $(SOURCE_TEST)/msgpack_test.c $(TEST_DEPS)
	$(executable)

.PHONY: test
test: msgpack_test hashmap_test linkedlist_test arraylist_test record_udf

###############################################################################
include project/rules.makefile
###############################################################################