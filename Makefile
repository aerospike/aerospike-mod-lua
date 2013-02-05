include project/build.makefile

###############################################################################
##  FLAGS                                                                    ##
###############################################################################

ifndef MSGPACK_PATH
MSGPACK_PATH = modules/msgpack
endif

# CFLAGS 	= -g -O3 -std=gnu99 -Wall -fPIC -fno-common -fno-strict-aliasing -finline-functions -Winline -march=nocona -DMARCH_$(ARCH) -DMEM_COUNT=1
CFLAGS 	= -g -std=gnu99 -Wall -fPIC -fno-common -fno-strict-aliasing -finline-functions -Winline -march=nocona -DMARCH_$(ARCH) -DMEM_COUNT=1
LDFLAGS = -Wall -Winline -rdynamic 

INC_PATH += modules/common/$(TARGET_INCL)
# INC_PATH += modules/common/$(TARGET_INCL)
INC_PATH += $(MSGPACK_PATH)/src

MODULES := common msgpack

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
modules: $(MODULES:%=modules/%)

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
	$(call archive, $(empty), $(empty), $(empty), $(empty))

$(TARGET_LIB)/libas_types.so: $(AS_TYPES:%=$(TARGET_OBJ)/%) | $(TARGET_LIB) modules
	$(call library, $(empty), $(empty), $(empty), $(empty))

$(TARGET_LIB)/libmod_lua.a: $(MOD_LUA:%=$(TARGET_OBJ)/%) $(AS_TYPES:%=$(TARGET_OBJ)/%) | $(TARGET_LIB) modules
	$(call archive, $(empty), $(empty), $(empty), $(empty), $(empty))

$(TARGET_LIB)/libmod_lua.so: $(MOD_LUA:%=$(TARGET_OBJ)/%) $(AS_TYPES:%=$(TARGET_OBJ)/%) | $(TARGET_LIB) modules
	$(call library, $(empty), $(empty), lua pthread, $(empty), $(empty))

$(TARGET_INCL):
	mkdir -p $(TARGET_INCL)
	cp -p $(SOURCE_INCL)/*.h $(TARGET_INCL)/.

###############################################################################
##  SUB-MODULES TARGETS                                                      ##
###############################################################################

.PHONY: modules/common
modules/common:
	make -C modules/common all MEM_COUNT=1

modules/msgpack/Makefile: 
	cd $(MSGPACK_PATH) && ./configure

modules/msgpack/src/.libs/libmsgpackc.a: modules/msgpack/Makefile
	cd $(MSGPACK_PATH) && make

.PHONY: modules/msgpack
modules/msgpack: modules/msgpack/src/.libs/libmsgpackc.a

###############################################################################
##  TEST TARGETS                                                      		 ##
###############################################################################

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
