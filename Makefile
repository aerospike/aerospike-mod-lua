###############################################################################
include project/settings.makefile
###############################################################################

###############################################################################
##  SETTING                                                                  ##
###############################################################################

ifndef MSGPACK_PATH
MSGPACK_PATH = modules/msgpack
endif

ifeq ($(DOPROFILE), 1)
CFLAGS = -O0
else 
CFLAGS = -O3
endif

CC_FLAGS = -g -std=gnu99 -Wall -Winline -fPIC 
CC_FLAGS += -fno-common -fno-strict-aliasing -finline-functions 
CC_FLAGS += -march=nocona -DMARCH_$(ARCH) -DMEM_COUNT

LD_FLAGS = -Wall -Winline -rdynamic
# -fPIC -lm

ifeq ($(DOPROFILE), 1)
CC_FLAGS += -pg -fprofile-arcs -ftest-coverage -g2
LD_FLAGS += -pg -fprofile-arcs -lgcov
endif



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


TEST = test.o
TEST += $(AS_TYPES) $(MOD_LUA)
# TEST += $(as_types) $(as_module) $(mod_lua)

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
	$(MAKE) -e -C modules/common libcf-shared.a

modules/common/$(TARGET_LIB)/libcf-server.a:
	$(MAKE) -e -C modules/common libcf-server.a

modules/common/$(TARGET_LIB)/libcf-client.a:
	$(MAKE) -e -C modules/common libcf-client.a

modules/common/$(TARGET_INCL)/*.h:
	$(MAKE) -e -C modules/common prepare

.PHONY: modules/common
modules/common: modules/common/$(TARGET_LIB)/libcf-shared.a modules/common/$(TARGET_LIB)/libcf-client.a modules/common/$(TARGET_INCL)/*.h
#modules/common: modules/common/$(TARGET_LIB)/libcf-shared.a  modules/common/$(TARGET_LIB)/libcf-server.a modules/common/$(TARGET_INCL)/*.h

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

TEST_CFLAGS =  -DMEM_COUNT=1
TEST_CFLAGS += -I$(TARGET_INCL)
TEST_CFLAGS += -Imodules/common/$(TARGET_INCL)

TEST_LDFLAGS = -lssl -lcrypto -llua -lpthread -lm -lrt 

TEST_DEPS =
TEST_DEPS += modules/common/$(TARGET_OBJ)/client/*.o 
TEST_DEPS += modules/common/$(TARGET_OBJ)/shared/*.o 
TEST_DEPS += $(MSGPACK_PATH)/src/.libs/libmsgpackc.a


TEST_TYPES = types
TEST_TYPES += types_integer
TEST_TYPES += types_string
TEST_TYPES += types_arraylist
TEST_TYPES += types_linkedlist
TEST_TYPES += types_hashmap

TEST_AGGR = aggr
TEST_AGGR += aggr_streams
TEST_AGGR += aggr_udf


.PHONY: test
test: test-build
	@$(TARGET_BIN)/test/types
	@$(TARGET_BIN)/test/aggr


.PHONY: test-build
test-build: test/aggr test/types
# test-build: test/types

.PHONY: test-clean
test-clean: 
	@rm -rf $(TARGET_BIN)/test
	@rm -rf $(TARGET_OBJ)/test


$(TARGET_OBJ)/test/%/%.o: CFLAGS = $(TEST_CFLAGS)
$(TARGET_OBJ)/test/%/%.o: LDFLAGS += $(TEST_LDFLAGS)
$(TARGET_OBJ)/test/%/%.o: $(SOURCE_TEST)/%/%.c
	$(object)

$(TARGET_OBJ)/test/%.o: CFLAGS = $(TEST_CFLAGS)
$(TARGET_OBJ)/test/%.o: LDFLAGS += $(TEST_LDFLAGS)
$(TARGET_OBJ)/test/%.o: $(SOURCE_TEST)/%.c
	$(object)

.PHONY: test/types
test/types: $(TARGET_BIN)/test/types
$(TARGET_BIN)/test/types: CFLAGS = $(TEST_CFLAGS)
$(TARGET_BIN)/test/types: LDFLAGS += $(TEST_LDFLAGS)
$(TARGET_BIN)/test/types: $(TEST_TYPES:%=$(TARGET_OBJ)/test/types/%.o) $(TARGET_OBJ)/test/test.o $(TARGET_OBJ)/*.o | modules build prepare
	$(executable)  $(TEST_DEPS)

.PHONY: test/aggr
test/aggr: $(TARGET_BIN)/test/aggr
$(TARGET_BIN)/test/aggr: CFLAGS = $(TEST_CFLAGS)
$(TARGET_BIN)/test/aggr: LDFLAGS += $(TEST_LDFLAGS)
$(TARGET_BIN)/test/aggr: $(TEST_AGGR:%=$(TARGET_OBJ)/test/aggr/%.o) $(TARGET_OBJ)/test/test.o $(TARGET_OBJ)/*.o | modules build prepare
	$(executable)  $(TEST_DEPS)



#PHONY: test
#test: msgpack_test hashmap_test linkedlist_test arraylist_test record_udf

###############################################################################
include project/rules.makefile
###############################################################################
