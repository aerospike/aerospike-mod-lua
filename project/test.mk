###############################################################################
##  TEST FLAGS                                                               ##
###############################################################################

TEST_VALGRIND = --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes -v

TEST_CFLAGS =
TEST_CFLAGS += -I$(TARGET_INCL)
TEST_CFLAGS += -I$(COMMON)/$(TARGET_INCL)

TEST_LDFLAGS += -lssl -lcrypto $(LIB_LUA) -lpthread -lm

ifeq ($(OS),Darwin)
  ifeq ($(USE_LUAJIT),1)
    TEST_LDFLAGS += -pagezero_size 10000 -image_base 100000000
  endif
else
  TEST_LDFLAGS += -lrt -ldl
endif

TEST_DEPS =
TEST_DEPS += $(COMMON)/$(TARGET_LIB)/libaerospike-common.a 

###############################################################################
##  TEST OBJECTS                                                             ##
###############################################################################

TEST_PLANS = 
TEST_PLANS += list/list_udf
TEST_PLANS += record/record_udf
TEST_PLANS += stream/stream_udf
TEST_PLANS += validation/validation_basics

TEST_UTIL = 
TEST_UTIL += util/consumer_stream
TEST_UTIL += util/producer_stream
TEST_UTIL += util/map_rec
TEST_UTIL += util/test_aerospike
TEST_UTIL += util/test_logger
#TEST_UTIL += util/test_memtracker

TEST_MOD_LUA = mod_lua_test
TEST_MOD_LUA += $(TEST_UTIL) 
TEST_MOD_LUA += $(TEST_PLANS)

###############################################################################
##  TEST TARGETS                                                             ##
###############################################################################

.PHONY: test
test: test-build
ifndef LUA_CORE
$(warning ***************************************************************)
$(warning *)
$(warning *  LUA_CORE is not defined. )
$(warning *  LUA_CORE should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
else
	AS_SYSTEM_LUA=$(LUA_CORE)/src $(TARGET_BIN)/test/mod_lua_test
endif

.PHONY: test-valgrind
test-valgrind: test-build
ifndef LUA_CORE
$(warning ***************************************************************)
$(warning *)
$(warning *  LUA_CORE is not defined. )
$(warning *  LUA_CORE should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
else
	AS_SYSTEM_LUA=$(LUA_CORE)/src valgrind $(TEST_VALGRIND) $(TARGET_BIN)/test/mod_lua_test 1>&2 2>mod_lua_test-valgrind
endif

.PHONY: test-build
test-build: test/mod_lua_test

.PHONY: test-clean
test-clean: 
	@rm -rf $(TARGET_BIN)/test
	@rm -rf $(TARGET_OBJ)/test

$(TARGET_OBJ)/test/%/%.o: CFLAGS = $(TEST_CFLAGS)
$(TARGET_OBJ)/test/%/%.o: LDFLAGS = $(TEST_LDFLAGS)
$(TARGET_OBJ)/test/%/%.o: $(SOURCE_TEST)/%/%.c
	$(object)

$(TARGET_OBJ)/test/%.o: CFLAGS = $(TEST_CFLAGS)
$(TARGET_OBJ)/test/%.o: LDFLAGS = $(TEST_LDFLAGS)
$(TARGET_OBJ)/test/%.o: $(SOURCE_TEST)/%.c
	$(object)

.PHONY: test/mod_lua_test
test/mod_lua_test: $(TARGET_BIN)/test/mod_lua_test
$(TARGET_BIN)/test/mod_lua_test: CFLAGS = $(TEST_CFLAGS)
$(TARGET_BIN)/test/mod_lua_test: LDFLAGS = $(TEST_DEPS) $(TEST_LDFLAGS)
$(TARGET_BIN)/test/mod_lua_test: $(TEST_MOD_LUA:%=$(TARGET_OBJ)/test/%.o) $(TARGET_OBJ)/test/test.o $(wildcard $(TARGET_OBJ)/*) | modules build prepare
	$(executable)
