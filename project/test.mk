###############################################################################
##  TEST FLAGS                                                               ##
###############################################################################

TEST_VALGRIND = --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes -v

TEST_CFLAGS =
TEST_CFLAGS += -I$(TARGET_INCL)
TEST_CFLAGS += -I$(COMMON)/$(TARGET_INCL)
TEST_CFLAGS += -I$(LUAMOD)

TEST_LDFLAGS =

ifeq ($(OS),Darwin)
  ifneq ($(wildcard /opt/homebrew/opt/openssl/lib),)
    # Mac new homebrew openssl lib path
    TEST_LDFLAGS += -L/opt/homebrew/opt/openssl/lib
  endif
endif

TEST_LDFLAGS += -lssl -lcrypto -lpthread -lm

ifeq ($(OS),FreeBSD)
  TEST_LDFLAGS += -lrt
else ifneq ($(OS),Darwin)
  TEST_LDFLAGS += -lrt -ldl
endif

TEST_DEPS =
TEST_DEPS += $(COMMON)/$(TARGET_LIB)/libaerospike-common.a
TEST_DEPS += $(LUAMOD)/liblua.a
TEST_DEPS += $(TARGET_LIB)/libmod_lua.a

###############################################################################
##  TEST OBJECTS                                                             ##
###############################################################################

TEST_PLANS =
TEST_PLANS += list/list_udf
TEST_PLANS += record/record_udf
TEST_PLANS += stream/stream_udf
TEST_PLANS += validation/validation_basics
TEST_PLANS += hash/hash_udf

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
	$(TARGET_BIN)/test/mod_lua_test

.PHONY: test-valgrind
test-valgrind: test-build
	valgrind $(TEST_VALGRIND) $(TARGET_BIN)/test/mod_lua_test 1>&2 2>mod_lua_test-valgrind

.PHONY: test-build
test-build: $(TEST_DEPS) test/mod_lua_test

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
