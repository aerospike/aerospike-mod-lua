include project/settings.mk
###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

OS := $(shell uname)

# Modules
COMMON 	:= $(COMMON)
MODULES := COMMON

# Override optimizations via: make O=n
O = 3

# Make-local Compiler Flags
CC_FLAGS = -std=gnu99 -g -Wall -fPIC
CC_FLAGS += -fno-common -fno-strict-aliasing -finline-functions
CC_FLAGS += -march=nocona -DMARCH_$(ARCH)
CC_FLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE $(EXT_CFLAGS)

ifeq ($(OS),Darwin)
CC_FLAGS += -D_DARWIN_UNLIMITED_SELECT
else
CC_FLAGS += -rdynamic
endif

ifneq ($(CF), )
CC_FLAGS += -I$(CF)/include
endif

# Linker flags
LD_FLAGS = $(LDFLAGS) -lm -fPIC 

ifeq ($(OS),Darwin)
LD_FLAGS += -undefined dynamic_lookup
endif

# DEBUG Settings
ifdef DEBUG
O=0
CC_FLAGS += -pg -fprofile-arcs -ftest-coverage -g2
LD_FLAGS += -pg -fprofile-arcs -lgcov
endif

# Make-tree Compiler Flags
CFLAGS = -O$(O) 

# Make-tree Linker Flags
# LDFLAGS = 

# Make-tree Linker Flags
# LDFLAGS = 

# Include Paths
INC_PATH += $(COMMON)/$(TARGET_INCL)

# Library Paths
# LIB_PATH +=

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

MOD_LUA =
MOD_LUA += mod_lua.o
MOD_LUA += mod_lua_reg.o
MOD_LUA += mod_lua_aerospike.o
MOD_LUA += mod_lua_record.o
MOD_LUA += mod_lua_iterator.o
MOD_LUA += mod_lua_list.o
MOD_LUA += mod_lua_map.o
MOD_LUA += mod_lua_bytes.o
MOD_LUA += mod_lua_stream.o
MOD_LUA += mod_lua_val.o

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: build prepare

.PHONY: prepare
prepare: $(TARGET_INCL)/aerospike/*.h

.PHONY: build 
build: libmod_lua

.PHONY: build-clean
build-clean:
	@rm -rf $(TARGET_BIN)
	@rm -rf $(TARGET_LIB)

.PHONY: libmod_lua libmod_lua.a libmod_lua.so
libmod_lua: libmod_lua.a libmod_lua.so
libmod_lua.a: $(TARGET_LIB)/libmod_lua.a
libmod_lua.so: $(TARGET_LIB)/libmod_lua.so

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_OBJ)/%.o: COMMON-prepare $(SOURCE_MAIN)/%.c | modules-prepare
	$(object)

$(TARGET_LIB)/libmod_lua.a $(TARGET_LIB)/libmod_lua.so: $(MOD_LUA:%=$(TARGET_OBJ)/%) | $(COMMON)/$(TARGET_INCL)/aerospike

$(TARGET_INCL)/aerospike: | $(TARGET_INCL)
	mkdir $@

$(TARGET_INCL)/aerospike/%.h:: $(SOURCE_INCL)/aerospike/%.h | $(TARGET_INCL)/aerospike
	cp -p $^ $(TARGET_INCL)/aerospike

# .PHONY: test
# test: 
# 	@echo "No tests"

###############################################################################
include project/modules.mk project/test.mk project/rules.mk
