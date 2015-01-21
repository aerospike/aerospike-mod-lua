include project/settings.mk
###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

# Modules
COMMON 	:= $(COMMON)
MODULES := COMMON

ifeq ($(USE_LUAJIT),1)
  MODULES += LUAJIT
endif

# Override optimizations via: make O=n
O = 3

# Make-local Compiler Flags
CC_FLAGS = -std=gnu99 -g -Wall -fPIC -O$(O)
CC_FLAGS += -fno-common -fno-strict-aliasing -finline-functions
CC_FLAGS += -march=nocona -DMARCH_$(ARCH)
CC_FLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE $(EXT_CFLAGS)

PREPRO_SUFFIX = .cpp
ifeq ($(PREPRO),1)
  SUFFIX = $(PREPRO_SUFFIX)
  CC_FLAGS += -E -DPREPRO=$(PREPRO) -DGEN_TAG=$(GEN_TAG)"\
"
endif

ifeq ($(OS),Darwin)
  CC_FLAGS += -D_DARWIN_UNLIMITED_SELECT
  CC_FLAGS += -DLUA_DEBUG_HOOK
else
  CC_FLAGS += -rdynamic
endif

ifneq ($(CF), )
  CC_FLAGS += -I$(CF)/include
endif

# Linker flags
LD_FLAGS = $(LDFLAGS)

ifeq ($(OS),Darwin)
  LD_FLAGS += -undefined dynamic_lookup
endif

# DEBUG Settings
ifdef DEBUG
  O = 0
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

ifeq ($(USE_LUAJIT),1)
  INC_PATH += $(LUAJIT)/src
  LIB_LUA = $(LUAJIT)/src/libluajit.a
else
  INC_PATH += $(or \
    $(wildcard /usr/include/lua-5.1), \
    $(wildcard /usr/include/lua5.1))
  INCLUDE_LUA_5_1 = /usr/include/lua5.1
  ifneq ($(wildcard $(INCLUDE_LUA_5_1)),)
    LUA_SUFFIX=5.1
  endif
  ifeq ($(OS),Darwin)
    ifneq ($(wildcard /usr/local/include),)
      INC_PATH += /usr/local/include
    endif
    ifneq ($(wildcard /usr/local/lib),)
      LIB_LUA = -L/usr/local/lib
    endif
  endif
  LIB_LUA += -llua$(LUA_SUFFIX)
endif

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

.PHONY: libmod_lua libmod_lua.a libmod_lua.$(DYNAMIC_SUFFIX)
libmod_lua: libmod_lua.a libmod_lua.$(DYNAMIC_SUFFIX)
libmod_lua.a: $(TARGET_LIB)/libmod_lua.a
libmod_lua.$(DYNAMIC_SUFFIX): $(TARGET_LIB)/libmod_lua.$(DYNAMIC_SUFFIX)

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_OBJ)/%.o: $(SOURCE_MAIN)/%.c | modules-prepare
	$(object)

$(TARGET_LIB)/libmod_lua.a $(TARGET_LIB)/libmod_lua.$(DYNAMIC_SUFFIX): $(MOD_LUA:%=$(TARGET_OBJ)/%) | $(COMMON)/$(TARGET_INCL)/aerospike

$(TARGET_INCL)/aerospike: | $(TARGET_INCL)
	mkdir $@

$(TARGET_INCL)/aerospike/%.h:: $(SOURCE_INCL)/aerospike/%.h | $(TARGET_INCL)/aerospike
	cp -p $^ $(TARGET_INCL)/aerospike

# .PHONY: test
# test: 
# 	@echo "No tests"

###############################################################################
include project/modules.mk project/test.mk project/rules.mk
