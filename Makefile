###############################################################################
##  SETTINGS                                                                 ##
###############################################################################
include project/settings.mk

# Modules
COMMON := $(COMMON)
LUAMOD := $(LUAMOD)
MODULES := COMMON
MODULES += LUAMOD

# Override optimizations via: make O=n
O = 3

# Make-local Compiler Flags
EXT_CFLAGS =
CC_FLAGS = -std=gnu99 -g -fPIC -O$(O)
CC_FLAGS += -fno-common -fno-strict-aliasing
CC_FLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE $(EXT_CFLAGS)

ifeq ($(ARCH),x86_64)
  REAL_ARCH = -march=nocona
endif

ifeq ($(ARCH),aarch64)
  REAL_ARCH = -mcpu=neoverse-n1
endif

CC_CFLAGS += $(REAL_ARCH)

ifeq ($(TARGET_SERVER), )
  CC_FLAGS += -DAS_MOD_LUA_CLIENT
endif

# Linker flags
LD_FLAGS = $(LDFLAGS)

ifeq ($(OS),Darwin)
  CC_FLAGS += -D_DARWIN_UNLIMITED_SELECT
  LD_FLAGS += -undefined dynamic_lookup
  LUA_PLATFORM = LUA_USE_MACOSX

  ifneq ($(wildcard /opt/homebrew/opt/openssl/include),)
    # Mac new homebrew openssl include path
    CC_FLAGS += -I/opt/homebrew/opt/openssl/include
  else ifneq ($(wildcard /usr/local/opt/openssl/include),)
    # Mac old homebrew openssl include path
    CC_FLAGS += -I/usr/local/opt/openssl/include
  else ifneq ($(wildcard /opt/local/include/openssl),)
    # macports openssl include path
    CC_FLAGS += -I/opt/local/include
  endif
else ifeq ($(OS),FreeBSD)
  CC_FLAGS += -finline-functions
  LUA_PLATFORM = LUA_USE_LINUX # nothing BSD specific in luaconf.h
else
  CC_FLAGS += -finline-functions -rdynamic
  LUA_PLATFORM = LUA_USE_LINUX

  ifneq ($(wildcard /etc/alpine-release),)
    CC_FLAGS += -DAS_ALPINE
  endif
endif

ifneq ($(CF), )
  CC_FLAGS += -I$(CF)/include
endif

# DEBUG Settings
ifdef DEBUG
  O = 0
  CC_FLAGS += -pg -fprofile-arcs -ftest-coverage -g2
  LD_FLAGS += -pg -fprofile-arcs -lgcov
endif

# Make-tree Compiler Flags
CFLAGS = -O$(O)

# Include Paths
INC_PATH += $(COMMON)/$(SOURCE_INCL)
INC_PATH += $(LUAMOD)

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

MOD_LUA =
MOD_LUA += mod_lua.o
MOD_LUA += mod_lua_aerospike.o
MOD_LUA += mod_lua_bytes.o
MOD_LUA += mod_lua_geojson.o
MOD_LUA += mod_lua_iterator.o
MOD_LUA += mod_lua_list.o
MOD_LUA += mod_lua_map.o
MOD_LUA += mod_lua_record.o
MOD_LUA += mod_lua_reg.o
MOD_LUA += mod_lua_stream.o
MOD_LUA += mod_lua_system.o
MOD_LUA += mod_lua_val.o

###############################################################################
##  HEADERS                                                                  ##
###############################################################################

MOD_LUA_HS = $(wildcard $(SOURCE_INCL)/aerospike/*.h)

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: build prepare

.PHONY: build
build: modules-build libmod_lua

.PHONY: prepare
prepare: modules-prepare $(subst $(SOURCE_INCL),$(TARGET_INCL),$(MOD_LUA_HS))

.PHONY: clean
clean:	modules-clean
	@rm -rf $(TARGET)

.PHONY: libmod_lua libmod_lua.a libmod_lua.$(DYNAMIC_SUFFIX)
libmod_lua: libmod_lua.a libmod_lua.$(DYNAMIC_SUFFIX)
libmod_lua.a: $(TARGET_LIB)/libmod_lua.a
libmod_lua.$(DYNAMIC_SUFFIX): $(TARGET_LIB)/libmod_lua.$(DYNAMIC_SUFFIX)

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_OBJ)/%.o: $(SOURCE_MAIN)/%.c
	$(object)

$(TARGET_LIB)/libmod_lua.$(DYNAMIC_SUFFIX): $(MOD_LUA:%=$(TARGET_OBJ)/%) | modules
	$(library)

$(TARGET_LIB)/libmod_lua.a: $(MOD_LUA:%=$(TARGET_OBJ)/%) | modules
	$(archive)

$(TARGET_INCL)/aerospike/%.h: $(SOURCE_INCL)/aerospike/%.h
	@mkdir -p $(@D)
	cp -p $< $@

###############################################################################
include project/modules.mk project/test.mk project/rules.mk
