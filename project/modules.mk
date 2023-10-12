###############################################################################
##  COMMON MODULE                                                            ##
###############################################################################

ifndef COMMON
$(warning ***************************************************************)
$(warning *)
$(warning *  COMMON is not defined. )
$(warning *  COMMON should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

ifeq ($(wildcard $(COMMON)/Makefile),)
$(warning ***************************************************************)
$(warning *)
$(warning *  COMMON is '$(COMMON)')
$(warning *  COMMON doesn't contain 'Makefile'. )
$(warning *  COMMON should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

.PHONY: COMMON-build
COMMON-build: $(COMMON)/$(TARGET_LIB)/libaerospike-common.a

.PHONY: COMMON-clean
COMMON-clean:
	$(MAKE) -e -C $(COMMON) clean

$(COMMON)/$(TARGET_LIB)/libaerospike-common.a:
	$(MAKE) -e -C $(COMMON) libaerospike-common.a

COMMON-HEADERS := $(wildcard $(COMMON)/$(TARGET_INCL)/aerospike/*.h) $(wildcard $(COMMON)/$(TARGET_INCL)/citrusleaf/*.h)

.PHONY: COMMON-prepare
COMMON-prepare: COMMON-make-prepare $(subst $(COMMON)/$(SOURCE_INCL),$(TARGET_INCL),$(COMMON-HEADERS))
	$(noop)

.PHONY: COMMON-make-prepare
COMMON-make-prepare:
	$(MAKE) -e -C $(COMMON) prepare

$(TARGET_INCL)/aerospike/%.h: $(COMMON)/$(TARGET_INCL)/aerospike/%.h
	@mkdir -p $(@D)
	cp -p $< $@

$(TARGET_INCL)/citrusleaf/%.h: $(COMMON)/$(TARGET_INCL)/citrusleaf/%.h
	@mkdir -p $(@D)
	cp -p $< $@

###############################################################################
##  LUA MODULE                                                               ##
###############################################################################

ifndef LUAMOD
  $(warning ***************************************************************)
  $(warning *)
  $(warning *  LUAMOD is not defined. )
  $(warning *  LUAMOD should be set to a valid path. )
  $(warning *)
  $(warning ***************************************************************)
  $(error )
endif

ifeq ($(wildcard $(LUAMOD)/makefile),)
  $(warning ***************************************************************)
  $(warning *)
  $(warning *  LUAMOD is '$(LUAMOD)')
  $(warning *  LUAMOD doesn't contain 'makefile'. )
  $(warning *  LUAMOD should be set to a valid path. )
  $(warning *)
  $(warning ***************************************************************)
  $(error )
endif

.PHONY: LUAMOD-build
LUAMOD-build: $(LUAMOD)/liblua.a

$(LUAMOD)/liblua.a:
	$(MAKE) -C $(LUAMOD) CFLAGS="-Wall -O2 -std=c99 -D$(LUA_PLATFORM) -fPIC -fno-stack-protector -fno-common $(REAL_ARCH) -g" a

.PHONY: LUAMOD-clean
LUAMOD-clean:
	$(MAKE) -e -C $(LUAMOD) clean

.PHONY: LUAMOD-prepare
LUAMOD-prepare: ;

