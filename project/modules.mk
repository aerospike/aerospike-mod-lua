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

$(TARGET_INCL)/aerospike/%.h: $(COMMON)/$(TARGET_INCL)/aerospike/%.h | $(TARGET_INCL)/aerospike
	 cp -p $^ $@

$(TARGET_INCL)/citrusleaf/%.h: $(COMMON)/$(TARGET_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
	 cp -p $^ $@

###############################################################################
##  LUA MODULE                                                               ##
###############################################################################

ifeq ($(USE_LUAMOD),1)
  ifndef LUAMOD
    $(warning ***************************************************************)
    $(warning *)
    $(warning *  LUAMOD is not defined. )
    $(warning *  LUAMOD should be set to a valid path. )
    $(warning *)
    $(warning ***************************************************************)
    $(error )
  endif

  ifeq ($(wildcard $(LUAMOD)/Makefile),)
    $(warning ***************************************************************)
    $(warning *)
    $(warning *  LUAMOD is '$(LUAMOD)')
    $(warning *  LUAMOD doesn't contain 'Makefile'. )
    $(warning *  LUAMOD should be set to a valid path. )
    $(warning *)
    $(warning ***************************************************************)
    $(error )
  endif
endif

.PHONY: LUAMOD-build
LUAMOD-build:	$(LUAMOD)/src/liblua.a

$(LUAMOD)/src/liblua.a:	$(LUAMOD)/src/luaconf.h
ifeq ($(USE_LUAMOD),1)
	$(MAKE) -C $(LUAMOD) $(LUA_PLATFORM)
endif

## This is necessary to build without luajit. ##
$(LUAMOD)/src/luaconf.h:  $(LUAMOD)/src/luaconf.h.orig
ifeq ($(USE_LUAMOD),1)
	(cd $(LUAMOD)/src; rm -f $(notdir $@); ln -s $(notdir $<) $(notdir $@))
endif

.PHONY: LUAMOD-clean
LUAMOD-clean:
ifeq ($(USE_LUAMOD),1)
	$(MAKE) -e -C $(LUAMOD) clean
	rm -f $(LUAMOD)/src/luaconf.h
endif

.PHONY: LUAMOD-prepare
LUAMOD-prepare: $(LUAMOD)/src/luaconf.h
