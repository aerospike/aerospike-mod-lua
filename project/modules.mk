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

$(LUAMOD)/src/luaconf.h:	$(LUAMOD)/src/luaconf.h.orig
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
LUAMOD-prepare:	$(LUAMOD)/src/luaconf.h

###############################################################################
##  LUA JIT MODULE                                                           ##
###############################################################################

ifeq ($(USE_LUAJIT),1)
  ifndef LUAJIT
    $(warning ***************************************************************)
    $(warning *)
    $(warning *  LUAJIT is not defined. )
    $(warning *  LUAJIT should be set to a valid path. )
    $(warning *)
    $(warning ***************************************************************)
    $(error )
  endif

  ifeq ($(wildcard $(LUAJIT)/Makefile),)
    $(warning ***************************************************************)
    $(warning *)
    $(warning *  LUAJIT is '$(LUAJIT)')
    $(warning *  LUAJIT doesn't contain 'Makefile'. )
    $(warning *  LUAJIT should be set to a valid path. )
    $(warning *)
    $(warning ***************************************************************)
    $(error )
  endif
endif

.PHONY: LUAJIT-build
LUAJIT-build:	$(LUAJIT)/src/libluajit.a

$(LUAJIT)/src/libluajit.a:	$(LUAJIT)/src/luaconf.h
ifeq ($(USE_LUAJIT),1)
	$(MAKE) -C $(LUAJIT) Q= TARGET_SONAME=libluajit.so CCDEBUG=-g CFLAGS= LDFLAGS=
endif

$(LUAJIT)/src/luaconf.h:	$(LUAJIT)/src/luaconf.h.orig
ifeq ($(USE_LUAJIT),1)
	(cd $(LUAJIT)/src; ln -s $(notdir $<) $(notdir $@))
endif

.PHONY: LUAJIT-clean
LUAJIT-clean:	$(LUAJIT)/src/luaconf.h
ifeq ($(USE_LUAJIT),1)
	$(MAKE) -e -C $(LUAJIT) clean
	(cd $(LUAJIT)/src; $(RM) $(LUAJIT)/src/luaconf.h $(LUAJIT)/src/libluajit.a)
endif

.PHONY: LUAJIT-prepare
LUAJIT-prepare:	$(LUAJIT)/src/luaconf.h
