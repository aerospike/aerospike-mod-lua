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
	$(MAKE) -e -C $(COMMON) clean MSGPACK=$(MSGPACK)

$(COMMON)/$(TARGET_LIB)/libaerospike-common.a:
	$(MAKE) -e -C $(COMMON) libaerospike-common.a MSGPACK=$(MSGPACK)



COMMON-HEADERS := $(wildcard $(COMMON)/$(TARGET_INCL)/aerospike/*.h) $(wildcard $(COMMON)/$(TARGET_INCL)/citrusleaf/*.h)

.PHONY: COMMON-prepare
COMMON-prepare: COMMON-make-prepare $(subst $(COMMON)/$(SOURCE_INCL),$(TARGET_INCL),$(COMMON-HEADERS)) 
	$(noop)

.PHONY: COMMON-make-prepare
COMMON-make-prepare:
	$(MAKE) -e -C $(COMMON) prepare MSGPACK=$(MSGPACK)

$(TARGET_INCL)/aerospike/%.h: $(COMMON)/$(TARGET_INCL)/aerospike/%.h | $(TARGET_INCL)/aerospike
	 cp $^ $@

$(TARGET_INCL)/citrusleaf/%.h: $(COMMON)/$(TARGET_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
	 cp $^ $@

###############################################################################
##  MSGPACK MODULE                                                           ##
###############################################################################

ifndef MSGPACK
$(warning ***************************************************************)
$(warning *)
$(warning *  MSGPACK is not defined. )
$(warning *  MSGPACK should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

ifeq ($(wildcard $(MSGPACK)/configure.in),) 
$(warning ***************************************************************)
$(warning *)
$(warning *  MSGPACK is '$(MSGPACK)')
$(warning *  MSGPACK doesn't contain 'configure'. )
$(warning *  MSGPACK should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

.PHONY: MSGPACK-build
MSGPACK-build: $(MSGPACK)/src/.libs/libmsgpackc.a

.PHONY: MSGPACK-prepare
MSGPACK-prepare: 
	$(noop)

PHONY: MSGPACK-clean
MSGPACK-clean:
	@if [ -e "$(MSGPACK)/Makefile" ]; then \
		$(MAKE) -e -C $(MSGPACK) clean; \
		$(MAKE) -e -C $(MSGPACK) distclean; \
	fi
	@if [ -e "$(MSGPACK)/configure" ]; then \
		rm -f $(MSGPACK)/configure; \
	fi

$(MSGPACK)/configure: $(MSGPACK)/configure.in
	cd $(MSGPACK) && autoreconf -iv

$(MSGPACK)/Makefile: $(MSGPACK)/configure
	cd $(MSGPACK) && ./configure CFLAGS="-fPIC"

$(MSGPACK)/src/.libs/libmsgpackc.a: $(MSGPACK)/Makefile
	cd $(MSGPACK) && $(MAKE) CFLAGS="-fPIC"
