###############################################################################
##  COMMON RULES                                                             ##
###############################################################################

$(TARGET_PATH):
	mkdir -p $@

$(TARGET_BASE): | $(TARGET_PATH)
	mkdir -p $@

$(TARGET_BIN): | $(TARGET_BASE)
	mkdir -p $@

$(TARGET_DOC): | $(TARGET_BASE)
	mkdir -p $@

$(TARGET_LIB): | $(TARGET_BASE)
	mkdir -p $@

$(TARGET_OBJ): | $(TARGET_BASE)
	mkdir -p $@

$(TARGET_SRC): | $(TARGET_BASE)
	mkdir -p $@

$(TARGET_INCL): | $(TARGET_BASE)
	mkdir -p $@

.PHONY: info
info:
	@echo
	@echo "  NAME:     " $(NAME) 
	@echo "  OS:       " $(OS)
	@echo "  ARCH:     " $(ARCH)
	@echo "  DISTRO:   " $(DISTRO_NAME)"-"$(DISTRO_VERS)
	@echo
	@echo "  PATHS:"
	@echo "      source:     " $(SOURCE)
	@echo "      target:     " $(TARGET_BASE)
	@echo "      includes:   " $(INC_PATH)
	@echo "      libraries:  " $(LIB_PATH)
	@echo
	@echo "  COMPILER:"
	@echo "      command:    " $(CC)
	@echo "      flags:      " $(CC_FLAGS) $(CFLAGS)
	@echo
	@echo "  LINKER:"
	@echo "      command:    " $(LD)
	@echo "      flags:      " $(LD_FLAGS) $(LDFLAGS)
	@echo
	@echo "  ARCHIVER:"
	@echo "      command:    " $(AR)
	@echo "      flags:      " $(AR_FLAGS) $(ARFLAGS)
	@echo

###############################################################################
##  MODULE RULES                                                             ##
###############################################################################

.PHONY: modules
modules: modules-build modules-prepare

.PHONY: modules-build
modules-build: $(MODULES:%=%-build)

.PHONY: modules-prepare
modules-prepare: $(MODULES:%=%-prepare)

.PHONY: modules-clean
modules-clean: $(MODULES:%=%-clean)
