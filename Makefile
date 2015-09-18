# Aerospike Server
# Makefile
#
# Main Build Targets:
#
#   make {all|server} - Build the Aerospike Server.
#   make clean        - Remove build products, excluding built packages.
#   make cleanpkg     - Remove built packages.
#   make cleanall     - Remove all build products, including built packages.
#   make cleangit     - Remove all files untracked by Git.  (Use with caution!)
#   make strip        - Build stripped versions of the server executables.
#
# Packaging Targets:
#
#   make deb     - Package server for Debian / Ubuntu platforms as a ".deb" file.
#   make rpm     - Package server for the Red Hat Package Manager (RPM.)
#   make tar     - Package server as a compressed tarball for every Linux platform.
#   make source  - Package the server source code as a compressed "tar" archive.
#
# Building a distribution release is a two step process:
#
#   1). The initial "make" builds the server itself.
#
#   2). The second step packages up the server using "make" with one of the following targets:
#
#       rpm:  Suitable for building and installing on Red Hat-derived systems.
#       deb:  Suitable for building and installing on Debian-derived systems.
#       tar:  Makes an "Every Linux" distribution, packaged as a compressed "tar" archive.
#
# Targets for running the Aerospike Server in the source tree:
#
#   make init    - Initialize the server run-time directories.
#   make start   - Start the server.
#   make stop    - Stop the server.
#
# Advanced Targets:
#
#   make asm  - Build with support for the ASMalloc memory allocation tracking tool.
#
# Intermediate Targets Used for ASMalloc Support:
#
#   make gen   - Generate the "cf/include/mallocation.h" file via "cpp" & Python.
#   make mexp  - Build server using 2-phase macro expansion to transform  and instrument the source tree.
#   make mexp1 - Macroexpand Phase 1:  Create the macroexpanded source tree using "m4" & Python.
#   make mexp2 - Macroexpand Phase 2:  Build the macroexpanded source tree.
#

# Common variable definitions:
include make_in/Makefile.vars

.PHONY: all server
all server:	targetdirs version $(JANSSON)/Makefile $(JEMALLOC)/Makefile $(LUAJIT)/src/luaconf.h
ifeq ($(USE_ASM),1)
	$(MAKE) -C $(ASMALLOC) jem SRCDIR=src
endif
ifeq ($(USE_JEM),1)
	$(MAKE) -C $(JEMALLOC)
endif
ifeq ($(USE_LUAJIT),1)
	$(MAKE) -C $(LUAJIT) Q= TARGET_SONAME=libluajit.so CCDEBUG=-g
endif
	$(MAKE) -C $(JANSSON)
	$(MAKE) -C $(COMMON) CF=$(CF) EXT_CFLAGS="$(EXT_CFLAGS)"
	$(MAKE) -C $(CF)
	$(MAKE) -C $(MOD_LUA) CF=$(CF) COMMON=$(COMMON) LUA_CORE=$(LUA_CORE) EXT_CFLAGS="$(EXT_CFLAGS)" USE_LUAJIT=$(USE_LUAJIT) LUAJIT=$(LUAJIT)
	$(MAKE) -C xdr
	$(MAKE) -C ai
	$(MAKE) -C as

.PHONY: targetdirs
targetdirs:
	mkdir -p $(GEN_DIR) $(LIBRARY_DIR) $(BIN_DIR)
	mkdir -p $(MEXP_DIR)/base $(MEXP_DIR)/fabric $(MEXP_DIR)/storage
	mkdir -p $(OBJECT_DIR)/base $(OBJECT_DIR)/fabric $(OBJECT_DIR)/storage

strip:	server
	$(MAKE) -C xdr strip
	$(MAKE) -C as strip

.PHONY: init start stop
init:
	@echo "Creating and initializing working directories..."
	mkdir -p run/log run/work/smd run/work/sys/udf/lua run/work/usr/udf/lua
	cp -pr modules/lua-core/src/* run/work/sys/udf/lua

start:
	@echo "Running the Aerospike Server locally..."
	@PIDFILE=run/asd.pid ; if [ -f $$PIDFILE ]; then echo "Aerospike already running?  Please do \"make stop\" first."; exit -1; fi
	$(BIN_DIR)/asd --config-file as/etc/aerospike_dev.conf

stop:
	@echo "Stopping the local Aerospike Server..."
	@PIDFILE=run/asd.pid ; if [ -f $$PIDFILE ]; then kill `cat $$PIDFILE`; rm $$PIDFILE; fi

.PHONY: clean
clean:	cleanmodules cleandist
	$(RM) $(VERSION_SRC) $(VERSION_OBJ)
	$(RM) -rf $(TARGET_DIR)

.PHONY: cleanmodules
cleanmodules:
	$(MAKE) -C $(ASMALLOC) cleanest cleanest.jem
	$(MAKE) -C $(COMMON) clean
	if [ -e "$(JANSSON)/Makefile" ]; then \
		$(MAKE) -C $(JANSSON) clean; \
		$(MAKE) -C $(JANSSON) distclean; \
	fi
	if [ -e "$(JEMALLOC)/Makefile" ]; then \
		$(MAKE) -C $(JEMALLOC) clean; \
		$(MAKE) -C $(JEMALLOC) distclean; \
	fi
	if [ -e "$(LUAJIT)/Makefile" ]; then \
		$(MAKE) -C $(LUAJIT) clean; \
	fi
	$(MAKE) -C $(MOD_LUA) COMMON=$(COMMON) LUA_CORE=$(LUA_CORE) USE_LUAJIT=$(USE_LUAJIT) LUAJIT=$(LUAJIT) clean

.PHONY: cleandist
cleandist:
	$(RM) -r pkg/dist/*

.PHONY: cleanall
cleanall: clean cleanpkg

.PHONY: cleanpkg
cleanpkg:
	$(RM) pkg/packages/*

GIT_CLEAN = git clean -fdx

.PHONY: cleangit
cleangit:
	cd $(ASMALLOC); $(GIT_CLEAN)
	cd $(COMMON); $(GIT_CLEAN)
	cd $(JANSSON); $(GIT_CLEAN)
	cd $(JEMALLOC); $(GIT_CLEAN)
	cd $(LUA_CORE); $(GIT_CLEAN)
	cd $(LUAJIT); $(GIT_CLEAN)
	cd $(MOD_LUA); $(GIT_CLEAN)
	$(GIT_CLEAN)

.PHONY: rpm deb tar
rpm deb tar src:
	$(MAKE) -C pkg/$@ EDITION=$(EDITION)

$(VERSION_SRC):	targetdirs
	build/gen_version $(EDITION) $(shell $(DEPTH)/build/os_version) > $(VERSION_SRC)

$(VERSION_OBJ):	$(VERSION_SRC)
	$(CC) -o $@ -c $<

.PHONY: version
version:	$(VERSION_OBJ)

.PHONY:	asm mexp
asm:	version
	$(MAKE) mexp USE_ASM=1 LD_JEM=dynamic

GEN_TAG = GEN_TAG_001
GEN_FILE = $(GEN_DIR)/CPPMallocations.py

gen:
	$(RM) $(GEN_FILE)
	touch $(GEN_FILE)
	$(MAKE) PREPRO=1 GEN_TAG=$(GEN_TAG)
	echo "def AddLocs(AddLoc):" > $(GEN_FILE)
	find -L . -name "*.o.cpp" -exec grep $(GEN_TAG) {} \; | grep -v "#define" | sed 's/.*GEN_TAG_001\( .*\)GEN_TAG_001.*/\1/g' | sort | uniq >> $(GEN_FILE)
	build/GenMallocations.py $(GEN_DIR)

mexp:	mexp2

mexp1:	gen
	$(MAKE) MEXP_PHASE=1 SRCDIR=$(realpath $(MEXP_DIR))/ TOOLS_DIR=$(realpath build)

mexp2: mexp1
	$(MAKE) MEXP_PHASE=2 SRCDIR=$(realpath $(MEXP_DIR))/

$(JANSSON)/configure:
	cd $(JANSSON) && autoreconf -i

$(JANSSON)/Makefile: $(JANSSON)/configure
	cd $(JANSSON) && ./configure $(JANSSON_CONFIG_OPT)

$(JEMALLOC)/configure:
	cd $(JEMALLOC) && autoconf

$(JEMALLOC)/Makefile: $(JEMALLOC)/configure
	cd $(JEMALLOC) && ./configure $(JEM_CONFIG_OPT)

$(LUAJIT)/src/luaconf.h: $(LUAJIT)/src/luaconf.h.orig
	ln -s $(notdir $<) $@

.PHONY: source
source: src

tags etags:
	etags `find ai as cf modules xdr $(EEREPO) -name "*.[ch]" | egrep -v '(target/Linux|m4)'` `find /usr/include -name "*.h"`

# Common target definitions:
ifneq ($(EEREPO),)
  include $(EEREPO)/make_in/Makefile.targets
endif
