# aerospike-mod-lua

Aeospike Mod Lua provides support for executing Lua language functions
using the Aerospike data types. This module is used by both the
Aerospike Server and the Aerospike C Client as a `git` submodule.

## Dependencies

### Linux Dependencies

Building Aerospike Mod Lua requires the development resources for the
Lua language version 5.1.4.  The installation instructions for common
Linux distributions are as follows:

#### Red Hat Dependencies

Red Hat Package Manager (RPM) based Linux Distributions (Red Hat,
Fedora, CentOS, SUSE, etc.) require the following packages:

* `lua-devel` - Development resources for the Lua language.

If `yum` is your package manager, then you should be able to run the following command:

	$ sudo yum install lua-devel

#### Debian Dependencies

Debian based Linux Distributions (Debian, Ubuntu, etc.) require the following packages:

* `liblua5.1-dev` - Development resources for the Lua language.

If `apt-get` is your package manager, then you should be able to run the following command:

	$ sudo apt-get install liblua5.1-dev

## Build

To build the test app:

	$ make test

To build a static archive `libmod_lua.a`:

	$ make libmod_lua.a

To build a dynamic library `libmod_lua.so`:

	$ make libmod_lua.so

## Install

All generated files are placed in `./target/{arch}`, where:

- `{arch}` is the target architecture, e.g., `Linux-x86_64`.
- The `lib` subdirectory contains all libraries. 
- The `bin` subdirectory contains all executables.

#### libmod_lua.so

You will want to either:

1. Move the `libmod_lua.so` to a location your program can access.
2. Add the path to `libmod_lua.so` to your `LD_LIBRARY_PATH`.

## Test

To test, you can run the following:

	$ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:target/Linux-x86_64/lib
	$ target/Linux-x86_64/bin/test record test1.record a b c d
