# aerospike-mod-lua

Aeospike Mod Lua provides support for executing Lua language functions
using the Aerospike data types. This module is used by both the
Aerospike Server and the Aerospike C Client as a `git` submodule.

## Dependencies

### Linux Dependencies

Building Aerospike Mod Lua requires the development resources for the
Lua language version 5.4. The installation instructions for common
Linux distributions are as follows:

## Build

### Build Linux and MacOS

To build the test app:

	$ make test

To build a static archive `libmod_lua.a`:

	$ make libmod_lua.a

To build a dynamic library `libmod_lua.so`:

	$ make libmod_lua.so

### Build MacOS XCode

- Double click xcode/aerospike-mod-lua.xcworkspace
- Click Product -> Build

### Build Windows Visual Studio 2022+

- Double click vs/aerospike-mod-lua.sln
- Click Build -> Build Solution

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
