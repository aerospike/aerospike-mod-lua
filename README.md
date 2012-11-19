# aerospike-mod-lua

Adds support to Aerospike for executing Lua functions.

## Dependencies

#### Lua 5.1.4

This project requires `lua` for executing lua scripts.

Redhat / CentOS
		
	$ yum install lua-devel.x86_64


## Build

To build the test app:

	$ make test

To build a static archive `libmod_lua.a`:

	$ make libmod_lua.a

To build a dynamic library `libmod_lua.so`:

	$ make libmod_lua.so

## Install

All generated files are placed in `./target/{arch}`, where:

- `{arch}` is the target architecture:
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