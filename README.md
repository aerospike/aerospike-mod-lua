# aerospike-mod-lua

Adds support to Aerospike for executing Lua functions.

## Dependencies

### Linux Dependencies

#### Redhat Dependencies

Redhat based Linux Distributions (Redhat, Fedora, CentOS, SUS, etc.)require the following packages:

* `lua-devel.x86_64` - should install development resources for `lua-5.1.4` 

If `yum` is your package manager, then you should be able to run the following command:

	$ sudo yum install lua-devel.x86_64

#### Debian Dependencies

Debian based Linux Distributions (Debian, Ubuntu, etc.) require the following packages:

* `liblua5.1-dev` - should install development resources for `lua-5.1.4` 

If `apt-get` is your package manager, then you should be able to run the following command:

	$ sudo apt-get install liblua5.1-dev


### Library Dependencies

#### mgspack-0.5.7

mod-lua utilizes msgpack for serializing some types. We recommend you follow the instructions provided on the msgpacks's [QuickStart for C Language](http://wiki.msgpack.org/display/MSGPACK/QuickStart+for+C+Language).


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