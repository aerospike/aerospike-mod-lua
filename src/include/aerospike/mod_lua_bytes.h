/* 
 * Copyright 2008-2018 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#pragma once

#include <lua.h>

#include <aerospike/as_bytes.h>
#include <aerospike/mod_lua_val.h>

int mod_lua_bytes_register(lua_State *);

as_bytes * mod_lua_pushbytes(lua_State *, as_bytes * );

as_bytes * mod_lua_tobytes(lua_State *, int);
