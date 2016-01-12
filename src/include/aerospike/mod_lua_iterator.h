/* 
 * Copyright 2008-2016 Aerospike, Inc.
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

#include <aerospike/as_iterator.h>

int mod_lua_iterator_register(lua_State *);

// Pushes an iterator userdata object, and returns that
// object so it can be initialized
// (works different than some of the other calls)
as_iterator * mod_lua_pushiterator(lua_State *, size_t sz);

as_iterator * mod_lua_toiterator(lua_State *, int);
