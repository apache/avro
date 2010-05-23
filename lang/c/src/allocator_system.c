/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0 
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
* 
* http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
* implied.  See the License for the specific language governing
* permissions and limitations under the License. 
*/

#include "allocator_system.h"
#include "allocator.h"

#include <stdlib.h>

//------------------------------------------------------------------------------
/**
*/
void
avro_allocator_system_initialize(void)
{
	g_avro_allocator.malloc  = avro_allocator_system_malloc;
	g_avro_allocator.calloc  = avro_allocator_system_calloc;
	g_avro_allocator.realloc = avro_allocator_system_realloc;
	g_avro_allocator.free    = avro_allocator_system_free;
}

//------------------------------------------------------------------------------
/**
*/
void*
avro_allocator_system_malloc(size_t size)
{
	return malloc(size);
}

//------------------------------------------------------------------------------
/**
*/
void*
avro_allocator_system_calloc(size_t count, size_t size)
{
	return calloc(count, size);
}

//------------------------------------------------------------------------------
/**
*/
void*
avro_allocator_system_realloc(void *ptr, size_t size)
{
	return realloc(ptr, size);
}

//------------------------------------------------------------------------------
/**
*/
void
avro_allocator_system_free(void *ptr)
{
	free(ptr);
}

