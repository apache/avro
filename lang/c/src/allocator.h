#ifndef AVRO_ALLOCATOR_H
#define AVRO_ALLOCATOR_H
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

#include <stddef.h>

typedef void * (*avro_malloc_def)(size_t size);
typedef void * (*avro_calloc_def)(size_t count, size_t size);
typedef void * (*avro_realloc_def)(void *ptr, size_t size);
typedef void   (*avro_free_def)(void *ptr);

struct avro_allocator_t_
{
	avro_malloc_def  malloc;
	avro_calloc_def  calloc;
	avro_realloc_def realloc;
	avro_free_def    free;
};

extern struct avro_allocator_t_ g_avro_allocator;

char* avro_strdup(const char* source);

#endif // AVRO_ALLOCATOR_H
