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

#include "allocator.h"
#include <string.h>

struct avro_allocator_t_ g_avro_allocator = {
	NULL,   // malloc
	NULL,   // calloc
	NULL,   // realloc
	NULL    // free
};

char* 
avro_strdup(const char* source)
{
	char* dest = (char*)g_avro_allocator.malloc(strlen(source) + 1);
	strcpy(dest, source);
	return dest;
}
