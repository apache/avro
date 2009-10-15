/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
#ifndef CONTAINER_OF_H
#define CONTAINER_OF_H

/* This simple macro is found in stddef.h on most os */
#ifndef offsetof
#define offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)
#endif

/* 
The container_of macro allows us to find the address of the outer
structure when passed the address of an inner function.

e.g. 
struct innerstruct {
  int foo;
};
struct outerstruct {
  int bar;
  struct innerstruct inner;
};

If we have a ptr to an inner structure called "in",
we can find the address of the outer struct with...

struct outer *out = container_of (in, struct outerstruct, inner);
*/
#define container_of(ptr, type, member) (\
{                       \
        const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
        (type *)( (char *)__mptr - offsetof(type,member) ); \
})

#endif
