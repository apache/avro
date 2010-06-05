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
#ifndef AVRO_PRIVATE_H
#define AVRO_PRIVATE_H

#include "avro.h"

#ifdef HAVE_CONFIG_H
/* This is only true for now in the autotools build */
#include "config.h"
#endif

#define check(rval, call) { rval = call; if(rval) return rval; }

#define AVRO_UNUSED(var) (void)var;

#define container_of(ptr_, type_, member_)  \
    ((type_ *)((char *)ptr_ - (size_t)&((type_ *)0)->member_))

#endif
