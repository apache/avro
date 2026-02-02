/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef avro_Config_hh
#define avro_Config_hh

#ifdef _MSC_VER
#pragma warning(disable : 4275 4251)
#endif // _MSC_VER

/*
 * Symbol visibility macros:
 *  - AVRO_DLL_EXPORT annotation for exporting symbols
 *  - AVRO_DLL_IMPORT annotation for importing symbols
 *  - AVRO_DLL_HIDDEN annotation for hiding symbols
 *  - AVRO_DYN_LINK needs to be defined when compiling / linking avro as dynamic library
 *  - AVRO_SOURCE needs to be defined when compiling avro as library
 *  - AVRO_DECL contains the correct symbol visibility annotation depending on AVRO_DYN_LINK and AVRO_SOURCE
 */

#if defined _WIN32 || defined __CYGWIN__
#define AVRO_DLL_EXPORT __declspec(dllexport)
#define AVRO_DLL_IMPORT __declspec(dllimport)
#define AVRO_DLL_HIDDEN
#else
#define AVRO_DLL_EXPORT [[gnu::visibility("default")]]
#define AVRO_DLL_IMPORT [[gnu::visibility("default")]]
#define AVRO_DLL_HIDDEN [[gnu::visibility("hidden")]]
#endif // _WIN32 || __CYGWIN__

#ifdef AVRO_DYN_LINK
#ifdef AVRO_SOURCE
#define AVRO_DECL AVRO_DLL_EXPORT
#else
#define AVRO_DECL AVRO_DLL_IMPORT
#endif // AVRO_SOURCE
#endif // AVRO_DYN_LINK

#ifndef AVRO_DECL
#define AVRO_DECL
#endif

#ifdef _WIN32
#include <intsafe.h>
using ssize_t = SSIZE_T;
#endif // _WIN32

#endif
