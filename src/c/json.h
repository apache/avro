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
#ifndef JSON_H
#define JSON_H

#include <wchar.h>
#include <apr.h>
#include <apr_pools.h>
#include <apr_tables.h>
#include <apr_hash.h>

enum JSON_type
{
  JSON_UNKNOWN,
  JSON_OBJECT,
  JSON_ARRAY,
  JSON_STRING,
  JSON_NUMBER,
  JSON_BOOLEAN,
  JSON_NULL
};
typedef enum JSON_type JSON_type;

struct JSON_value
{
  JSON_type type;
  union
  {
    apr_hash_t *object;
    apr_array_header_t *array;
    wchar_t *z;
    double number;
    int boolean;
  } value_u;
  apr_pool_t *pool;
};
typedef struct JSON_value JSON_value;
#define json_object value_u.object
#define json_array  value_u.array
#define json_string value_u.z
#define json_number value_u.number
#define json_boolean value_u.boolean

JSON_value *JSON_parse (apr_pool_t * pool, char *text, int text_len);

JSON_value *JSON_value_new (apr_pool_t * pool, int type);

void JSON_print (FILE * file, const JSON_value * value);

struct JSON_ctx
{
  apr_pool_t *pool;
  JSON_value *result;
  int error;
};
typedef struct JSON_ctx JSON_ctx;

/* in json_parser.c */
void *JSONParserAlloc (void *(*mallocProc) (size_t));
void JSONParser (void *yyp, int yymajor, JSON_value * value, JSON_ctx * ctx);
void JSONParserFree (void *p, void (*freeProc) (void *));
void JSONParserTrace (FILE * TraceFILE, char *zTracePrompt);

#endif
