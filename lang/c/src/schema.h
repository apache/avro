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
#ifndef AVRO_SCHEMA_H
#define AVRO_SCHEMA_H

#include "avro.h"
#include "container_of.h"
#include "queue.h"

struct record_field_t
{
  char *name;
  avro_schema_t type;
  /* TODO: default */
    TAILQ_ENTRY (record_field_t) fields;
};

struct schema_record_t
{
  struct avro_obj_t obj;
  char *name;
    TAILQ_HEAD (fields, record_field_t) fields;
};

struct enum_symbol_t
{
  char *symbol;
    TAILQ_ENTRY (enum_symbol_t) symbols;
};

struct schema_enum_t
{
  struct avro_obj_t obj;
  char *name;
    TAILQ_HEAD (symbols, enum_symbol_t) symbols;
};

struct schema_array_t
{
  struct avro_obj_t obj;
  avro_schema_t items;
};

struct schema_map_t
{
  struct avro_obj_t obj;
  avro_schema_t values;
};

struct union_schema_t
{
  avro_schema_t schema;
    TAILQ_ENTRY (union_schema_t) schemas;
};

struct schema_union_t
{
  struct avro_obj_t obj;
    TAILQ_HEAD (schemas, union_schema_t) schemas;
};

struct schema_fixed_t
{
  struct avro_obj_t obj;
  const char *name;
  size_t size;
};

struct schema_link_t
{
  struct avro_obj_t obj;
  avro_schema_t to;
};

#define avro_schema_to_record(schema_)  container_of(schema_, struct schema_record_t, obj)
#define avro_schema_to_enum(schema_)    container_of(schema_, struct schema_enum_t, obj)
#define avro_schema_to_array(schema_)   container_of(schema_, struct schema_array_t, obj)
#define avro_schema_to_map(schema_)     container_of(schema_, struct schema_map_t, obj)
#define avro_schema_to_union(schema_)   container_of(schema_, struct schema_union_t, obj)
#define avro_schema_to_fixed(schema_)   container_of(schema_, struct schema_fixed_t, obj)
#define avro_schema_to_link(schema_)    container_of(schema_, struct schema_link_t, obj)

static inline avro_schema_t
avro_schema_incref (avro_schema_t schema)
{
  if (schema && schema->refcount != (unsigned int) -1)
    {
      ++schema->refcount;
    }
  return schema;
}

void avro_schema_free (avro_schema_t schema);

static inline void
avro_schema_decref (avro_schema_t schema)
{
  if (schema && schema->refcount != (unsigned int) -1
      && --schema->refcount == 0)
    {
      avro_schema_free (schema);
    }
}


#endif
