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

struct avro_record_field_t
{
  char *name;
  avro_schema_t type;
  /* TODO: default values */
    STAILQ_ENTRY (avro_record_field_t) fields;
};

struct avro_record_schema_t
{
  struct avro_obj_t obj;
  char *name;
    STAILQ_HEAD (fields, avro_record_field_t) fields;
};

struct avro_enum_symbol_t
{
  char *symbol;
    STAILQ_ENTRY (avro_enum_symbol_t) symbols;
};

struct avro_enum_schema_t
{
  struct avro_obj_t obj;
  char *name;
    STAILQ_HEAD (symbols, avro_enum_symbol_t) symbols;
};

struct avro_array_schema_t
{
  struct avro_obj_t obj;
  avro_schema_t items;
};

struct avro_map_schema_t
{
  struct avro_obj_t obj;
  avro_schema_t values;
};

struct avro_union_branch_t
{
  avro_schema_t schema;
    STAILQ_ENTRY (avro_union_branch_t) branches;
};

struct avro_union_schema_t
{
  struct avro_obj_t obj;
    STAILQ_HEAD (branch, avro_union_branch_t) branches;
};

struct avro_fixed_schema_t
{
  struct avro_obj_t obj;
  const char *name;
  size_t size;
};

struct avro_link_schema_t
{
  struct avro_obj_t obj;
  avro_schema_t to;
};

#define avro_schema_to_record(schema_)  (container_of(schema_, struct avro_record_schema_t, obj))
#define avro_schema_to_enum(schema_)    (container_of(schema_, struct avro_enum_schema_t, obj))
#define avro_schema_to_array(schema_)   (container_of(schema_, struct avro_array_schema_t, obj))
#define avro_schema_to_map(schema_)     (container_of(schema_, struct avro_map_schema_t, obj))
#define avro_schema_to_union(schema_)   (container_of(schema_, struct avro_union_schema_t, obj))
#define avro_schema_to_fixed(schema_)   (container_of(schema_, struct avro_fixed_schema_t, obj))
#define avro_schema_to_link(schema_)    (container_of(schema_, struct avro_link_schema_t, obj))

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
