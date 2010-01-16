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

#include "schema.h"
#include <string.h>

static int
schema_record_equal (struct schema_record_t *a, struct schema_record_t *b)
{
  struct record_field_t *field_a, *field_b;
  if (strcmp (a->name, b->name))
    {
      /* They have different names */
      return 0;
    }

  for (field_a = TAILQ_FIRST (&a->fields),
       field_b = TAILQ_FIRST (&b->fields);
       !(field_a == NULL && field_b == NULL);
       field_a = TAILQ_NEXT (field_a, fields),
       field_b = TAILQ_NEXT (field_b, fields))
    {
      if (field_a == NULL || field_b == NULL)
	{
	  return 0;		/* different num fields */
	}
      if (strcmp (field_a->name, field_b->name))
	{
	  /* They have fields with different names */
	  return 0;
	}
      if (!avro_schema_equal (field_a->type, field_b->type))
	{
	  /* They have fields with different schemas */
	  return 0;
	}
    }
  return 1;
}

static int
schema_enum_equal (struct schema_enum_t *a, struct schema_enum_t *b)
{
  struct enum_symbol_t *sym_a, *sym_b;

  if (strcmp (a->name, b->name))
    {
      /* They have different names */
      return 0;
    }
  for (sym_a = TAILQ_FIRST (&a->symbols),
       sym_b = TAILQ_FIRST (&b->symbols);
       !(sym_a == NULL && sym_b == NULL);
       sym_a = TAILQ_NEXT (sym_a, symbols),
       sym_b = TAILQ_NEXT (sym_b, symbols))
    {
      if (sym_a == NULL || sym_b == NULL)
	{
	  return 0;		/* different num symbols */
	}
      if (strcmp (sym_a->symbol, sym_b->symbol))
	{
	  /* They have different symbol names */
	  return 0;
	}
    }
  return 1;
}

static int
schema_fixed_equal (struct schema_fixed_t *a, struct schema_fixed_t *b)
{
  if (strcmp (a->name, b->name))
    {
      /* They have different names */
      return 0;
    }
  return (a->size == b->size);
}

static int
schema_map_equal (struct schema_map_t *a, struct schema_map_t *b)
{
  return avro_schema_equal (a->values, b->values);
}

static int
schema_array_equal (struct schema_array_t *a, struct schema_array_t *b)
{
  return avro_schema_equal (a->items, b->items);
}

static int
schema_union_equal (struct schema_union_t *a, struct schema_union_t *b)
{
  struct union_schema_t *schema_a, *schema_b;

  for (schema_a = TAILQ_FIRST (&a->schemas),
       schema_b = TAILQ_FIRST (&b->schemas);
       !(schema_a == NULL && schema_b == NULL);
       schema_a = TAILQ_NEXT (schema_a, schemas),
       schema_b = TAILQ_NEXT (schema_b, schemas))
    {
      if (schema_a == NULL || schema_b == NULL)
	{
	  return 0;		/* different num symbols */
	}
      if (!avro_schema_equal (schema_a->schema, schema_b->schema))
	{
	  /* They don't have the same schema types */
	  return 0;
	}
    }
  return 1;
}

static int
schema_link_equal (struct schema_link_t *a, struct schema_link_t *b)
{
  /* NOTE: links can only be used for named types. They are used
     in recursive schemas so we just check the name of the 
     schema pointed to instead of a deep check.  Otherwise, we
     recurse forever... */
  return (strcmp (avro_schema_name (a->to), avro_schema_name (b->to)) == 0);
}

int
avro_schema_equal (avro_schema_t a, avro_schema_t b)
{
  if (!a || !b)
    {
      /* this is an error. protecting from segfault. */
      return 0;
    }
  else if (a == b)
    {
      /* an object is equal to itself */
      return 1;
    }
  else if (avro_typeof (a) != avro_typeof (b))
    {
      return 0;
    }
  else if (is_avro_record (a))
    {
      return schema_record_equal (avro_schema_to_record (a),
				  avro_schema_to_record (b));
    }
  else if (is_avro_enum (a))
    {
      return schema_enum_equal (avro_schema_to_enum (a),
				avro_schema_to_enum (b));
    }
  else if (is_avro_fixed (a))
    {
      return schema_fixed_equal (avro_schema_to_fixed (a),
				 avro_schema_to_fixed (b));
    }
  else if (is_avro_map (a))
    {
      return schema_map_equal (avro_schema_to_map (a),
			       avro_schema_to_map (b));
    }
  else if (is_avro_array (a))
    {
      return schema_array_equal (avro_schema_to_array (a),
				 avro_schema_to_array (b));
    }
  else if (is_avro_union (a))
    {
      return schema_union_equal (avro_schema_to_union (a),
				 avro_schema_to_union (b));
    }
  else if (is_avro_link (a))
    {
      return schema_link_equal (avro_schema_to_link (a),
				avro_schema_to_link (b));
    }
  return 1;
}
