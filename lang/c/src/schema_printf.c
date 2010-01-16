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

/* print */
struct avro_schema_args_t
{
  int depth;
  FILE *fp;
};

static void
avro_schema_printf_internal (avro_schema_t schema,
			     struct avro_schema_args_t *args);

static void
avro_schema_printf_indent (struct avro_schema_args_t *args)
{
  int i;
  for (i = 0; i < args->depth; i++)
    {
      fprintf (args->fp, "  ");
    }
}

static void
avro_schema_union_print (struct schema_union_t *unionp,
			 struct avro_schema_args_t *args)
{
  fprintf (args->fp, "union");
}

static void
avro_schema_array_print (struct schema_array_t *array,
			 struct avro_schema_args_t *args)
{
  avro_schema_printf_indent (args);
  fprintf (args->fp, "array\n");
  args->depth++;
  avro_schema_printf_internal (array->items, args);
  args->depth--;
}

static void
avro_schema_map_print (struct schema_map_t *map,
		       struct avro_schema_args_t *args)
{
  avro_schema_printf_indent (args);
  fprintf (args->fp, "map\n");
  args->depth++;
  avro_schema_printf_internal (map->values, args);
  args->depth--;
}

static void
avro_schema_fixed_print (struct schema_fixed_t *fixed,
			 struct avro_schema_args_t *args)
{
  fprintf (args->fp, "fixed \"%s\" size=%ld", fixed->name, fixed->size);
}

static void
avro_schema_enum_print (struct schema_enum_t *enump,
			struct avro_schema_args_t *args)
{
  fprintf (args->fp, "enum \"%s\"", enump->name);
}

static void
avro_schema_record_field_print (struct record_field_t *field,
				struct avro_schema_args_t *args)
{
  avro_schema_printf_indent (args);
  fprintf (args->fp, "field \"%s\"\n", field->name);
  args->depth++;
  avro_schema_printf_internal (field->type, args);
  args->depth--;
}

static void
avro_schema_record_print (struct schema_record_t *record,
			  struct avro_schema_args_t *args)
{
  size_t i;
  struct record_field_t *field;
  avro_schema_printf_indent (args);
  fprintf (args->fp, "record \"%s\"\n", record->name);
  args->depth++;
  for (field = TAILQ_FIRST (&record->fields);
       field != NULL; field = TAILQ_NEXT (field, fields))
    {
      avro_schema_record_field_print (field, args);
    }
  args->depth--;
}

const char *
avro_schema_name (const avro_schema_t schema)
{
  if (is_avro_record (schema))
    {
      return (avro_schema_to_record (schema))->name;
    }
  else if (is_avro_enum (schema))
    {
      return (avro_schema_to_enum (schema))->name;
    }
  else if (is_avro_fixed (schema))
    {
      return (avro_schema_to_fixed (schema))->name;
    }
  return NULL;
}

static void
avro_schema_link_print (const struct schema_link_t *link,
			struct avro_schema_args_t *args)
{
  avro_schema_printf_indent (args);
  fprintf (args->fp, "linkto \"%s\"\n", avro_schema_name (link->to));
}

static void
avro_schema_primitive_print (const char *type,
			     struct avro_schema_args_t *args)
{
  avro_schema_printf_indent (args);
  fprintf (args->fp, "primitive %s\n", type);
}

static void
avro_schema_printf_internal (avro_schema_t schema,
			     struct avro_schema_args_t *args)
{
  if (is_avro_link (schema))
    {
      avro_schema_link_print (avro_schema_to_link (schema), args);
    }
  else if (is_avro_record (schema))
    {
      avro_schema_record_print (avro_schema_to_record (schema), args);
    }
  else if (is_avro_enum (schema))
    {
      avro_schema_enum_print (avro_schema_to_enum (schema), args);
    }
  else if (is_avro_fixed (schema))
    {
      avro_schema_fixed_print (avro_schema_to_fixed (schema), args);
    }
  else if (is_avro_map (schema))
    {
      avro_schema_map_print (avro_schema_to_map (schema), args);
    }
  else if (is_avro_array (schema))
    {
      avro_schema_array_print (avro_schema_to_array (schema), args);
    }
  else if (is_avro_union (schema))
    {
      avro_schema_union_print (avro_schema_to_union (schema), args);
    }
  else if (is_avro_string (schema))
    {
      avro_schema_primitive_print ("string", args);
    }
  else if (is_avro_bytes (schema))
    {
      avro_schema_primitive_print ("bytes", args);
    }
  else if (is_avro_int (schema))
    {
      avro_schema_primitive_print ("int", args);
    }
  else if (is_avro_long (schema))
    {
      avro_schema_primitive_print ("long", args);
    }
  else if (is_avro_float (schema))
    {
      avro_schema_primitive_print ("float", args);
    }
  else if (is_avro_double (schema))
    {
      avro_schema_primitive_print ("double", args);
    }
  else if (is_avro_boolean (schema))
    {
      avro_schema_primitive_print ("boolean", args);
    }
  else if (is_avro_null (schema))
    {
      avro_schema_primitive_print ("null", args);
    }
}

void
avro_schema_printf (avro_schema_t schema, FILE * fp)
{
  struct avro_schema_args_t args = {.depth = 0,.fp = fp };
  if (fp)
    {
      avro_schema_printf_internal (schema, &args);
      fprintf (fp, "\n");
    }
}
