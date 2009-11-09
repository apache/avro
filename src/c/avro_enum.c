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
#include "avro_private.h"

struct avro_avro_enum
{
  avro_string_t name;
  avro_long_t offset;
  apr_array_header_t *symbols;
  int value_set;
  struct avro_value base_value;
};

static void
avro_enum_print (struct avro_value *value, FILE * fp)
{
  struct avro_avro_enum *self =
    container_of (value, struct avro_avro_enum, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "enum(%p) name=%ls", self, self->name);
  if (self->value_set)
    {
      fprintf (fp, " value=%ls",
	       ((avro_string_t *) self->symbols->elts)[self->offset]);
    }
  fprintf (fp, "\n");
}

static avro_status_t
avro_enum_read_skip (struct avro_value *value, struct avro_reader *reader,
		     int skip)
{
  avro_status_t status;
  struct avro_io_reader *io;
  struct avro_avro_enum *self =
    container_of (value, struct avro_avro_enum, base_value);

  if (!reader)
    {
      return AVRO_FAILURE;
    }
  io = reader->io;
  if (!io)
    {
      return AVRO_FAILURE;
    }
  /* Read the offset */
  status = avro_read_long (io, &self->offset);
  if (status != AVRO_OK)
    {
      return status;
    }
  if (self->offset > self->symbols->nelts)
    {
      /* Offset outside of our symbol table */
      return AVRO_FAILURE;
    }
  self->value_set = !skip;
  return AVRO_OK;
}

static avro_status_t
avro_enum_read (struct avro_value *value, struct avro_reader *reader)
{
  return avro_enum_read_skip (value, reader, 0);
}

static avro_status_t
avro_enum_skip (struct avro_value *value, struct avro_reader *reader)
{
  return avro_enum_read_skip (value, reader, 1);
}

static avro_status_t
avro_enum_write (struct avro_value *value, struct avro_writer *writer)
{
/* TODO
  struct avro_avro_enum *self =
    container_of (value, struct avro_avro_enum, base_value);
*/
  return AVRO_OK;
}

static struct avro_value *
avro_enum_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		  apr_pool_t * pool, const JSON_value * json)
{
  struct avro_avro_enum *self;
  const JSON_value *name;
  const JSON_value *symbols;
  int i;

  DEBUG (fprintf (stderr, "Creating enum\n"));
  self = apr_palloc (pool, sizeof (struct avro_avro_enum));
  if (!self)
    {
      return NULL;
    }
  self->base_value.type = AVRO_ENUM;
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = json;

  /* collect and save required name */
  name = json_attr_get_check_type (json, L"name", JSON_STRING);
  if (!name)
    {
      return NULL;
    }
  self->name = name->json_string;

  /* register self with named objects */
  apr_hash_set (ctx->named_objects, self->name,
		wcslen (self->name) * sizeof (wchar_t), json);

  /* collect and save required symbols */
  symbols = json_attr_get_check_type (json, L"symbols", JSON_ARRAY);
  if (!symbols || symbols->json_array->nelts == 0)
    {
      return NULL;
    }
  self->symbols = apr_array_make (pool, 8, sizeof (avro_string_t));
  for (i = 0; i < symbols->json_array->nelts; i++)
    {
      const JSON_value *symbol =
	((JSON_value **) symbols->json_array->elts)[i];
      if (symbol->type != JSON_STRING)
	{
	  return NULL;
	}
      *(avro_string_t *) apr_array_push (self->symbols) = symbol->json_string;
    }
  self->value_set = 0;
  return &self->base_value;
}

const struct avro_value_info avro_enum_info = {
  .name = L"enum",
  .type = AVRO_ENUM,
  .private = 0,
  .create = avro_enum_create,
  .formats = {{
	       .read_data = avro_enum_read,
	       .skip_data = avro_enum_skip,
	       .write_data = avro_enum_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_enum_read,
	       .skip_data = avro_enum_skip,
	       .write_data = avro_enum_write}},
  .print_info = avro_enum_print
};
