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

struct avro_union_value
{
  apr_array_header_t *schemas;
  int value_set;
  avro_long_t value_index;
  struct avro_value base_value;
};

void
avro_union_print (struct avro_value *value, FILE * fp)
{
  struct avro_union_value *self =
    container_of (value, struct avro_union_value, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "union(%p)\n", self);
}

static avro_status_t
avro_union_read_skip (struct avro_value *value, struct avro_reader *reader,
		      int skip)
{
  avro_status_t status;
  struct avro_union_value *self =
    container_of (value, struct avro_union_value, base_value);
  struct avro_io_reader *io;
  avro_value *requested_value;

  io = reader->io;
  if (!io)
    {
      return AVRO_FAILURE;
    }

  status = avro_read_long (io, &self->value_index);
  if (status != AVRO_OK)
    {
      return status;
    }

  if (self->value_index < 0 || self->value_index > self->schemas->nelts)
    {
      return AVRO_FAILURE;
    }

  requested_value = ((avro_value **) self->schemas->elts)[self->value_index];
  if (!requested_value)
    {
      return AVRO_FAILURE;
    }

  self->value_set = !skip;

  if (skip)
    {
      status = avro_value_skip_data (requested_value, reader);
    }
  else
    {
      status = avro_value_read_data (requested_value, reader);
    }
  return status;
}

static avro_status_t
avro_union_read (struct avro_value *value, struct avro_reader *reader)
{
  return avro_union_read_skip (value, reader, 0);
}

static avro_status_t
avro_union_skip (struct avro_value *value, struct avro_reader *reader)
{
  return avro_union_read_skip (value, reader, 1);
}

static avro_status_t
avro_union_write (struct avro_value *value, struct avro_writer *writer)
{
  /* TODO */
  return AVRO_FAILURE;
}

static struct avro_value *
avro_union_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		   apr_pool_t * pool, const JSON_value * json)
{
  avro_value *value;
  struct avro_union_value *self;
  int i;

  DEBUG (fprintf (stderr, "Creating a union\n"));

  if (json->type != JSON_ARRAY)
    {
      return NULL;
    }
  self = apr_palloc (pool, sizeof (struct avro_union_value));
  if (!self)
    {
      return NULL;
    }
  self->base_value.type = AVRO_UNION;
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = json;

  self->schemas =
    apr_array_make (pool, json->json_array->nelts, sizeof (avro_value *));
  if (!self->schemas)
    {
      return NULL;
    }
  for (i = 0; i < json->json_array->nelts; i++)
    {
      value =
	avro_value_from_json (ctx, self->base_value.parent,
			      ((JSON_value **) json->json_array->elts)[i]);
      if (!value)
	{
	  /* Invalid union schema */
	  return NULL;
	}
      *(avro_value **) apr_array_push (self->schemas) = value;
    }
  self->value_set = 0;
  return &self->base_value;
}

const struct avro_value_info avro_union_info = {
  .name = L"union",
  .type = AVRO_UNION,
  .private = 0,
  .create = avro_union_create,
  .formats = {{
	       .read_data = avro_union_read,
	       .skip_data = avro_union_skip,
	       .write_data = avro_union_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_union_read,
	       .skip_data = avro_union_skip,
	       .write_data = avro_union_write}},
  .print_info = avro_union_print
};
