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

struct avro_decorator_value
{
  struct avro_value_ctx *ctx;
  struct avro_value *decoratee;
  struct avro_value base_value;
};

static void
avro_decorator_print (struct avro_value *value, FILE * fp)
{
  struct avro_decorator_value *self =
    container_of (value, struct avro_decorator_value, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "decorator(%p)\n", self);
  avro_value_print_info (self->decoratee, fp);
}

static avro_status_t
avro_decorator_read_skip (struct avro_value *value,
			  struct avro_reader *reader, int skip)
{
  struct avro_decorator_value *self =
    container_of (value, struct avro_decorator_value, base_value);
  /* Create a new decoratee */
  self->decoratee = avro_value_from_json (self->ctx, value, value->schema);
  if (!self->decoratee)
    {
      return AVRO_FAILURE;
    }
  if (skip)
    {
      return avro_value_skip_data (self->decoratee, reader);
    }
  return avro_value_read_data (self->decoratee, reader);
}

static avro_status_t
avro_decorator_read (struct avro_value *value, struct avro_reader *reader)
{
  return avro_decorator_read_skip (value, reader, 0);
}

static avro_status_t
avro_decorator_skip (struct avro_value *value, struct avro_reader *reader)
{
  return avro_decorator_read_skip (value, reader, 1);
}

static avro_status_t
avro_decorator_write (struct avro_value *value, struct avro_writer *writer)
{
  return AVRO_FAILURE;
}

/* Used for recursive schemas */
struct avro_value *
avro_decorator_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		       apr_pool_t * pool, const JSON_value * json)
{
  struct avro_decorator_value *self =
    apr_palloc (pool, sizeof (struct avro_decorator_value));
  DEBUG (fprintf (stderr, "Creating a decorator\n"));
  if (!self)
    {
      return NULL;
    }
  /* Save away the context */
  self->ctx = ctx;
  self->base_value.type = AVRO_DECORATOR;
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = json;
  return &self->base_value;
}

const struct avro_value_module avro_decorator_module = {
  .name = L"decorator",
  .type = AVRO_DECORATOR,
  .private = 1,
  .create = avro_decorator_create,
  .formats = {{
	       .read_data = avro_decorator_read,
	       .skip_data = avro_decorator_skip,
	       .write_data = avro_decorator_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_decorator_read,
	       .skip_data = avro_decorator_skip,
	       .write_data = avro_decorator_write}},
  .print_info = avro_decorator_print
};
