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
#include "util/dump.h"

struct avro_fixed_value
{
  avro_string_t name;
  avro_long_t size;
  void *value;

  int value_set;

  avro_value base_value;
};

static void
avro_fixed_print (struct avro_value *value, FILE * fp)
{
  struct avro_fixed_value *self =
    container_of (value, struct avro_fixed_value, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "fixed(%p) name=%ls size=%lld ", self, self->name, self->size);
  if (self->value_set)
    {
      dump (fp, self->value, self->size);
    }
}

static avro_status_t
avro_fixed_read_skip (struct avro_value *value, struct avro_reader *reader,
		      int skip)
{
  struct avro_fixed_value *self =
    container_of (value, struct avro_fixed_value, base_value);
  avro_io_reader *io;

  if (!reader)
    {
      return AVRO_FAILURE;
    }
  io = reader->io;
  if (!io)
    {
      return AVRO_FAILURE;
    }
  self->value_set = !skip;
  return io->read (io, self->value, self->size);
}

static avro_status_t
avro_fixed_read (struct avro_value *value, struct avro_reader *reader)
{
  return avro_fixed_read_skip (value, reader, 0);
}

static avro_status_t
avro_fixed_skip (struct avro_value *value, struct avro_reader *reader)
{
  return avro_fixed_read_skip (value, reader, 1);
}

static avro_status_t
avro_fixed_write (struct avro_value *value, struct avro_writer *writer)
{
  /* TODO */
  return AVRO_FAILURE;
}

static struct avro_value *
avro_fixed_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		   apr_pool_t * pool, const JSON_value * json)
{
  struct avro_fixed_value *self;
  const JSON_value *size;
  const JSON_value *name;

  DEBUG (fprintf (stderr, "Creating fixed\n"));
  self = apr_palloc (pool, sizeof (struct avro_fixed_value));
  if (!self)
    {
      return NULL;
    }
  self->base_value.type = AVRO_FIXED;
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = json;

  /* collect and save required size */
  size = json_attr_get_check_type (json, L"size", JSON_NUMBER);
  if (!size)
    {
      return NULL;
    }
  self->size = size->json_number;
  if (self->size < 0)
    {
      return NULL;
    }

  /* collect and save the require name */
  name = json_attr_get_check_type (json, L"name", JSON_STRING);
  if (!name)
    {
      return NULL;
    }
  self->name = name->json_string;
  /* register self with named objects */
  apr_hash_set (ctx->named_objects, self->name,
		wcslen (self->name) * sizeof (wchar_t), json);

  self->value = apr_palloc (pool, self->size);
  if (!self->value)
    {
      return NULL;
    }

  self->value_set = 0;
  return &self->base_value;
}

const struct avro_value_info avro_fixed_info = {
  .name = L"fixed",
  .type = AVRO_FIXED,
  .private = 0,
  .create = avro_fixed_create,
  .formats = {{
	       .read_data = avro_fixed_read,
	       .skip_data = avro_fixed_skip,
	       .write_data = avro_fixed_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_fixed_read,
	       .skip_data = avro_fixed_skip,
	       .write_data = avro_fixed_write}},
  .print_info = avro_fixed_print
};
