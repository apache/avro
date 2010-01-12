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

struct avro_string_value
{
  apr_pool_t *pool;
  avro_string_t value;
  int value_set;
  struct avro_value base_value;
};

static void
avro_string_print (struct avro_value *value, FILE * fp)
{
  struct avro_string_value *self =
    container_of (value, struct avro_string_value, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "string(%p)", self);
  if (self->value_set)
    {
      fprintf (fp, " value=%ls", self->value);
    }
  fprintf (fp, "\n");
}

static avro_status_t
avro_string_read (struct avro_value *value, struct avro_reader *reader)
{
  struct avro_io_reader *io;
  struct avro_string_value *self =
    container_of (value, struct avro_string_value, base_value);
  if (!reader)
    {
      return AVRO_FAILURE;
    }
  io = reader->io;
  if (!io)
    {
      return AVRO_FAILURE;
    }
  apr_pool_clear (self->pool);
  self->value_set = 1;
  return avro_read_string (io, self->pool, &self->value);
}

static avro_status_t
avro_string_skip (struct avro_value *value, struct avro_reader *reader)
{
  avro_status_t status;
  avro_long_t len;
  struct avro_io_reader *io;
  struct avro_string_value *self =
    container_of (value, struct avro_string_value, base_value);

  self->value_set = 0;
  if (!reader)
    {
      return AVRO_FAILURE;
    }
  io = reader->io;
  if (!io)
    {
      return AVRO_FAILURE;
    }
  status = avro_read_long (io, &len);
  if (status != AVRO_OK)
    {
      return status;
    }
  if (len < 0)
    {
      return AVRO_FAILURE;
    }
  return io->skip (io, len);
}

static avro_status_t
avro_string_write (struct avro_value *value, struct avro_writer *writer)
{
  struct avro_io_writer *io;
  struct avro_string_value *self =
    container_of (value, struct avro_string_value, base_value);
  if (!writer)
    {
      return AVRO_FAILURE;
    }
  io = writer->io;
  if (!io)
    {
      return AVRO_FAILURE;
    }
  return avro_write_string (io, self->pool, self->value);
}

static struct avro_value *
avro_string_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		    apr_pool_t * pool, const JSON_value * json)
{
  struct avro_string_value *self =
    apr_palloc (pool, sizeof (struct avro_string_value));
  DEBUG (fprintf (stderr, "Creating a string\n"));
  if (!self)
    {
      return NULL;
    }
  self->base_value.type = AVRO_STRING;
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = json;
  self->value_set = 0;
  if (apr_pool_create (&self->pool, pool) != APR_SUCCESS)
    {
      return NULL;
    }
  return &self->base_value;
}

const struct avro_value_module avro_string_module = {
  .name = L"string",
  .type = AVRO_STRING,
  .private = 0,
  .create = avro_string_create,
  .formats = {{
	       .read_data = avro_string_read,
	       .skip_data = avro_string_skip,
	       .write_data = avro_string_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_string_read,
	       .skip_data = avro_string_skip,
	       .write_data = avro_string_write}},
  .print_info = avro_string_print
};
