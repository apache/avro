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

struct avro_long_value
{
  avro_long_t value;
  int value_set;
  struct avro_value base_value;
};

static void
avro_long_print (struct avro_value *value, FILE * fp)
{
  struct avro_long_value *self =
    container_of (value, struct avro_long_value, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "long");
  if (self->value_set)
    {
      fprintf (fp, " value=%ld", self->value);
    }
  fprintf (fp, "\n");
}

static avro_status_t
avro_long_read_skip (struct avro_value *value, struct avro_reader *reader,
		     int skip)
{
  struct avro_long_value *self =
    container_of (value, struct avro_long_value, base_value);
  struct avro_io_reader *io;
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
  return avro_read_long (io, &self->value);
}

static avro_status_t
avro_long_read (struct avro_value *value, struct avro_reader *reader)
{
  return avro_long_read_skip (value, reader, 0);
}

static avro_status_t
avro_long_skip (struct avro_value *value, struct avro_reader *reader)
{
  return avro_long_read_skip (value, reader, 1);
}

static avro_status_t
avro_long_write (struct avro_value *value, struct avro_writer *writer)
{
  struct avro_long_value *self =
    container_of (value, struct avro_long_value, base_value);
  struct avro_io_writer *io;
  if (!value || !writer)
    {
      return AVRO_FAILURE;
    }
  io = writer->io;
  if (!io)
    {
      return AVRO_FAILURE;
    }
  return avro_write_long (io, &self->value);
}

static struct avro_value *
avro_long_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		  apr_pool_t * pool, const JSON_value * json)
{
  struct avro_long_value *self =
    apr_palloc (pool, sizeof (struct avro_long_value));
  DEBUG (fprintf (stderr, "Creating long\n"));
  if (!self)
    {
      return NULL;
    }
  self->base_value.type = AVRO_LONG;
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = json;
  self->value_set = 0;
  return &self->base_value;
}

const struct avro_value_info avro_long_info = {
  .name = L"long",
  .type = AVRO_LONG,
  .private = 0,
  .create = avro_long_create,
  .formats = {{
	       .read_data = avro_long_read,
	       .skip_data = avro_long_skip,
	       .write_data = avro_long_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_long_read,
	       .skip_data = avro_long_skip,
	       .write_data = avro_long_write}},
  .print_info = avro_long_print
};
