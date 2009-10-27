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

struct avro_int_value
{
  avro_int_t value;
  int value_set;
  struct avro_value base_value;
};

static void
avro_int_print (struct avro_value *value, FILE * fp)
{
  struct avro_int_value *self =
    container_of (value, struct avro_int_value, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "int");
  if (self->value_set)
    {
      fprintf (fp, " value=%d", self->value);
    }
  fprintf (fp, "\n");
}

static avro_status_t
avro_int_read_skip (struct avro_value *value, struct avro_channel *channel,
		    int skip)
{
  struct avro_int_value *self =
    container_of (value, struct avro_int_value, base_value);
  struct avro_io *io;
  if (!channel)
    {
      return AVRO_FAILURE;
    }
  io = channel->io;
  if (!io)
    {
      return AVRO_FAILURE;
    }
  self->value_set = !skip;
  return avro_getint (io, &self->value);
}

static avro_status_t
avro_int_read (struct avro_value *value, struct avro_channel *channel)
{
  return avro_int_read_skip (value, channel, 0);
}

static avro_status_t
avro_int_skip (struct avro_value *value, struct avro_channel *channel)
{
  return avro_int_read_skip (value, channel, 1);
}

static avro_status_t
avro_int_write (struct avro_value *value, struct avro_channel *channel)
{
  struct avro_int_value *self =
    container_of (value, struct avro_int_value, base_value);
  struct avro_io *io;
  if (!value || !channel)
    {
      return AVRO_FAILURE;
    }
  io = channel->io;
  if (!io)
    {
      return AVRO_FAILURE;
    }
  return avro_putint (io, &self->value);
}

static struct avro_value *
avro_int_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		 apr_pool_t * pool, const JSON_value * json)
{
  struct avro_int_value *self =
    apr_palloc (pool, sizeof (struct avro_int_value));
  DEBUG (fprintf (stderr, "Creating int\n"));
  if (!self)
    {
      return NULL;
    }
  self->base_value.type = AVRO_INT;
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = json;
  self->value_set = 0;
  return &self->base_value;
}

const struct avro_value_info avro_int_info = {
  .name = L"int",
  .type = AVRO_INT,
  .private = 0,
  .create = avro_int_create,
  .formats = {{
	       .read_data = avro_int_read,
	       .skip_data = avro_int_skip,
	       .write_data = avro_int_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_int_read,
	       .skip_data = avro_int_skip,
	       .write_data = avro_int_write}},
  .print_info = avro_int_print
};
