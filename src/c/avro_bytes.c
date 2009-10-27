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
#include "dump.h"

struct avro_bytes_value
{
  char *value;
  int value_set;
  avro_long_t size;
  avro_value base_value;
  apr_pool_t *pool;
};

static void
avro_bytes_print (struct avro_value *value, FILE * fp)
{
  struct avro_bytes_value *self =
    container_of (value, struct avro_bytes_value, base_value);

  avro_value_indent (value, fp);
  fprintf (fp, "bytes");
  if (self->value_set)
    {
      fprintf (fp, " size=%ld ", self->size);
      dump (fp, self->value, self->size);
    }
  fprintf (fp, "\n");
}

static avro_status_t
avro_bytes_read (struct avro_value *value, struct avro_channel *channel)
{
  struct avro_io *io;
  struct avro_bytes_value *self =
    container_of (value, struct avro_bytes_value, base_value);
  if (!channel)
    {
      return AVRO_FAILURE;
    }
  io = channel->io;
  if (!io)
    {
      return AVRO_FAILURE;
    }
  /* Flush old data to make room for new */
  self->value_set = 1;
  apr_pool_clear (self->pool);
  return avro_getbytes (io, self->pool, &self->value, &self->size);
}

static avro_status_t
avro_bytes_skip (struct avro_value *value, struct avro_channel *channel)
{
  avro_status_t status;
  struct avro_io *io;
  avro_long_t len;
  struct avro_bytes_value *self =
    container_of (value, struct avro_bytes_value, base_value);

  self->value_set = 0;
  if (!channel)
    {
      return AVRO_FAILURE;
    }
  io = channel->io;
  if (!io)
    {
      return AVRO_FAILURE;
    }
  status = avro_getlong (io, &len);
  if (status != AVRO_OK)
    {
      return AVRO_FAILURE;
    }
  return io->skip (io, len);
}

static avro_status_t
avro_bytes_write (struct avro_value *value, struct avro_channel *channel)
{
  struct avro_bytes_value *self =
    container_of (value, struct avro_bytes_value, base_value);
  return AVRO_OK;
}

static struct avro_value *
avro_bytes_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		   apr_pool_t * pool, const JSON_value * json)
{
  struct avro_bytes_value *self =
    apr_palloc (pool, sizeof (struct avro_bytes_value));
  DEBUG (fprintf (stderr, "Creating bytes\n"));
  if (!self)
    {
      return NULL;
    }
  self->base_value.type = AVRO_BYTES;
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

const struct avro_value_info avro_bytes_info = {
  .name = L"bytes",
  .type = AVRO_BYTES,
  .private = 0,
  .create = avro_bytes_create,
  .formats = {{
	       .read_data = avro_bytes_read,
	       .skip_data = avro_bytes_skip,
	       .write_data = avro_bytes_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_bytes_read,
	       .skip_data = avro_bytes_skip,
	       .write_data = avro_bytes_write}},
  .print_info = avro_bytes_print
};
