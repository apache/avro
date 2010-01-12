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

struct avro_float_value
{
  float value;
  int value_set;
  struct avro_value base_value;
};

static void
avro_float_print (struct avro_value *value, FILE * fp)
{
  struct avro_float_value *self =
    container_of (value, struct avro_float_value, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "float(%p)", self);
  if (self->value_set)
    {
      fprintf (fp, " value=%f", self->value);
    }
  fprintf (fp, "\n");
}

static avro_status_t
avro_float_read (struct avro_value *value, struct avro_reader *reader)
{
  float *fp;
  struct avro_float_value *self =
    container_of (value, struct avro_float_value, base_value);
  if (!reader)
    {
      return AVRO_FAILURE;
    }
  fp = &self->value;
  self->value_set = 1;
  return avro_read_float (reader->io, fp);
}

static avro_status_t
avro_float_skip (struct avro_value *value, struct avro_reader *reader)
{
  float f;
  float *fp = &f;
  struct avro_float_value *self =
    container_of (value, struct avro_float_value, base_value);
  if (!reader)
    {
      return AVRO_FAILURE;
    }
  self->value_set = 0;
  return avro_read_float (reader->io, fp);
}

static avro_status_t
avro_float_write (struct avro_value *value, struct avro_writer *writer)
{
  struct avro_float_value *self =
    container_of (value, struct avro_float_value, base_value);
  if (!writer)
    {
      return AVRO_FAILURE;
    }
  return avro_write_float (writer->io, self->value);
}

static struct avro_value *
avro_float_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		   apr_pool_t * pool, const JSON_value * json)
{
  struct avro_float_value *self =
    apr_palloc (pool, sizeof (struct avro_float_value));
  DEBUG (fprintf (stderr, "Creating float\n"));
  if (!self)
    {
      return NULL;
    }
  self->base_value.type = AVRO_FLOAT;
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = json;
  self->value_set = 0;
  return &self->base_value;
}

const struct avro_value_module avro_float_module = {
  .name = L"float",
  .type = AVRO_FLOAT,
  .private = 0,
  .create = avro_float_create,
  .formats = {{
	       .read_data = avro_float_read,
	       .skip_data = avro_float_skip,
	       .write_data = avro_float_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_float_read,
	       .skip_data = avro_float_skip,
	       .write_data = avro_float_write}},
  .print_info = avro_float_print
};
