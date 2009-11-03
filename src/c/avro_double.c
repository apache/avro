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

struct avro_double_value
{
  double value;
  int value_set;
  struct avro_value base_value;
};

static void
avro_double_print (struct avro_value *value, FILE * fp)
{
  struct avro_double_value *self =
    container_of (value, struct avro_double_value, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "double");
  if (self->value_set)
    {
      fprintf (fp, " value=%f", self->value);
    }
  fprintf (fp, "\n");
}

static avro_status_t
avro_double_read (struct avro_value *value, struct avro_reader *reader)
{
  double *dp;
  struct avro_double_value *self =
    container_of (value, struct avro_double_value, base_value);
  if (!reader)
    {
      return AVRO_FAILURE;
    }
  self->value_set = 1;
  dp = &self->value;
  return avro_read_double (reader->io, dp);
}

static avro_status_t
avro_double_skip (struct avro_value *value, struct avro_reader *reader)
{
  double d;
  double *dp = &d;
  struct avro_double_value *self =
    container_of (value, struct avro_double_value, base_value);
  if (!reader)
    {
      return AVRO_FAILURE;
    }
  self->value_set = 0;
  return avro_read_double (reader->io, dp);
}

static avro_status_t
avro_double_write (struct avro_value *value, struct avro_writer *writer)
{
  struct avro_double_value *self =
    container_of (value, struct avro_double_value, base_value);
  if (!writer)
    {
      return AVRO_FAILURE;
    }
  return avro_write_double (writer->io, self->value);
}

static struct avro_value *
avro_double_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		    apr_pool_t * pool, const JSON_value * json)
{
  struct avro_double_value *self =
    apr_palloc (pool, sizeof (struct avro_double_value));
  DEBUG (fprintf (stderr, "Creating double\n"));
  if (!self)
    {
      return NULL;
    }
  self->base_value.type = AVRO_DOUBLE;
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = json;
  self->value_set = 0;
  return &self->base_value;
}

const struct avro_value_info avro_double_info = {
  .name = L"double",
  .type = AVRO_DOUBLE,
  .private = 0,
  .create = avro_double_create,
  .formats = {{
	       .read_data = avro_double_read,
	       .skip_data = avro_double_skip,
	       .write_data = avro_double_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_double_read,
	       .skip_data = avro_double_skip,
	       .write_data = avro_double_write}},
  .print_info = avro_double_print
};
