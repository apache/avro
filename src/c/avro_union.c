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
avro_union_read (struct avro_value *value, struct avro_reader *reader)
{
  struct avro_union_value *self =
    container_of (value, struct avro_union_value, base_value);
  return AVRO_OK;
}

static avro_status_t
avro_union_skip (struct avro_value *value, struct avro_reader *reader)
{
  struct avro_union_value *self =
    container_of (value, struct avro_union_value, base_value);
  return AVRO_OK;
}

static avro_status_t
avro_union_write (struct avro_value *value, struct avro_writer *writer)
{
  struct avro_union_value *self =
    container_of (value, struct avro_union_value, base_value);
  return AVRO_OK;
}

static struct avro_value *
avro_union_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		   apr_pool_t * pool, const JSON_value * json)
{
  struct avro_union_value *self;
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
  /* TODO: check the schemas ... and save them */
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
