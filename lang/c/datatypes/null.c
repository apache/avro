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

static avro_status_t
avro_null_noop_reader (struct avro_value *value, struct avro_reader *reader)
{
  return AVRO_OK;
}

static avro_status_t
avro_null_noop_writer (struct avro_value *value, struct avro_writer *writer)
{
  return AVRO_OK;
}

static void
avro_null_print (struct avro_value *value, FILE * fp)
{
  avro_value_indent (value, fp);
  fprintf (fp, "null(%p)\n", value);
}

static struct avro_value *
avro_null_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		  apr_pool_t * pool, const JSON_value * json)
{
  struct avro_value *self = apr_palloc (pool, sizeof (struct avro_value));
  DEBUG (fprintf (stderr, "Creating null\n"));
  if (!self)
    {
      return NULL;
    }
  self->type = AVRO_NULL;
  self->pool = pool;
  self->parent = parent;
  self->schema = json;
  return self;
}

const struct avro_value_module avro_null_module = {
  .name = L"null",
  .type = AVRO_NULL,
  .private = 0,
  .create = avro_null_create,
  .formats = {{
	       .read_data = avro_null_noop_reader,
	       .skip_data = avro_null_noop_reader,
	       .write_data = avro_null_noop_writer},
	      {
	       /* TODO: import/export */
	       .read_data = avro_null_noop_reader,
	       .skip_data = avro_null_noop_reader,
	       .write_data = avro_null_noop_writer}},
  .print_info = avro_null_print
};
