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

struct avro_map_value
{
  apr_hash_t *map;
  struct avro_value *keys;
  struct avro_value *values;
  struct avro_value base_value;
};

static void
avro_map_print (struct avro_value *value, FILE * fp)
{
  struct avro_map_value *self =
    container_of (value, struct avro_map_value, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "map(%p) key/value\n", self);
  avro_value_print_info (self->keys, fp);
  avro_value_print_info (self->values, fp);
}

static avro_status_t
avro_map_read (struct avro_value *value, struct avro_channel *channel)
{
  struct avro_map_value *self =
    container_of (value, struct avro_map_value, base_value);
  return AVRO_OK;
}

static avro_status_t
avro_map_skip (struct avro_value *value, struct avro_channel *channel)
{
  struct avro_map_value *self =
    container_of (value, struct avro_map_value, base_value);
  return AVRO_OK;
}

static avro_status_t
avro_map_write (struct avro_value *value, struct avro_channel *channel)
{
  struct avro_map_value *self =
    container_of (value, struct avro_map_value, base_value);
  return AVRO_OK;
}

static struct avro_value *
avro_map_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		 apr_pool_t * pool, const JSON_value * json)
{
  struct avro_map_value *self;
  const JSON_value *keys;
  const JSON_value *values;

  DEBUG (fprintf (stderr, "Creating map\n"));
  self = apr_palloc (pool, sizeof (struct avro_map_value));
  if (!self)
    {
      return NULL;
    }
  self->base_value.type = AVRO_MAP;
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = json;

  /* collect and save required keys */
  keys = json_attr_get (json, L"keys");
  if (keys)
    {
      self->keys = avro_value_from_json (ctx, &self->base_value, keys);
      if (!self->keys)
	{
	  return NULL;
	}
    }
  else
    {
      /* TODO: should keys default to string? */
      self->keys =
	avro_value_registry[AVRO_STRING]->create (ctx, &self->base_value,
						  pool, NULL);
    }

  /* collect and save required values */
  values = json_attr_get (json, L"values");
  if (!values)
    {
      return NULL;
    }
  self->values = avro_value_from_json (ctx, &self->base_value, values);
  if (!self->values)
    {
      return NULL;
    }
  return &self->base_value;
}

const struct avro_value_info avro_map_info = {
  .name = L"map",
  .type = AVRO_MAP,
  .private = 0,
  .create = avro_map_create,
  .formats = {{
	       .read_data = avro_map_read,
	       .skip_data = avro_map_skip,
	       .write_data = avro_map_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_map_read,
	       .skip_data = avro_map_skip,
	       .write_data = avro_map_write}},
  .print_info = avro_map_print
};
