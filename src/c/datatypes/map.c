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
  apr_pool_t *pool;
  const JSON_value *key_schema;
  const JSON_value *value_schema;

  /* Need the ctx to dupe value */
  struct avro_value_ctx *ctx;

  struct avro_value base_value;
};

static void
avro_map_print (struct avro_value *value, FILE * fp)
{
  struct avro_map_value *self =
    container_of (value, struct avro_map_value, base_value);

  avro_value_indent (value, fp);
  fprintf (fp, "map(%p)", self);
  if (self->map)
    {
      int i;
      apr_pool_t *subpool;
      apr_hash_index_t *hi;
      apr_ssize_t len;

      fprintf (fp, " start\n");
      apr_pool_create (&subpool, self->pool);
      for (i = 0, hi = apr_hash_first (subpool, self->map); hi;
	   hi = apr_hash_next (hi), i++)
	{
	  avro_value *key;
	  avro_value *value;
	  apr_hash_this (hi, (void *) &key, &len, (void *) &value);
	  avro_value_print_info (key, fp);
	  avro_value_print_info (value, fp);
	}
      apr_pool_clear (subpool);
      avro_value_indent (value, fp);
      fprintf (fp, "map(%p) end", self);
    }
  fprintf (fp, "\n");
}

static avro_status_t
avro_map_read_skip (struct avro_value *value, struct avro_reader *reader,
		    int skip)
{
  struct avro_map_value *self =
    container_of (value, struct avro_map_value, base_value);
  avro_status_t status;
  avro_long_t i, count;
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
  status = avro_read_long (io, &count);
  if (status != AVRO_OK)
    {
      return status;
    }
  apr_pool_clear (self->pool);
  self->map = skip ? NULL : apr_hash_make (self->pool);

  while (count > 0)
    {
      for (i = 0; i < count; i++)
	{
	  avro_value *key =
	    avro_value_from_json (self->ctx, self->base_value.parent,
				  self->key_schema);
	  avro_value *value =
	    avro_value_from_json (self->ctx, self->base_value.parent,
				  self->value_schema);
	  if (!key || !value)
	    {
	      return AVRO_FAILURE;
	    }
	  status = avro_value_read_data (key, reader);
	  if (status != AVRO_OK)
	    {
	      return status;
	    }
	  status = avro_value_read_data (value, reader);
	  if (status != AVRO_OK)
	    {
	      return status;
	    }
	  apr_hash_set (self->map, key, sizeof (struct avro_value), value);
	}

      status = avro_read_long (io, &count);
      if (status != AVRO_OK)
	{
	  return status;
	}
    }

  return AVRO_OK;
}

static avro_status_t
avro_map_read (struct avro_value *value, struct avro_reader *reader)
{
  return avro_map_read_skip (value, reader, 0);
}

static avro_status_t
avro_map_skip (struct avro_value *value, struct avro_reader *reader)
{
  return avro_map_read_skip (value, reader, 1);
}

static avro_status_t
avro_map_write (struct avro_value *value, struct avro_writer *writer)
{
  /* TODO */
  return AVRO_FAILURE;
}

static struct avro_value *
avro_map_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		 apr_pool_t * pool, const JSON_value * json)
{
  struct avro_map_value *self;
  avro_value *value;
  apr_pool_t *tmp_pool;

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

  /* map keys are always assumed to be strings */
  char *default_key = "{\"type\":\"string\"}";
  self->key_schema = JSON_parse (pool, default_key, strlen (default_key));
  if (!self->key_schema)
    {
      return NULL;
    }

  /* collect and save required values */
  self->value_schema = json_attr_get (json, L"values");
  if (!self->value_schema)
    {
      return NULL;
    }
  /* verify that the value schema is valid */
  apr_pool_create (&tmp_pool, pool);
  value = avro_value_from_json (ctx, parent, self->value_schema);
  apr_pool_clear (tmp_pool);
  if (!value)
    {
      return NULL;
    }

  /* Create a value pool */
  if (apr_pool_create (&self->pool, pool) != APR_SUCCESS)
    {
      return NULL;
    }
  self->map = NULL;
  self->ctx = ctx;
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
