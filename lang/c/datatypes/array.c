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

struct avro_array_value
{
  apr_array_header_t *values;
  const JSON_value *item_schema;

  apr_pool_t *pool;

  /* Need the ctx to dupe value */
  struct avro_value_ctx *ctx;

  struct avro_value base_value;
};

static void
avro_array_print (struct avro_value *value, FILE * fp)
{
  int i;
  struct avro_array_value *self =
    container_of (value, struct avro_array_value, base_value);

  avro_value_indent (value, fp);
  fprintf (fp, "array(%p) with %d items\n", self,
	   self->values ? self->values->nelts : 0);
  if (self->values)
    {
      for (i = 0; i < self->values->nelts; i++)
	{
	  struct avro_value *item =
	    ((struct avro_value **) self->values->elts)[i];
	  avro_value_print_info (item, fp);
	}
    }
}

static avro_status_t
avro_array_read_skip (struct avro_value *value, struct avro_reader *reader,
		      int skip)
{
  avro_status_t status;
  avro_long_t i, count;
  struct avro_array_value *self =
    container_of (value, struct avro_array_value, base_value);
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

  if (count < 0)
    {
      /* TODO */
      return AVRO_FAILURE;
    }

  /* Clear the pool to of any previous state */
  apr_pool_clear (self->pool);

  self->values =
    skip ? NULL : apr_array_make (self->pool, count,
				  sizeof (struct avro_value *));

  while (count > 0)
    {
      /* Read in the count number of items */
      if (skip)
	{
	  struct avro_value *item =
	    avro_value_from_json (self->ctx, self->base_value.parent,
				  self->item_schema);
	  for (i = 0; i < count; i++)
	    {
	      status = avro_value_skip_data (item, reader);
	      if (status != AVRO_OK)
		{
		  return status;
		}
	    }
	}
      else
	{
	  for (i = 0; i < count; i++)
	    {
	      struct avro_value *item =
		avro_value_from_json (self->ctx, self->base_value.parent,
				      self->item_schema);
	      if (!item)
		{
		  return AVRO_FAILURE;
		}
	      status = avro_value_read_data (item, reader);
	      if (status != AVRO_OK)
		{
		  return status;
		}
	      /* Save the item */
	      *(struct avro_value **) apr_array_push (self->values) = item;
	    }
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
avro_array_read (struct avro_value *value, struct avro_reader *reader)
{
  return avro_array_read_skip (value, reader, 0);
}

static avro_status_t
avro_array_skip (struct avro_value *value, struct avro_reader *reader)
{
  return avro_array_read_skip (value, reader, 1);
}

static avro_status_t
avro_array_write (struct avro_value *value, struct avro_writer *writer)
{
  struct avro_array_value *self =
    container_of (value, struct avro_array_value, base_value);
  struct avro_io_writer *io;
  avro_status_t status;
  const avro_long_t zero = 0;

  if (!writer)
    {
      return AVRO_FAILURE;
    }
  io = writer->io;
  if (!io)
    {
      return AVRO_FAILURE;
    }

  if (self->values)
    {
      avro_long_t len = self->values->nelts;
      if (len > 0)
	{
	  avro_long_t i;
	  status = avro_write_long (io, &len);
	  if (status != AVRO_OK)
	    {
	      return status;
	    }
	  for (i = 0; i < len; i++)
	    {
	      struct avro_value *value =
		((struct avro_value **) self->values->elts)[i];
	      status = avro_value_write_data (value, writer);
	      if (status != AVRO_OK)
		{
		  return status;
		}
	    }
	}
    }
  /* Zero terminate */
  return avro_write_long (io, (avro_long_t *) & zero);
}

static struct avro_value *
avro_array_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		   apr_pool_t * pool, const JSON_value * json)
{
  struct avro_value *value;
  struct avro_array_value *self;
  apr_pool_t *tmp_pool;

  self = apr_palloc (pool, sizeof (struct avro_array_value));
  if (!self)
    {
      return NULL;
      self->values = NULL;
    }
  self->base_value.type = AVRO_ARRAY;
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = json;

  /* collect and save required items */
  self->item_schema = json_attr_get (json, L"items");
  if (!self->item_schema)
    {
      return NULL;
    }

  /* Validate the item schema */
  if (apr_pool_create (&tmp_pool, pool) != APR_SUCCESS)
    {
      return NULL;
    }
  value = avro_value_from_json (ctx, parent, self->item_schema);
  apr_pool_clear (tmp_pool);
  if (!value)
    {
      /* Invalid item schema */
      return NULL;
    }

  /* Create a pool for the value processing */
  if (apr_pool_create (&self->pool, pool) != APR_SUCCESS)
    {
      return NULL;
    }
  self->values = NULL;
  self->ctx = ctx;
  return &self->base_value;
}

const struct avro_value_module avro_array_module = {
  .name = L"array",
  .type = AVRO_ARRAY,
  .private = 0,
  .create = avro_array_create,
  .formats = {{
	       .read_data = avro_array_read,
	       .skip_data = avro_array_skip,
	       .write_data = avro_array_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_array_read,
	       .skip_data = avro_array_skip,
	       .write_data = avro_array_write}},
  .print_info = avro_array_print
};
