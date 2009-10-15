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

struct avro_decorator_value
{
  struct avro_value *decoratee;
  struct avro_value base_value;
};

static void
avro_decorator_print (struct avro_value *value, FILE * fp)
{
  struct avro_decorator_value *self =
    container_of (value, struct avro_decorator_value, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "avro decorator\n");
}

static avro_status_t
avro_decorator_read (struct avro_value *value, struct avro_channel *channel)
{
  struct avro_decorator_value *self =
    container_of (value, struct avro_decorator_value, base_value);
  return self->decoratee->read_data (self->decoratee, channel);
}

static avro_status_t
avro_decorator_skip (struct avro_value *value, struct avro_channel *channel)
{
  struct avro_decorator_value *self =
    container_of (value, struct avro_decorator_value, base_value);
  return self->decoratee->skip_data (self->decoratee, channel);
}

static avro_status_t
avro_decorator_write (struct avro_value *value, struct avro_channel *channel)
{
  struct avro_decorator_value *self =
    container_of (value, struct avro_decorator_value, base_value);
  return self->decoratee->write_data (self->decoratee, channel);
}

/* Used for recursive schemas */
struct avro_value *
avro_decorator_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		       apr_pool_t * pool, struct avro_value *decoratee)
{
  struct avro_decorator_value *self =
    apr_palloc (pool, sizeof (struct avro_decorator_value));
  DEBUG (fprintf (stderr, "Creating a decorator\n"));
  if (!self)
    {
      return NULL;
    }
  self->base_value.type = 1000;	/* TODO: ... */
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = decoratee->schema;
  self->base_value.read_data = avro_decorator_read;
  self->base_value.skip_data = avro_decorator_skip;
  self->base_value.write_data = avro_decorator_write;
  self->base_value.print_info = avro_decorator_print;

  /* object we're decorating */
  self->decoratee = decoratee;
  return &self->base_value;
}

struct avro_value *
avro_value_from_json (struct avro_value_ctx *ctx,
		      struct avro_value *parent, const JSON_value * json)
{
  apr_status_t status;
  struct avro_value *value;
  const avro_type_t *avro_type;
  apr_pool_t *subpool;

  status = apr_pool_create (&subpool, parent ? parent->pool : NULL);
  if (status != APR_SUCCESS)
    {
      return NULL;
    }

  avro_type = avro_type_from_json (json);
  if (!avro_type)
    {
      if (json->type == JSON_STRING)
	{
	  avro_value *named_object =
	    apr_hash_get (ctx->named_objects, json->json_string,
			  wcslen (json->json_string) * sizeof (wchar_t));
	  if (named_object)
	    {
	      return avro_decorator_create (ctx, parent, subpool,
					    named_object);
	    }
	}
      return NULL;
    }

  value = NULL;
  switch (*avro_type)
    {
    case AVRO_STRING:
      value = avro_string_create (ctx, parent, subpool, json);
      break;
    case AVRO_BYTES:
      value = avro_bytes_create (ctx, parent, subpool, json);
      break;
    case AVRO_INT:
      value = avro_int_create (ctx, parent, subpool, json);
      break;
    case AVRO_LONG:
      value = avro_long_create (ctx, parent, subpool, json);
      break;
    case AVRO_FLOAT:
      value = avro_float_create (ctx, parent, subpool, json);
      break;
    case AVRO_DOUBLE:
      value = avro_double_create (ctx, parent, subpool, json);
      break;
    case AVRO_BOOLEAN:
      value = avro_boolean_create (ctx, parent, subpool, json);
      break;
    case AVRO_NULL:
      value = avro_null_create (ctx, parent, subpool, json);
      break;
    case AVRO_RECORD:
      value = avro_record_create (ctx, parent, subpool, json);
      break;
    case AVRO_ENUM:
      value = avro_enum_create (ctx, parent, subpool, json);
      break;
    case AVRO_FIXED:
      value = avro_fixed_create (ctx, parent, subpool, json);
      break;
    case AVRO_MAP:
      value = avro_map_create (ctx, parent, subpool, json);
      break;
    case AVRO_ARRAY:
      value = avro_array_create (ctx, parent, subpool, json);
      break;
    case AVRO_UNION:
      value = avro_union_create (ctx, parent, subpool, json);
      break;
    case AVRO_FIELD:
      /* fields are created by record_create */
      break;
    }
  return value;
}

struct avro_value *
avro_value_create (apr_pool_t * pool, char *jsontext, apr_size_t textlen)
{
  apr_status_t status;
  struct avro_value_ctx *ctx;

  const JSON_value *json = JSON_parse (pool, jsontext, textlen);
  if (!json)
    {
      return NULL;
    }

  ctx =
    (struct avro_value_ctx *) apr_palloc (pool,
					  sizeof (struct avro_value_ctx));
  if (!ctx)
    {
      return NULL;
    }

  ctx->named_objects = apr_hash_make (pool);
  if (!ctx->named_objects)
    {
      return NULL;
    }

  return avro_value_from_json (ctx, NULL, json);
}
