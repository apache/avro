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

extern const struct avro_value_info avro_string_info;
extern const struct avro_value_info avro_bytes_info;
extern const struct avro_value_info avro_int_info;
extern const struct avro_value_info avro_long_info;
extern const struct avro_value_info avro_float_info;
extern const struct avro_value_info avro_double_info;
extern const struct avro_value_info avro_boolean_info;
extern const struct avro_value_info avro_null_info;
extern const struct avro_value_info avro_record_info;
extern const struct avro_value_info avro_field_info;
extern const struct avro_value_info avro_enum_info;
extern const struct avro_value_info avro_fixed_info;
extern const struct avro_value_info avro_map_info;
extern const struct avro_value_info avro_array_info;
extern const struct avro_value_info avro_union_info;
extern const struct avro_value_info avro_decorator_info;
/* WARNING: This registry must match the avro_value_t enum! */
const struct avro_value_info *avro_value_registry[AVRO_NUM_TYPES] = {
  &avro_string_info,
  &avro_bytes_info,
  &avro_int_info,
  &avro_long_info,
  &avro_float_info,
  &avro_double_info,
  &avro_boolean_info,
  &avro_null_info,
  &avro_record_info,
  &avro_enum_info,
  &avro_fixed_info,
  &avro_map_info,
  &avro_array_info,
  &avro_union_info,
  &avro_field_info,
  &avro_decorator_info
};

/* TODO: gperf this? */
static avro_status_t
avro_type_lookup (const avro_string_t name, avro_type_t * type)
{
  int i;
  if (name)
    {
      for (i = 0; i < AVRO_NUM_TYPES; i++)
	{
	  const struct avro_value_info *info = avro_value_registry[i];
	  if (!info->private && wcscmp (info->name, name) == 0)
	    {
	      *type = info->type;
	      return AVRO_OK;
	    }
	}
    }
  return AVRO_FAILURE;
}

static avro_status_t
avro_type_from_json (const JSON_value * json, avro_type_t * type)
{
  if (!json)
    {
      return AVRO_FAILURE;
    }

  if (json->type == JSON_STRING)
    {
      return avro_type_lookup (json->json_string, type);
    }
  else if (json->type == JSON_ARRAY)
    {
      *type = AVRO_UNION;
      return AVRO_OK;
    }
  else if (json->type == JSON_OBJECT)
    {
      const JSON_value *type_attr =
	json_attr_get_check_type (json, L"type", JSON_STRING);
      if (type_attr)
	{
	  return avro_type_lookup (type_attr->json_string, type);
	}
    }

  return AVRO_FAILURE;
}

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
  return avro_value_read_data (self->decoratee, channel);
}

static avro_status_t
avro_decorator_skip (struct avro_value *value, struct avro_channel *channel)
{
  struct avro_decorator_value *self =
    container_of (value, struct avro_decorator_value, base_value);
  return avro_value_skip_data (self->decoratee, channel);
}

static avro_status_t
avro_decorator_write (struct avro_value *value, struct avro_channel *channel)
{
  struct avro_decorator_value *self =
    container_of (value, struct avro_decorator_value, base_value);
  return avro_value_write_data (self->decoratee, channel);
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
  self->base_value.type = AVRO_DECORATOR;
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = decoratee->schema;
  /* object we're decorating */
  self->decoratee = decoratee;
  return &self->base_value;
}

static struct avro_value *
avro_decorator_create_noop (struct avro_value_ctx *ctx,
			    struct avro_value *parent, apr_pool_t * pool,
			    const JSON_value * json)
{
  return NULL;
}

const struct avro_value_info avro_decorator_info = {
  .name = L"decorator",
  .type = AVRO_DECORATOR,
  .private = 1,
  .create = avro_decorator_create_noop,
  .formats = {{
	       .read_data = avro_decorator_read,
	       .skip_data = avro_decorator_skip,
	       .write_data = avro_decorator_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_decorator_read,
	       .skip_data = avro_decorator_skip,
	       .write_data = avro_decorator_write}},
  .print_info = avro_decorator_print
};


struct avro_value *
avro_value_from_json (struct avro_value_ctx *ctx,
		      struct avro_value *parent, const JSON_value * json)
{
  avro_status_t avro_status;
  apr_status_t status;
  avro_type_t avro_type;
  apr_pool_t *subpool;

  status = apr_pool_create (&subpool, parent ? parent->pool : NULL);
  if (status != APR_SUCCESS)
    {
      return NULL;
    }

  avro_status = avro_type_from_json (json, &avro_type);
  if (avro_status != AVRO_OK)
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

  return avro_value_registry[avro_type]->private ? NULL:
    avro_value_registry[avro_type]->create (ctx, parent, subpool, json);
}

struct avro_value *
avro_value_create (apr_pool_t * pool, char *jsontext, apr_size_t textlen)
{
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

avro_status_t
avro_value_read_data (struct avro_value * value,
		      struct avro_channel * channel)
{
  if (!value || !channel)
    {
      return AVRO_FAILURE;
    }
  return avro_value_registry[value->type]->formats[channel->format].
    read_data (value, channel);
}

avro_status_t
avro_value_skip_data (struct avro_value * value,
		      struct avro_channel * channel)
{
  if (!value || !channel)
    {
      return AVRO_FAILURE;
    }
  return avro_value_registry[value->type]->formats[channel->format].
    skip_data (value, channel);
}

avro_status_t
avro_value_write_data (struct avro_value * value,
		       struct avro_channel * channel)
{
  if (!value || !channel)
    {
      return AVRO_FAILURE;
    }
  return avro_value_registry[value->type]->formats[channel->format].
    write_data (value, channel);
}

void
avro_value_print_info (struct avro_value *value, FILE * fp)
{
  if (!value || !fp)
    {
      return;
    }
  avro_value_registry[value->type]->print_info (value, fp);
}
