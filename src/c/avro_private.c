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
#include <stdlib.h>
#include "apr_pools.h"
#include "apr_file_io.h"
#include "apr_file_info.h"

extern const struct avro_value_module avro_string_module;
extern const struct avro_value_module avro_bytes_module;
extern const struct avro_value_module avro_int_module;
extern const struct avro_value_module avro_long_module;
extern const struct avro_value_module avro_float_module;
extern const struct avro_value_module avro_double_module;
extern const struct avro_value_module avro_boolean_module;
extern const struct avro_value_module avro_null_module;
extern const struct avro_value_module avro_record_module;
extern const struct avro_value_module avro_field_module;
extern const struct avro_value_module avro_enum_module;
extern const struct avro_value_module avro_fixed_module;
extern const struct avro_value_module avro_map_module;
extern const struct avro_value_module avro_array_module;
extern const struct avro_value_module avro_union_module;
extern const struct avro_value_module avro_decorator_module;
/* WARNING! This registry needs to match avro_type and avro_private_type enums! */
const struct avro_value_module *avro_value_registry[AVRO_NUM_PRIVATE_TYPES] = {
  &avro_string_module,
  &avro_bytes_module,
  &avro_int_module,
  &avro_long_module,
  &avro_float_module,
  &avro_double_module,
  &avro_boolean_module,
  &avro_null_module,
  &avro_record_module,
  &avro_enum_module,
  &avro_fixed_module,
  &avro_map_module,
  &avro_array_module,
  &avro_union_module,
  &avro_field_module,
  &avro_decorator_module
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
	  const struct avro_value_module *info = avro_value_registry[i];
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

struct avro_value *
avro_value_from_json (struct avro_value_ctx *ctx,
		      struct avro_value *parent, const JSON_value * json)
{
  avro_status_t avro_status;
  apr_status_t status;
  avro_type_t avro_type;
  apr_pool_t *subpool;
  const JSON_value *schema = json;

  status = apr_pool_create (&subpool, parent ? parent->pool : NULL);
  if (status != APR_SUCCESS)
    {
      return NULL;
    }

  avro_status = avro_type_from_json (schema, &avro_type);
  if (avro_status != AVRO_OK)
    {
      if (!ctx || json->type != JSON_STRING)
	{
	  return NULL;
	}
      schema =
	apr_hash_get (ctx->named_objects, json->json_string,
		      wcslen (json->json_string) * sizeof (wchar_t));
      if (!schema)
	{
	  return NULL;
	}
      avro_type = AVRO_DECORATOR;
    }

  return avro_value_registry[avro_type]->create (ctx, parent, subpool,
						 schema);
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
avro_value_read_data (struct avro_value * value, struct avro_reader * reader)
{
  if (!value || !reader)
    {
      return AVRO_FAILURE;
    }
  return avro_value_registry[value->type]->formats[reader->format].
    read_data (value, reader);
}

avro_status_t
avro_value_skip_data (struct avro_value * value, struct avro_reader * reader)
{
  if (!value || !reader)
    {
      return AVRO_FAILURE;
    }
  return avro_value_registry[value->type]->formats[reader->format].
    skip_data (value, reader);
}

avro_status_t
avro_value_write_data (struct avro_value * value, struct avro_writer * writer)
{
  if (!value || !writer)
    {
      return AVRO_FAILURE;
    }
  return avro_value_registry[value->type]->formats[writer->format].
    write_data (value, writer);
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

char *
avro_util_file_read_full (apr_pool_t * pool, const char *fname,
			  apr_size_t * len)
{
  apr_status_t status;
  apr_finfo_t finfo;
  apr_file_t *file;
  char *rval;
  apr_size_t bytes_read;

  /* open the file */
  status = apr_file_open (&file, fname, APR_READ, 0, pool);
  if (status != APR_SUCCESS)
    {
      return NULL;
    }

  /* get the file length */
  status = apr_file_info_get (&finfo, APR_FINFO_SIZE, file);
  if (status != APR_SUCCESS)
    {
      return NULL;
    }

  /* alloc space for the data */
  rval = apr_palloc (pool, finfo.size + 1);
  if (!rval)
    {
      return NULL;
    }

  /* read in the data */
  status = apr_file_read_full (file, rval, finfo.size, &bytes_read);
  if (status != APR_SUCCESS)
    {
      return NULL;
    }
  rval[finfo.size] = '\0';

  if (len)
    {
      *len = bytes_read;
    }
  return rval;
}

/* Helper utility for reading an attribute from a JSON object */
const JSON_value *
json_attr_get (const JSON_value * obj, const wchar_t * key)
{
  apr_ssize_t klen;
  apr_hash_t *table;

  if (!obj || !key || obj->type != JSON_OBJECT)
    {
      return NULL;
    }

  table = obj->json_object;
  klen = wcslen (key) * sizeof (wchar_t);
  return apr_hash_get (table, key, klen);
}

/* Helper utility for reading an attribute from JSON w/type checking */
const JSON_value *
json_attr_get_check_type (const JSON_value * obj, const wchar_t * key,
			  JSON_type type)
{
  const JSON_value *value = json_attr_get (obj, key);
  return value && value->type == type ? value : NULL;
}

/* Helper to print indent before a value */
void
avro_value_indent (struct avro_value *value, FILE * fp)
{
  struct avro_value *cur;
  for (cur = value->parent; cur; cur = cur->parent)
    {
      fprintf (fp, "  ");
    }
}
