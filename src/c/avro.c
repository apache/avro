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
#include <locale.h>
#include "apr_pools.h"
#include "apr_file_io.h"
#include "apr_file_info.h"

static struct avro_type_entry
{
  const avro_string_t name;
  const avro_type_t type;
} avro_type_lookup_table[] =
{
  {
  L"string", AVRO_STRING},
  {
  L"bytes", AVRO_BYTES},
  {
  L"int", AVRO_INT},
  {
  L"long", AVRO_LONG},
  {
  L"float", AVRO_FLOAT},
  {
  L"double", AVRO_DOUBLE},
  {
  L"boolean", AVRO_BOOLEAN},
  {
  L"null", AVRO_NULL},
  {
  L"record", AVRO_RECORD},
  {
  L"enum", AVRO_ENUM},
  {
  L"fixed", AVRO_FIXED},
  {
  L"map", AVRO_MAP},
  {
  L"array", AVRO_ARRAY},
  {
  L"union", AVRO_UNION}
};

#define NUM_AVRO_NODE_TYPES (sizeof(avro_type_lookup_table)/sizeof(avro_type_lookup_table[0]))

/* TODO: gperf this */
const avro_type_t *
avro_type_lookup (const avro_string_t name)
{
  int i;
  if (name)
    {
      for (i = 0; i < NUM_AVRO_NODE_TYPES; i++)
	{
	  const struct avro_type_entry *entry = avro_type_lookup_table + i;
	  if (wcscmp (entry->name, name) == 0)
	    {
	      return &entry->type;
	    }
	}
    }
  return NULL;
}

const avro_type_t *
avro_type_from_json (const JSON_value * json)
{
  const avro_type_t *avro_type = NULL;
  const static avro_type_t union_type = AVRO_UNION;
  const JSON_value *type_attr;

  if (!json)
    {
      return NULL;
    }

  if (json->type == JSON_STRING)
    {
      avro_type = avro_type_lookup (json->json_string);
    }
  else if (json->type == JSON_ARRAY)
    {
      avro_type = &union_type;
    }
  else if (json->type == JSON_OBJECT)
    {
      type_attr = json_attr_get_check_type (json, L"type", JSON_STRING);
      if (!type_attr)
	{
	  return NULL;
	}
      avro_type = avro_type_lookup (type_attr->json_string);
    }

  return avro_type;
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

avro_status_t
avro_initialize (void)
{
  apr_initialize ();
  atexit (apr_terminate);

  /* Set the locale to UTF-8 */
  if (!setlocale (LC_CTYPE, "en_US.UTF-8"))
    {
      return AVRO_FAILURE;
    }

  return AVRO_OK;
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
