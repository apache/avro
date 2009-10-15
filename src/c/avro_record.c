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

struct avro_field_value
{
  avro_string_t name;
  struct avro_value *value;
  struct avro_value *default_value;

  avro_value base_value;
};

struct avro_record_value
{
  avro_string_t name;
  avro_string_t space;
  apr_array_header_t *fields;

  avro_value base_value;
};

static void
field_print (struct avro_value *value, FILE * fp)
{
  struct avro_field_value *self =
    container_of (value, struct avro_field_value, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "field name=%ls\n", self->name);
  self->value->print_info (self->value, fp);
}

static void
record_print (struct avro_value *value, FILE * fp)
{
  int i;
  struct avro_record_value *self =
    container_of (value, struct avro_record_value, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "record name=%ls\n", self->name);
  for (i = 0; i < self->fields->nelts; i++)
    {
      struct avro_value *field =
	((struct avro_value **) self->fields->elts)[i];
      field->print_info (field, fp);
    }
}

/* FIELDS */
static avro_status_t
field_read (struct avro_value *value, struct avro_channel *channel)
{
  struct avro_field_value *self =
    container_of (value, struct avro_field_value, base_value);
  return self->value->read_data (self->value, channel);
}

static avro_status_t
field_skip (struct avro_value *value, struct avro_channel *channel)
{
  struct avro_field_value *self =
    container_of (value, struct avro_field_value, base_value);
  return self->value->skip_data (self->value, channel);
}

static avro_status_t
field_write (struct avro_value *value, struct avro_channel *channel)
{
  struct avro_field_value *self =
    container_of (value, struct avro_field_value, base_value);
  return self->value->write_data (self->value, channel);
}

/* Private */
static struct avro_value *
avro_field_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		   apr_pool_t * pool, const JSON_value * json)
{
  struct avro_field_value *self =
    apr_palloc (pool, sizeof (struct avro_field_value));
  const JSON_value *name;
  const JSON_value *type;

  if (!self)
    {
      return NULL;
    }
  self->base_value.type = AVRO_FIELD;
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = json;
  self->base_value.read_data = field_read;
  self->base_value.skip_data = field_skip;
  self->base_value.write_data = field_write;
  self->base_value.print_info = field_print;

  /* collect and save required name */
  name = json_attr_get_check_type (json, L"name", JSON_STRING);
  if (!name)
    {
      return NULL;
    }
  self->name = name->json_string;

  /* collect the save the required type */
  type = json_attr_get (json, L"type");
  if (!type)
    {
      return NULL;
    }

  self->value = avro_value_from_json (ctx, &self->base_value, type);
  if (!self->value)
    {
      return NULL;
    }

  /* TODO: collect the optional default value based on type 
     switch(field->value.type){
     }
   */
  return &self->base_value;
}

static avro_status_t
record_read_skip (struct avro_value *value, struct avro_channel *channel,
		  int skip)
{
  int i;
  struct avro_record_value *self =
    container_of (value, struct avro_record_value, base_value);

  for (i = 0; i < self->fields->nelts; i++)
    {
      struct avro_value *field =
	((struct avro_value **) self->fields->elts)[i];
      if (skip)
	{
	  field->skip_data (field, channel);
	}
      else
	{
	  field->read_data (field, channel);
	}
    }
  return AVRO_OK;
}

/* RECORD */
static avro_status_t
record_read (struct avro_value *value, struct avro_channel *channel)
{
  return record_read_skip (value, channel, 0);
}

static avro_status_t
record_skip (struct avro_value *value, struct avro_channel *channel)
{
  return record_read_skip (value, channel, 1);
}

static avro_status_t
record_write (struct avro_value *value, struct avro_channel *channel)
{
/* TODO:
  struct avro_record_value *record =
    container_of (value, struct avro_record_value, base_value);
*/
  return AVRO_OK;
}

struct avro_value *
avro_record_create (struct avro_value_ctx *ctx, struct avro_value *parent,
		    apr_pool_t * pool, const JSON_value * json)
{
  struct avro_record_value *self;
  const JSON_value *name;
  const JSON_value *space;
  const JSON_value *fields;
  apr_status_t status;
  apr_pool_t *subpool;
  int i;

  DEBUG (fprintf (stderr, "CREATING A RECORD\n"));

  self = apr_palloc (pool, sizeof (struct avro_record_value));
  if (!self)
    {
      return NULL;
    }
  self->base_value.type = AVRO_RECORD;
  self->base_value.pool = pool;
  self->base_value.parent = parent;
  self->base_value.schema = json;
  self->base_value.read_data = record_read;
  self->base_value.skip_data = record_skip;
  self->base_value.write_data = record_write;
  self->base_value.print_info = record_print;

  /* collect and save required name */
  name = json_attr_get_check_type (json, L"name", JSON_STRING);
  if (!name)
    {
      return NULL;
    }
  self->name = name->json_string;

  /* register self with named objects */
  apr_hash_set (ctx->named_objects, self->name,
		wcslen (self->name) * sizeof (wchar_t), &self->base_value);

  /* collect and save optional namespace */
  self->space = NULL;
  space = json_attr_get_check_type (json, L"namespace", JSON_STRING);
  if (space)
    {
      self->space = space->json_string;
    }

  /* collect the required fields */
  fields = json_attr_get_check_type (json, L"fields", JSON_ARRAY);
  if (!fields)
    {
      return NULL;
    }

  /* allocate space for the fields */
  self->fields =
    apr_array_make (pool, fields->json_array->nelts,
		    sizeof (struct avro_value *));
  if (!self->fields)
    {
      return NULL;
    }

  for (i = 0; i < fields->json_array->nelts; i++)
    {
      struct avro_value *field_value;
      const JSON_value *field_json =
	((JSON_value **) fields->json_array->elts)[i];
      status = apr_pool_create (&subpool, pool);
      if (status != APR_SUCCESS)
	{
	  return NULL;
	}
      field_value =
	avro_field_create (ctx, &self->base_value, subpool, field_json);
      if (!field_value)
	{
	  return NULL;
	}
      *(struct avro_value **) apr_array_push (self->fields) = field_value;
    }

  return &self->base_value;
}
