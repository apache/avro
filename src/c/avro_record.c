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
avro_field_print (struct avro_value *value, FILE * fp)
{
  struct avro_field_value *self =
    container_of (value, struct avro_field_value, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "field(%p) name=%ls\n", self, self->name);
  avro_value_print_info (self->value, fp);
}

static void
avro_record_print (struct avro_value *value, FILE * fp)
{
  int i;
  struct avro_record_value *self =
    container_of (value, struct avro_record_value, base_value);
  avro_value_indent (value, fp);
  fprintf (fp, "record(%p) name=%ls\n", self, self->name);
  for (i = 0; i < self->fields->nelts; i++)
    {
      struct avro_value *field =
	((struct avro_value **) self->fields->elts)[i];
      avro_value_print_info (field, fp);
    }
}

/* FIELDS */
static avro_status_t
avro_field_read (struct avro_value *value, struct avro_reader *reader)
{
  struct avro_field_value *self =
    container_of (value, struct avro_field_value, base_value);
  return avro_value_read_data (self->value, reader);
}

static avro_status_t
avro_field_skip (struct avro_value *value, struct avro_reader *reader)
{
  struct avro_field_value *self =
    container_of (value, struct avro_field_value, base_value);
  return avro_value_skip_data (self->value, reader);
}

static avro_status_t
avro_field_write (struct avro_value *value, struct avro_writer *writer)
{
  struct avro_field_value *self =
    container_of (value, struct avro_field_value, base_value);
  return avro_value_write_data (self->value, writer);
}

/* The field constructor is private so we register a noop */
static struct avro_value *
avro_field_create_noop (struct avro_value_ctx *ctx, struct avro_value *parent,
			apr_pool_t * pool, const JSON_value * json)
{
  return NULL;
}

/* Should only be called by record functions */
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
avro_record_read_skip (struct avro_value *value, struct avro_reader *reader,
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
	  avro_value_skip_data (field, reader);
	}
      else
	{
	  avro_value_read_data (field, reader);
	}
    }
  return AVRO_OK;
}

/* RECORD */
static avro_status_t
avro_record_read (struct avro_value *value, struct avro_reader *reader)
{
  return avro_record_read_skip (value, reader, 0);
}

static avro_status_t
avro_record_skip (struct avro_value *value, struct avro_reader *reader)
{
  return avro_record_read_skip (value, reader, 1);
}

static avro_status_t
avro_record_write (struct avro_value *value, struct avro_writer *writer)
{
  int i;
  struct avro_record_value *self =
    container_of (value, struct avro_record_value, base_value);

  for (i = 0; i < self->fields->nelts; i++)
    {
      struct avro_value *field =
	((struct avro_value **) self->fields->elts)[i];
      avro_value_write_data (field, writer);
    }
  return AVRO_OK;
}

static struct avro_value *
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

  /* collect and save required name */
  name = json_attr_get_check_type (json, L"name", JSON_STRING);
  if (!name)
    {
      return NULL;
    }
  self->name = name->json_string;

  /* register self with named objects */
  apr_hash_set (ctx->named_objects, self->name,
		wcslen (self->name) * sizeof (wchar_t), json);

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

const struct avro_value_info avro_field_info = {
  .name = L"field",
  .type = AVRO_FIELD,
  .private = 1,
  .create = avro_field_create_noop,
  .formats = {{
	       .read_data = avro_field_read,
	       .skip_data = avro_field_skip,
	       .write_data = avro_field_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_field_read,
	       .skip_data = avro_field_skip,
	       .write_data = avro_field_write}},
  .print_info = avro_field_print
};

const struct avro_value_info avro_record_info = {
  .name = L"record",
  .type = AVRO_RECORD,
  .private = 0,
  .create = avro_record_create,
  .formats = {{
	       .read_data = avro_record_read,
	       .skip_data = avro_record_skip,
	       .write_data = avro_record_write},
	      {
	       /* TODO: import/export */
	       .read_data = avro_record_read,
	       .skip_data = avro_record_skip,
	       .write_data = avro_record_write}},
  .print_info = avro_record_print
};
