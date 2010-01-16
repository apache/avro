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
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "avro.h"
#include "schema.h"
#include "datum.h"
#include "encoding.h"

static void
avro_datum_init (avro_datum_t datum, avro_type_t type)
{
  datum->type = type;
  datum->class_type = AVRO_DATUM;
  datum->refcount = 1;
}

avro_datum_t
avro_string (const char *str)
{
  struct avro_string_datum_t *string_datum =
    malloc (sizeof (struct avro_string_datum_t));
  if (!string_datum)
    {
      return NULL;
    }
  string_datum->s = strdup (str);

  avro_datum_init (&string_datum->obj, AVRO_STRING);
  return &string_datum->obj;
}

avro_datum_t
avro_bytes (const char *buf, int64_t len)
{
  struct avro_bytes_datum_t *bytes_datum =
    malloc (sizeof (struct avro_bytes_datum_t));
  if (!bytes_datum)
    {
      return NULL;
    }
  bytes_datum->buf = malloc (len);
  if (!bytes_datum->buf)
    {
      free (bytes_datum);
      return NULL;
    }
  memcpy (bytes_datum->buf, buf, len);
  bytes_datum->len = len;

  avro_datum_init (&bytes_datum->obj, AVRO_BYTES);
  return &bytes_datum->obj;
}

avro_datum_t
avro_int (int32_t i)
{
  struct avro_int_datum_t *int_datum =
    malloc (sizeof (struct avro_int_datum_t));
  if (!int_datum)
    {
      return NULL;
    }
  int_datum->i = i;

  avro_datum_init (&int_datum->obj, AVRO_INT);
  return &int_datum->obj;
}

avro_datum_t
avro_long (int64_t l)
{
  struct avro_long_datum_t *long_datum =
    malloc (sizeof (struct avro_long_datum_t));
  if (!long_datum)
    {
      return NULL;
    }
  long_datum->l = l;

  avro_datum_init (&long_datum->obj, AVRO_LONG);
  return &long_datum->obj;
}

avro_datum_t
avro_float (float f)
{
  struct avro_float_datum_t *float_datum =
    malloc (sizeof (struct avro_float_datum_t));
  if (!float_datum)
    {
      return NULL;
    }
  float_datum->f = f;

  avro_datum_init (&float_datum->obj, AVRO_FLOAT);
  return &float_datum->obj;
}

avro_datum_t
avro_double (double d)
{
  struct avro_double_datum_t *double_datum =
    malloc (sizeof (struct avro_double_datum_t));
  if (!double_datum)
    {
      return NULL;
    }
  double_datum->d = d;

  avro_datum_init (&double_datum->obj, AVRO_DOUBLE);
  return &double_datum->obj;
}

avro_datum_t
avro_boolean (int8_t i)
{
  struct avro_boolean_datum_t *boolean_datum =
    malloc (sizeof (struct avro_boolean_datum_t));
  if (!boolean_datum)
    {
      return NULL;
    }
  boolean_datum->i = i;

  avro_datum_init (&boolean_datum->obj, AVRO_BOOLEAN);
  return &boolean_datum->obj;
}

avro_datum_t
avro_null (void)
{
  static struct avro_obj_t obj = {
    .type = AVRO_NULL,
    .class_type = AVRO_DATUM,
    .refcount = 1
  };
  return &obj;
}

avro_datum_t
avro_record (const char *name)
{
  /* TODO */
  return NULL;
}

avro_datum_t
avro_record_field_get (const avro_datum_t record, const char *field_name)
{
  /* TODO */
  return NULL;
}

int
avro_record_field_append (const avro_datum_t record,
			  const char *field_name, const avro_datum_t value)
{
  /* TODO */
  return 1;
}

avro_datum_t
avro_enum (const char *name, const char *symbol)
{
  /* TODO */
  return NULL;
}

int
avro_enum_symbol_append (const avro_datum_t enum_value, const char *symbol)
{
  /* TODO */
  return 1;
}

avro_datum_t
avro_fixed_from_schema (struct schema_fixed_t * schema)
{
  struct avro_fixed_datum_t *datum;
  if (!schema)
    {
      return NULL;
    }
  datum = malloc (sizeof (struct avro_fixed_datum_t));
  if (!datum)
    {
      return NULL;
    }
  avro_schema_incref (&schema->obj);
  datum->schema = schema;
  avro_datum_init (&datum->obj, AVRO_FIXED);
  return &datum->obj;
}

avro_datum_t
avro_fixed (const char *name, const size_t len, const char *bytes)
{
  struct avro_fixed_datum_t *fixed;
  avro_datum_t datum;
  avro_schema_t schema = avro_schema_fixed (name, len);
  if (!schema)
    {
      return NULL;
    }
  datum = avro_fixed_from_schema (schema);
  avro_schema_decref (schema);	/* only want one reference */
  if (!datum)
    {
      return NULL;
    }
  fixed = avro_datum_to_fixed (datum);
  /* TODO */
  return datum;
}

avro_datum_t
avro_map (const avro_datum_t values)
{
  /* TODO */
  return NULL;
}

avro_datum_t
avro_array (const avro_datum_t items)
{
  /* TODO */
  return NULL;
}

avro_datum_t
avro_union (void)
{
  /* TODO */
  return NULL;
}

int
avro_union_append (const avro_datum_t union_value, const avro_datum_t value)
{
  /* TODO */
  return 1;
}

avro_datum_t
avro_datum_incref (avro_datum_t value)
{
  /* TODO */
  return NULL;
}

void
avro_datum_decref (avro_datum_t value)
{

}

void
avro_datum_print (avro_datum_t value, FILE * fp)
{

}

static int
schema_match (avro_schema_t writers_schema, avro_schema_t readers_schema)
{
  if (is_avro_union (writers_schema) || is_avro_union (readers_schema))
    {
      return 1;
    }
  /* union */
  else if (is_avro_primitive (writers_schema)
	   && is_avro_primitive (readers_schema)
	   && avro_typeof (writers_schema) == avro_typeof (readers_schema))
    {
      return 1;
    }
  /* record */
  else if (is_avro_record (writers_schema) && is_avro_record (readers_schema)
	   && strcmp (avro_schema_name (writers_schema),
		      avro_schema_name (readers_schema)) == 0)
    {
      return 1;
    }
  /* fixed */
  else if (is_avro_fixed (writers_schema) && is_avro_fixed (readers_schema)
	   && strcmp (avro_schema_name (writers_schema),
		      avro_schema_name (readers_schema)) == 0
	   && (avro_schema_to_fixed (writers_schema))->size ==
	   (avro_schema_to_fixed (readers_schema))->size)
    {
      return 1;
    }
  /* enum */
  else if (is_avro_enum (writers_schema) && is_avro_enum (readers_schema)
	   && strcmp (avro_schema_name (writers_schema),
		      avro_schema_name (readers_schema)) == 0)
    {
      return 1;
    }
  /* map */
  else if (is_avro_map (writers_schema) && is_avro_map (readers_schema)
	   && avro_typeof ((avro_schema_to_map (writers_schema))->values)
	   == avro_typeof ((avro_schema_to_map (readers_schema))->values))
    {
      return 1;
    }
  /* array */
  else if (is_avro_array (writers_schema) && is_avro_array (readers_schema)
	   && avro_typeof ((avro_schema_to_array (writers_schema))->items)
	   == avro_typeof ((avro_schema_to_array (readers_schema))->items))
    {
      return 1;
    }

  /* handle schema promotion */
  else if (is_avro_int (writers_schema)
	   && (is_avro_long (readers_schema) || is_avro_float (readers_schema)
	       || is_avro_double (readers_schema)))
    {
      return 1;
    }
  else if (is_avro_long (writers_schema)
	   && (is_avro_float (readers_schema)
	       && is_avro_double (readers_schema)))
    {
      return 1;
    }
  else if (is_avro_float (writers_schema) && is_avro_double (readers_schema))
    {
      return 1;
    }
  return 0;
}

static int
read_fixed (avro_reader_t reader, const avro_encoding_t * enc,
	    avro_schema_t writers_schema, avro_schema_t readers_schema,
	    avro_datum_t * datum)
{
  return 1;
}

static int
read_enum (avro_reader_t reader, const avro_encoding_t * enc,
	   avro_schema_t writers_schema, avro_schema_t readers_schema,
	   avro_datum_t * datum)
{
  return 1;
}

static int
read_array (avro_reader_t reader, const avro_encoding_t * enc,
	    avro_schema_t writers_schema, avro_schema_t readers_schema,
	    avro_datum_t * datum)
{
  return 1;
}

static int
read_map (avro_reader_t reader, const avro_encoding_t * enc,
	  avro_schema_t writers_schema, avro_schema_t readers_schema,
	  avro_datum_t * datum)
{
  return 1;
}

static int
read_union (avro_reader_t reader, const avro_encoding_t * enc,
	    avro_schema_t writers_schema, avro_schema_t readers_schema,
	    avro_datum_t * datum)
{
  return 1;
}

static int
read_record (avro_reader_t reader, const avro_encoding_t * enc,
	     avro_schema_t writers_schema, avro_schema_t readers_schema,
	     avro_datum_t * datum)
{
  return 1;
}

int
avro_read_data (avro_reader_t reader, avro_schema_t writers_schema,
		avro_schema_t readers_schema, avro_datum_t * datum)
{
  int rval = EINVAL;
  const avro_encoding_t *enc = &avro_binary_encoding;

  if (!reader || !schema_match (writers_schema, readers_schema) || !datum)
    {
      return EINVAL;
    }

  /* schema resolution */
  if (!is_avro_union (writers_schema) && is_avro_union (readers_schema))
    {
      struct union_schema_t *s;
      struct schema_union_t *union_schema =
	avro_schema_to_union (readers_schema);

      for (s = TAILQ_FIRST (&union_schema->schemas);
	   s != NULL; s = TAILQ_NEXT (s, schemas))
	{
	  if (schema_match (writers_schema, s->schema))
	    {
	      return avro_read_data (reader, writers_schema, s->schema,
				     datum);
	    }
	}
      return EINVAL;
    }

  switch (avro_typeof (writers_schema))
    {
    case AVRO_NULL:
      rval = enc->read_null (reader);
      break;

    case AVRO_BOOLEAN:
      {
	int8_t b;
	rval = enc->read_boolean (reader, &b);
	*datum = avro_boolean (b);
      }
      break;

    case AVRO_STRING:
      {
	char *s;
	rval = enc->read_string (reader, &s);
	*datum = avro_string (s);
      }
      break;

    case AVRO_INT:
      {
	int32_t i;
	rval = enc->read_int (reader, &i);
	*datum = avro_int (i);
      }
      break;

    case AVRO_LONG:
      {
	int64_t l;
	rval = enc->read_long (reader, &l);
	*datum = avro_long (l);
      }
      break;

    case AVRO_FLOAT:
      {
	float f;
	rval = enc->read_float (reader, &f);
	*datum = avro_float (f);
      }
      break;

    case AVRO_DOUBLE:
      {
	double d;
	rval = enc->read_double (reader, &d);
	*datum = avro_double (d);
      }
      break;

    case AVRO_BYTES:
      {
	char *bytes;
	int64_t len;
	rval = enc->read_bytes (reader, &bytes, &len);
	*datum = avro_bytes (bytes, len);
      }
      break;

    case AVRO_FIXED:
      rval = read_fixed (reader, enc, writers_schema, readers_schema, datum);
      break;

    case AVRO_ENUM:
      rval = read_enum (reader, enc, writers_schema, readers_schema, datum);
      break;

    case AVRO_ARRAY:
      rval = read_array (reader, enc, writers_schema, readers_schema, datum);
      break;

    case AVRO_MAP:
      rval = read_map (reader, enc, writers_schema, readers_schema, datum);
      break;

    case AVRO_UNION:
      rval = read_union (reader, enc, writers_schema, readers_schema, datum);
      break;

    case AVRO_RECORD:
      rval = read_record (reader, enc, writers_schema, readers_schema, datum);
      break;

    case AVRO_LINK:
      /* TODO */
      break;
    }

  return rval;
}

int
avro_write_data (avro_writer_t writer, avro_schema_t writer_schema,
		 avro_datum_t datum)
{
  /* TODO */
  return 1;
}
