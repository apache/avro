/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License. 
 */

#include "avro_errors.h"
#include "avro_private.h"
#include "allocation.h"
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "encoding.h"
#include "schema.h"
#include "datum.h"

int
avro_schema_match(avro_schema_t writers_schema, avro_schema_t readers_schema)
{
	if (!is_avro_schema(writers_schema) || !is_avro_schema(readers_schema)) {
		return 0;
	}

	switch (avro_typeof(writers_schema)) {
	case AVRO_UNION:
		return 1;

	case AVRO_INT32:
		return is_avro_int32(readers_schema)
		    || is_avro_int64(readers_schema)
		    || is_avro_float(readers_schema)
		    || is_avro_double(readers_schema);

	case AVRO_INT64:
		return is_avro_int64(readers_schema)
		    || is_avro_float(readers_schema)
		    || is_avro_double(readers_schema);

	case AVRO_FLOAT:
		return is_avro_float(readers_schema)
		    || is_avro_double(readers_schema);

	case AVRO_STRING:
	case AVRO_BYTES:
	case AVRO_DOUBLE:
	case AVRO_BOOLEAN:
	case AVRO_NULL:
		return avro_typeof(writers_schema) ==
		    avro_typeof(readers_schema);

	case AVRO_RECORD:
		return is_avro_record(readers_schema)
		    && strcmp(avro_schema_name(writers_schema),
			      avro_schema_name(readers_schema)) == 0;

	case AVRO_FIXED:
		return is_avro_fixed(readers_schema)
		    && strcmp(avro_schema_name(writers_schema),
			      avro_schema_name(readers_schema)) == 0
		    && (avro_schema_to_fixed(writers_schema))->size ==
		    (avro_schema_to_fixed(readers_schema))->size;

	case AVRO_ENUM:
		return is_avro_enum(readers_schema)
		    && strcmp(avro_schema_to_enum(writers_schema)->name,
			      avro_schema_to_enum(readers_schema)->name) == 0;

	case AVRO_MAP:
		return is_avro_map(readers_schema)
		    && avro_typeof(avro_schema_to_map(writers_schema)->values)
		    == avro_typeof(avro_schema_to_map(readers_schema)->values);

	case AVRO_ARRAY:
		return is_avro_array(readers_schema)
		    && avro_typeof(avro_schema_to_array(writers_schema)->items)
		    == avro_typeof(avro_schema_to_array(readers_schema)->items);

	case AVRO_LINK:
		/*
		 * TODO 
		 */
		break;
	}

	return 0;
}

static int
read_enum(avro_reader_t reader, const avro_encoding_t * enc,
	  struct avro_enum_schema_t *writers_schema,
	  struct avro_enum_schema_t *readers_schema, avro_datum_t * datum)
{
	int rval;
	int64_t index;

	AVRO_UNUSED(writers_schema);

	check_prefix(rval, enc->read_long(reader, &index),
		     "Cannot read enum value: ");
	*datum = avro_enum(&readers_schema->obj, index);
	return 0;
}

static int
read_array(avro_reader_t reader, const avro_encoding_t * enc,
	   struct avro_array_schema_t *writers_schema,
	   struct avro_array_schema_t *readers_schema, avro_datum_t * datum)
{
	int rval;
	int64_t i;
	int64_t block_count;
	int64_t block_size;
	avro_datum_t array_datum;

	check_prefix(rval, enc->read_long(reader, &block_count),
		     "Cannot read array block count: ");

	array_datum = avro_array(&readers_schema->obj);
	while (block_count != 0) {
		if (block_count < 0) {
			block_count = block_count * -1;
			check_prefix(rval, enc->read_long(reader, &block_size),
				     "Cannot read array block size: ");
		}

		for (i = 0; i < block_count; i++) {
			avro_datum_t datum;

			rval =
			    avro_read_data(reader, writers_schema->items,
					   readers_schema->items, &datum);
			if (rval) {
				avro_datum_decref(array_datum);
				return rval;
			}
			rval = avro_array_append_datum(array_datum, datum);
			if (rval) {
				avro_set_error("Cannot append element to array");
				avro_datum_decref(array_datum);
				return rval;
			}
			avro_datum_decref(datum);
		}

		rval = enc->read_long(reader, &block_count);
		if (rval) {
			avro_prefix_error("Cannot read array block count: ");
			avro_datum_decref(array_datum);
			return rval;
		}
	}
	*datum = array_datum;
	return 0;
}

static int
read_map(avro_reader_t reader, const avro_encoding_t * enc,
	 struct avro_map_schema_t *writers_schema,
	 struct avro_map_schema_t *readers_schema, avro_datum_t * datum)
{
	int rval;
	int64_t i, block_count;
	avro_datum_t map = avro_map(&readers_schema->obj);

	rval = enc->read_long(reader, &block_count);
	if (rval) {
		avro_prefix_error("Cannot read map block count: ");
		avro_datum_decref(map);
		return rval;
	}
	while (block_count != 0) {
		int64_t block_size;
		if (block_count < 0) {
			block_count = block_count * -1;
			rval = enc->read_long(reader, &block_size);
			if (rval) {
				avro_prefix_error("Cannot read map block size: ");
				avro_datum_decref(map);
				return rval;
			}
		}
		for (i = 0; i < block_count; i++) {
			char *key;
			int64_t key_size;
			avro_datum_t value;
			rval = enc->read_string(reader, &key, &key_size);
			if (rval) {
				avro_prefix_error("Cannot read map key: ");
				avro_datum_decref(map);
				return rval;
			}
			rval =
			    avro_read_data(reader,
					   avro_schema_to_map(writers_schema)->
					   values,
					   avro_schema_to_map(readers_schema)->
					   values, &value);
			if (rval) {
				avro_free(key, key_size);
				return rval;
			}
			rval = avro_map_set(map, key, value);
			if (rval) {
				avro_set_error("Cannot append element to map");
				avro_free(key, key_size);
				avro_datum_decref(map);
				return rval;
			}
			avro_datum_decref(value);
			avro_free(key, key_size);
		}
		rval = enc->read_long(reader, &block_count);
		if (rval) {
			avro_prefix_error("Cannot read map block count: ");
			avro_datum_decref(map);
			return rval;
		}
	}
	*datum = map;
	return 0;
}

static int
read_union(avro_reader_t reader, const avro_encoding_t * enc,
	   struct avro_union_schema_t *writers_schema,
	   struct avro_union_schema_t *readers_schema, avro_datum_t * datum)
{
	int rval;
	int64_t discriminant;
	avro_datum_t value;
	union {
		st_data_t data;
		avro_schema_t schema;
	} val;

	AVRO_UNUSED(readers_schema);

	check_prefix(rval, enc->read_long(reader, &discriminant),
		     "Cannot read union discriminant: ");
	if (!st_lookup(writers_schema->branches, discriminant, &val.data)) {
		avro_set_error("Union doesn't have branch %ld", (long) discriminant);
		return EILSEQ;
	}
	check(rval, avro_read_data(reader, val.schema, NULL, &value));
	*datum = avro_union(&readers_schema->obj, discriminant, value);
	avro_datum_decref(value);
	return 0;
}

/* TODO: handle default values in fields */
static int
read_record(avro_reader_t reader, const avro_encoding_t * enc,
	    struct avro_record_schema_t *writers_schema,
	    struct avro_record_schema_t *readers_schema, avro_datum_t * datum)
{
	int rval;
	long i;
	avro_datum_t record;
	avro_datum_t field_datum;

	AVRO_UNUSED(enc);

	record = *datum = avro_record(&readers_schema->obj);
	for (i = 0; i < writers_schema->fields->num_entries; i++) {
		union {
			st_data_t data;
			struct avro_record_field_t *field;
		} rfield, wfield;
		st_lookup(writers_schema->fields, i, &wfield.data);
		if (st_lookup
		    (readers_schema->fields_byname,
		     (st_data_t) wfield.field->name, &rfield.data)) {
			rval =
			    avro_read_data(reader, wfield.field->type,
					   rfield.field->type, &field_datum);
			if (rval) {
				return rval;
			}
			rval =
			    avro_record_set(record, wfield.field->name,
					    field_datum);
			if (rval) {
				avro_set_error("Cannot append field to record");
				return rval;
			}
			avro_datum_decref(field_datum);
		} else {
			rval = avro_skip_data(reader, wfield.field->type);
			if (rval) {
				return rval;
			}
		}
	}
	return 0;
}

static void
free_bytes(void *ptr, size_t sz)
{
	// The binary encoder class allocates bytes values with an extra
	// byte, so that they're NUL terminated.
	avro_free(ptr, sz+1);
}

int
avro_read_data(avro_reader_t reader, avro_schema_t writers_schema,
	       avro_schema_t readers_schema, avro_datum_t * datum)
{
	int rval = EINVAL;
	const avro_encoding_t *enc = &avro_binary_encoding;

	check_param(EINVAL, reader, "reader");
	check_param(EINVAL, is_avro_schema(writers_schema), "writer schema");
	check_param(EINVAL, datum, "datum pointer");

	if (readers_schema == NULL) {
		readers_schema = writers_schema;
	} else if (!avro_schema_match(writers_schema, readers_schema)) {
		avro_set_error("Reader and writer schemas aren't compatible");
		return EINVAL;
	}

	switch (avro_typeof(writers_schema)) {
	case AVRO_NULL:
		rval = enc->read_null(reader);
		if (rval) {
			avro_prefix_error("Cannot read null value: ");
		} else {
			*datum = avro_null();
		}
		break;

	case AVRO_BOOLEAN:
		{
			int8_t b;
			rval = enc->read_boolean(reader, &b);
			if (rval) {
				avro_prefix_error("Cannot read boolean value: ");
			} else {
				*datum = avro_boolean(b);
			}
		}
		break;

	case AVRO_STRING:
		{
			int64_t len;
			char *s;
			rval = enc->read_string(reader, &s, &len);
			if (rval) {
				avro_prefix_error("Cannot read string value: ");
			} else {
				*datum = avro_givestring(s, avro_alloc_free);
			}
		}
		break;

	case AVRO_INT32:
		{
			int32_t i;
			rval = enc->read_int(reader, &i);
			if (rval) {
				avro_prefix_error("Cannot read int value: ");
			} else {
				*datum = avro_int32(i);
			}
		}
		break;

	case AVRO_INT64:
		{
			int64_t l;
			rval = enc->read_long(reader, &l);
			if (rval) {
				avro_prefix_error("Cannot read long value: ");
			} else {
				*datum = avro_int64(l);
			}
		}
		break;

	case AVRO_FLOAT:
		{
			float f;
			rval = enc->read_float(reader, &f);
			if (rval) {
				avro_prefix_error("Cannot read float value: ");
			} else {
				*datum = avro_float(f);
			}
		}
		break;

	case AVRO_DOUBLE:
		{
			double d;
			rval = enc->read_double(reader, &d);
			if (rval) {
				avro_prefix_error("Cannot read double value: ");
			} else {
				*datum = avro_double(d);
			}
		}
		break;

	case AVRO_BYTES:
		{
			char *bytes;
			int64_t len;
			rval = enc->read_bytes(reader, &bytes, &len);
			if (rval) {
				avro_prefix_error("Cannot read bytes value: ");
			} else {
				*datum = avro_givebytes(bytes, len, free_bytes);
			}
		}
		break;

	case AVRO_FIXED:{
			char *bytes;
			int64_t size =
			    avro_schema_to_fixed(writers_schema)->size;

			bytes = avro_malloc(size);
			if (!bytes) {
				avro_prefix_error("Cannot allocate new fixed value");
				return ENOMEM;
			}
			rval = avro_read(reader, bytes, size);
			if (rval) {
				avro_prefix_error("Cannot read fixed value: ");
			} else {
				*datum = avro_givefixed(readers_schema, bytes, size,
							avro_alloc_free);
			}
		}
		break;

	case AVRO_ENUM:
		rval =
		    read_enum(reader, enc, avro_schema_to_enum(writers_schema),
			      avro_schema_to_enum(readers_schema), datum);
		break;

	case AVRO_ARRAY:
		rval =
		    read_array(reader, enc,
			       avro_schema_to_array(writers_schema),
			       avro_schema_to_array(readers_schema), datum);
		break;

	case AVRO_MAP:
		rval =
		    read_map(reader, enc, avro_schema_to_map(writers_schema),
			     avro_schema_to_map(readers_schema), datum);
		break;

	case AVRO_UNION:
		rval =
		    read_union(reader, enc,
			       avro_schema_to_union(writers_schema),
			       avro_schema_to_union(readers_schema), datum);
		break;

	case AVRO_RECORD:
		rval =
		    read_record(reader, enc,
				avro_schema_to_record(writers_schema),
				avro_schema_to_record(readers_schema), datum);
		break;

	case AVRO_LINK:
		rval =
		    avro_read_data(reader,
				   (avro_schema_to_link(writers_schema))->to,
				   readers_schema, datum);
		break;
	}

	return rval;
}
