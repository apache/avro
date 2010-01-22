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

	case AVRO_INT:
		return is_avro_int(readers_schema)
		    || is_avro_long(readers_schema)
		    || is_avro_float(readers_schema)
		    || is_avro_double(readers_schema);

	case AVRO_LONG:
		return is_avro_long(readers_schema)
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
read_fixed(avro_reader_t reader, const avro_encoding_t * enc,
	   avro_schema_t writers_schema, avro_schema_t readers_schema,
	   avro_datum_t * datum)
{
	return 1;
}

static int
read_enum(avro_reader_t reader, const avro_encoding_t * enc,
	  avro_schema_t writers_schema, avro_schema_t readers_schema,
	  avro_datum_t * datum)
{
	return 1;
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

	rval = enc->read_long(reader, &block_count);
	if (rval) {
		return rval;
	}

	array_datum = avro_array();
	while (block_count != 0) {
		if (block_count < 0) {
			block_count = block_count * -1;
			rval = enc->read_long(reader, &block_size);
			if (rval) {
				return rval;
			}
		}

		for (i = 0; i < block_count; i++) {
			avro_datum_t datum;

			rval =
			    avro_read_data(reader, writers_schema->items,
					   readers_schema->items, &datum);
			if (rval) {
				return rval;
			}
			rval = avro_array_append_datum(array_datum, datum);
			if (rval) {
				avro_datum_decref(array_datum);
				return rval;
			}
		}

		rval = enc->read_long(reader, &block_count);
		if (rval) {
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
	avro_datum_t map = avro_map();

	rval = enc->read_long(reader, &block_count);
	if (rval) {
		return rval;
	}
	while (block_count != 0) {
		int64_t block_size;
		if (block_count < 0) {
			block_count = block_count * -1;
			rval = enc->read_long(reader, &block_size);
			if (rval) {
				return rval;
			}
		}
		for (i = 0; i < block_count; i++) {
			char *key;
			avro_datum_t value;
			rval = enc->read_string(reader, &key);
			if (rval) {
				return rval;
			}
			rval =
			    avro_read_data(reader,
					   avro_schema_to_map(writers_schema)->
					   values,
					   avro_schema_to_map(readers_schema)->
					   values, &value);
			if (rval) {
				free(key);
				return rval;
			}
			rval = avro_map_set(map, key, value);
			if (rval) {
				free(key);
				return rval;
			}
			free(key);
		}
		rval = enc->read_long(reader, &block_count);
		if (rval) {
			return rval;
		}
	}
	*datum = map;
	return 0;
}

static int
read_union(avro_reader_t reader, const avro_encoding_t * enc,
	   avro_schema_t writers_schema, avro_schema_t readers_schema,
	   avro_datum_t * datum)
{
	return 1;
}

/* TODO: handle default values in fields */
static int
read_record(avro_reader_t reader, const avro_encoding_t * enc,
	    struct avro_record_schema_t *writers_schema,
	    struct avro_record_schema_t *readers_schema, avro_datum_t * datum)
{
	int rval;
	struct avro_record_field_t *reader_field;
	struct avro_record_field_t *field;
	avro_datum_t record;
	avro_datum_t field_datum;

	record = *datum = avro_record(writers_schema->name);
	for (field = STAILQ_FIRST(&writers_schema->fields);
	     field != NULL; field = STAILQ_NEXT(field, fields)) {
		for (reader_field = STAILQ_FIRST(&readers_schema->fields);
		     reader_field != NULL;
		     reader_field = STAILQ_NEXT(reader_field, fields)) {
			if (strcmp(field->name, reader_field->name) == 0) {
				break;
			}
		}
		if (reader_field) {
			rval =
			    avro_read_data(reader, field->type,
					   reader_field->type, &field_datum);
			if (rval) {
				return rval;
			}
			rval =
			    avro_record_field_set(record, field->name,
						  field_datum);
			if (rval) {
				return rval;
			}
		} else {
			/* TODO: skip_record */
			return -1;
		}
	}
	return 0;
}

int
avro_read_data(avro_reader_t reader, avro_schema_t writers_schema,
	       avro_schema_t readers_schema, avro_datum_t * datum)
{
	int rval = EINVAL;
	const avro_encoding_t *enc = &avro_binary_encoding;

	if (!reader || !is_avro_schema(writers_schema) || !datum) {
		return EINVAL;
	}

	if (readers_schema == NULL) {
		readers_schema = writers_schema;
	} else if (!avro_schema_match(writers_schema, readers_schema)) {
		return EINVAL;
	}

	/*
	 * schema resolution 
	 */
	if (!is_avro_union(writers_schema) && is_avro_union(readers_schema)) {
		struct avro_union_branch_t *branch;
		struct avro_union_schema_t *union_schema =
		    avro_schema_to_union(readers_schema);

		for (branch = STAILQ_FIRST(&union_schema->branches);
		     branch != NULL; branch = STAILQ_NEXT(branch, branches)) {
			if (avro_schema_match(writers_schema, branch->schema)) {
				return avro_read_data(reader, writers_schema,
						      branch->schema, datum);
			}
		}
		return EINVAL;
	}

	switch (avro_typeof(writers_schema)) {
	case AVRO_NULL:
		rval = enc->read_null(reader);
		*datum = avro_null();
		break;

	case AVRO_BOOLEAN:
		{
			int8_t b;
			rval = enc->read_boolean(reader, &b);
			*datum = avro_boolean(b);
		}
		break;

	case AVRO_STRING:
		{
			char *s;
			rval = enc->read_string(reader, &s);
			*datum = avro_givestring(s);
		}
		break;

	case AVRO_INT:
		{
			int32_t i;
			rval = enc->read_int(reader, &i);
			*datum = avro_int(i);
		}
		break;

	case AVRO_LONG:
		{
			int64_t l;
			rval = enc->read_long(reader, &l);
			*datum = avro_long(l);
		}
		break;

	case AVRO_FLOAT:
		{
			float f;
			rval = enc->read_float(reader, &f);
			*datum = avro_float(f);
		}
		break;

	case AVRO_DOUBLE:
		{
			double d;
			rval = enc->read_double(reader, &d);
			*datum = avro_double(d);
		}
		break;

	case AVRO_BYTES:
		{
			char *bytes;
			int64_t len;
			rval = enc->read_bytes(reader, &bytes, &len);
			*datum = avro_givebytes(bytes, len);
		}
		break;

	case AVRO_FIXED:
		rval =
		    read_fixed(reader, enc, writers_schema, readers_schema,
			       datum);
		break;

	case AVRO_ENUM:
		rval =
		    read_enum(reader, enc, writers_schema, readers_schema,
			      datum);
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
		    read_union(reader, enc, writers_schema, readers_schema,
			       datum);
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
