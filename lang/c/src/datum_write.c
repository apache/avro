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
#include <errno.h>
#include <assert.h>
#include <string.h>
#include "schema.h"
#include "datum.h"
#include "encoding.h"

static int write_datum(avro_writer_t writer, const avro_encoding_t * enc,
		       avro_schema_t writers_schema, avro_datum_t datum);

static int
write_record(avro_writer_t writer, const avro_encoding_t * enc,
	     struct avro_record_schema_t *schema, avro_datum_t datum)
{
	int rval;
	long i;
	avro_datum_t field_datum;

	if (schema) {
		for (i = 0; i < schema->fields->num_entries; i++) {
			union {
				st_data_t data;
				struct avro_record_field_t *field;
			} val;
			st_lookup(schema->fields, i, &val.data);
			check(rval,
			      avro_record_get(datum, val.field->name,
					      &field_datum));
			check(rval,
			      write_datum(writer, enc, val.field->type,
					  field_datum));
		}
	} else {
		/* No schema.  Just write the record datum */
		struct avro_record_datum_t *record =
		    avro_datum_to_record(datum);
		for (i = 0; i < record->field_order->num_entries; i++) {
			union {
				st_data_t data;
				char *name;
			} val;
			st_lookup(record->field_order, i, &val.data);
			check(rval,
			      avro_record_get(datum, val.name, &field_datum));
			check(rval,
			      write_datum(writer, enc, NULL, field_datum));
		}
	}
	return 0;
}

static int
write_enum(avro_writer_t writer, const avro_encoding_t * enc,
	   struct avro_enum_schema_t *enump, struct avro_enum_datum_t *datum)
{
	AVRO_UNUSED(enump);

	return enc->write_long(writer, datum->value);
}

struct write_map_args {
	int rval;
	avro_writer_t writer;
	const avro_encoding_t *enc;
	avro_schema_t values_schema;
};

static int
write_map_foreach(char *key, avro_datum_t datum, struct write_map_args *args)
{
	int rval = args->enc->write_string(args->writer, key);
	if (rval) {
		args->rval = rval;
		return ST_STOP;
	}
	rval = write_datum(args->writer, args->enc, args->values_schema, datum);
	if (rval) {
		args->rval = rval;
		return ST_STOP;
	}
	return ST_CONTINUE;
}

static int
write_map(avro_writer_t writer, const avro_encoding_t * enc,
	  struct avro_map_schema_t *writers_schema,
	  struct avro_map_datum_t *datum)
{
	int rval;
	struct write_map_args args =
	    { 0, writer, enc, writers_schema ? writers_schema->values : NULL };

	if (datum->map->num_entries) {
		check_prefix(rval, enc->write_long(writer, datum->map->num_entries),
			     "Cannot write map block count: ");
		st_foreach(datum->map, write_map_foreach, (st_data_t) & args);
	}
	if (args.rval) {
		return args.rval;
	}

	check_prefix(rval, enc->write_long(writer, 0),
		     "Cannot write map block count: ");
	return 0;
}

static int
write_array(avro_writer_t writer, const avro_encoding_t * enc,
	    struct avro_array_schema_t *schema,
	    struct avro_array_datum_t *array)
{
	int rval;
	long i;

	if (array->els->num_entries) {
		check_prefix(rval, enc->write_long(writer, array->els->num_entries),
			     "Cannot write array block count: ");
		for (i = 0; i < array->els->num_entries; i++) {
			union {
				st_data_t data;
				avro_datum_t datum;
			} val;
			st_lookup(array->els, i, &val.data);
			check(rval,
			      write_datum(writer, enc,
					  schema ? schema->items : NULL,
					  val.datum));
		}
	}
	check_prefix(rval, enc->write_long(writer, 0),
		     "Cannot write array block count: ");
	return 0;
}

static int
write_union(avro_writer_t writer, const avro_encoding_t * enc,
	    struct avro_union_schema_t *schema,
	    struct avro_union_datum_t *unionp)
{
	int rval;
	avro_schema_t write_schema = NULL;

	check_prefix(rval, enc->write_long(writer, unionp->discriminant),
		     "Cannot write union discriminant: ");
	if (schema) {
		write_schema =
		    avro_schema_union_branch(&schema->obj, unionp->discriminant);
		if (!write_schema) {
			return EINVAL;
		}
	}
	return write_datum(writer, enc, write_schema, unionp->value);
}

static int write_datum(avro_writer_t writer, const avro_encoding_t * enc,
		       avro_schema_t writers_schema, avro_datum_t datum)
{
	if (is_avro_schema(writers_schema) && is_avro_link(writers_schema)) {
		return write_datum(writer, enc,
				   (avro_schema_to_link(writers_schema))->to,
				   datum);
	}

	switch (avro_typeof(datum)) {
	case AVRO_NULL:
		return enc->write_null(writer);

	case AVRO_BOOLEAN:
		return enc->write_boolean(writer,
					  avro_datum_to_boolean(datum)->i);

	case AVRO_STRING:
		return enc->write_string(writer,
					 avro_datum_to_string(datum)->s);

	case AVRO_BYTES:
		return enc->write_bytes(writer,
					avro_datum_to_bytes(datum)->bytes,
					avro_datum_to_bytes(datum)->size);

	case AVRO_INT32:
	case AVRO_INT64:{
			int64_t val = avro_typeof(datum) == AVRO_INT32 ?
			    avro_datum_to_int32(datum)->i32 :
			    avro_datum_to_int64(datum)->i64;
			if (is_avro_schema(writers_schema)) {
				/* handle promotion */
				if (is_avro_float(writers_schema)) {
					return enc->write_float(writer,
								(float)val);
				} else if (is_avro_double(writers_schema)) {
					return enc->write_double(writer,
								 (double)val);
				}
			}
			return enc->write_long(writer, val);
		}

	case AVRO_FLOAT:{
			float val = avro_datum_to_float(datum)->f;
			if (is_avro_schema(writers_schema)
			    && is_avro_double(writers_schema)) {
				/* handle promotion */
				return enc->write_double(writer, (double)val);
			}
			return enc->write_float(writer, val);
		}

	case AVRO_DOUBLE:
		return enc->write_double(writer,
					 avro_datum_to_double(datum)->d);

	case AVRO_RECORD:
		return write_record(writer, enc,
				    avro_schema_to_record(writers_schema),
				    datum);

	case AVRO_ENUM:
		return write_enum(writer, enc,
				  avro_schema_to_enum(writers_schema),
				  avro_datum_to_enum(datum));

	case AVRO_FIXED:
		return avro_write(writer,
				  avro_datum_to_fixed(datum)->bytes,
				  avro_datum_to_fixed(datum)->size);

	case AVRO_MAP:
		return write_map(writer, enc,
				 avro_schema_to_map(writers_schema),
				 avro_datum_to_map(datum));

	case AVRO_ARRAY:
		return write_array(writer, enc,
				   avro_schema_to_array(writers_schema),
				   avro_datum_to_array(datum));

	case AVRO_UNION:
		return write_union(writer, enc,
				   avro_schema_to_union(writers_schema),
				   avro_datum_to_union(datum));

	case AVRO_LINK:
		break;
	}

	return 0;
}

int avro_write_data(avro_writer_t writer, avro_schema_t writers_schema,
		    avro_datum_t datum)
{
	check_param(EINVAL, writer, "writer");
	check_param(EINVAL, is_avro_datum(datum), "datum");
	/* Only validate datum if a writer's schema is provided */
	if (is_avro_schema(writers_schema)
	    && !avro_schema_datum_validate(writers_schema, datum)) {
		avro_set_error("Datum doesn't validate against schema");
		return EINVAL;
	}
	return write_datum(writer, &avro_binary_encoding,
			   writers_schema, datum);
}
