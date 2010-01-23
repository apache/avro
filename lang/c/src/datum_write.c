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
#include <assert.h>
#include <string.h>
#include "schema.h"
#include "datum.h"
#include "encoding.h"

static int
write_record(avro_writer_t writer, const avro_encoding_t * enc,
	     struct avro_record_schema_t *record, avro_datum_t datum)
{
	int rval;
	struct avro_record_field_t *field = STAILQ_FIRST(&record->fields);
	for (; field != NULL; field = STAILQ_NEXT(field, fields)) {
		rval = avro_write_data(writer, field->type,
				       avro_record_field_get(datum,
							     field->name));
		if (rval) {
			return rval;
		}
	}
	return 0;
}

static int
write_enum(avro_writer_t writer, const avro_encoding_t * enc,
	   struct avro_enum_schema_t *enump, struct avro_enum_datum_t *datum)
{
	int64_t index;
	struct avro_enum_symbol_t *sym = STAILQ_FIRST(&enump->symbols);
	for (index = 0; sym != NULL; sym = STAILQ_NEXT(sym, symbols), index++) {
		if (strcmp(sym->symbol, datum->symbol) == 0) {
			break;
		}
	}
	if (!sym) {
		return EINVAL;
	}
	return enc->write_long(writer, index);
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
	rval = avro_write_data(args->writer, args->values_schema, datum);
	if (rval) {
		args->rval = rval;
		return ST_STOP;
	}
	return ST_CONTINUE;
}

static int
write_map(avro_writer_t writer, const avro_encoding_t * enc,
	  struct avro_map_schema_t *writer_schema,
	  struct avro_map_datum_t *datum)
{
	int rval;
	struct write_map_args args = { 0, writer, enc, writer_schema->values };

	if (datum->map->num_entries) {
		rval = enc->write_long(writer, datum->map->num_entries);
		if (rval) {
			return rval;
		}
		st_foreach(datum->map, write_map_foreach, (st_data_t) & args);
	}
	if (!args.rval) {
		rval = enc->write_long(writer, 0);
		if (rval) {
			return rval;
		}
		return 0;
	}
	return args.rval;
}

static int
write_array(avro_writer_t writer, const avro_encoding_t * enc,
	    struct avro_array_schema_t *schema,
	    struct avro_array_datum_t *array)
{
	int rval;
	struct avro_array_element_t *el;

	if (array->num_elements) {
		rval = enc->write_long(writer, array->num_elements);
		if (rval) {
			return rval;
		}
		for (el = STAILQ_FIRST(&array->els);
		     el != NULL; el = STAILQ_NEXT(el, els)) {
			rval =
			    avro_write_data(writer, schema->items, el->datum);
			if (rval) {
				return rval;
			}
		}
	}
	return enc->write_long(writer, 0);
}

static int
write_union(avro_writer_t writer, const avro_encoding_t * enc,
	    struct avro_union_schema_t *schema, avro_datum_t datum)
{
	int rval;
	int64_t index;
	struct avro_union_branch_t *branch = STAILQ_FIRST(&schema->branches);
	for (index = 0; branch != NULL;
	     branch = STAILQ_NEXT(branch, branches), index++) {
		if (avro_schema_datum_validate(branch->schema, datum)) {
			break;
		}
	}
	if (!branch) {
		return EINVAL;
	}
	rval = enc->write_long(writer, index);
	if (rval) {
		return rval;
	}
	return avro_write_data(writer, branch->schema, datum);
}

int
avro_write_data(avro_writer_t writer, avro_schema_t writer_schema,
		avro_datum_t datum)
{
	const avro_encoding_t *enc = &avro_binary_encoding;
	int rval = -1;

	if (!writer || !(is_avro_schema(writer_schema) && is_avro_datum(datum))) {
		return EINVAL;
	}
	if (!avro_schema_datum_validate(writer_schema, datum)) {
		return EINVAL;
	}
	switch (avro_typeof(writer_schema)) {
	case AVRO_NULL:
		rval = enc->write_null(writer);
		break;
	case AVRO_BOOLEAN:
		rval =
		    enc->write_boolean(writer, avro_datum_to_boolean(datum)->i);
		break;
	case AVRO_STRING:
		rval =
		    enc->write_string(writer, avro_datum_to_string(datum)->s);
		break;
	case AVRO_BYTES:
		rval =
		    enc->write_bytes(writer, avro_datum_to_bytes(datum)->bytes,
				     avro_datum_to_bytes(datum)->size);
		break;
	case AVRO_INT:
		{
			int32_t i;
			if (is_avro_int(datum)) {
				i = avro_datum_to_int(datum)->i;
			} else if (is_avro_long(datum)) {
				i = (int32_t) avro_datum_to_long(datum)->l;
			} else {
				assert(0
				       &&
				       "Serious bug in schema validation code");
			}
			rval = enc->write_int(writer, i);
		}
		break;
	case AVRO_LONG:
		rval = enc->write_long(writer, avro_datum_to_long(datum)->l);
		break;
	case AVRO_FLOAT:
		{
			float f;
			if (is_avro_int(datum)) {
				f = (float)(avro_datum_to_int(datum)->i);
			} else if (is_avro_long(datum)) {
				f = (float)(avro_datum_to_long(datum)->l);
			} else if (is_avro_float(datum)) {
				f = avro_datum_to_float(datum)->f;
			} else if (is_avro_double(datum)) {
				f = (float)(avro_datum_to_double(datum)->d);
			} else {
				assert(0
				       &&
				       "Serious bug in schema validation code");
			}
			rval = enc->write_float(writer, f);
		}
		break;
	case AVRO_DOUBLE:
		{
			double d;
			if (is_avro_int(datum)) {
				d = (double)(avro_datum_to_int(datum)->i);
			} else if (is_avro_long(datum)) {
				d = (double)(avro_datum_to_long(datum)->l);
			} else if (is_avro_float(datum)) {
				d = (double)(avro_datum_to_float(datum)->f);
			} else if (is_avro_double(datum)) {
				d = avro_datum_to_double(datum)->d;
			} else {
				assert(0 && "Bug in schema validation code");
			}
			rval = enc->write_double(writer, d);
		}
		break;

	case AVRO_RECORD:
		rval =
		    write_record(writer, enc,
				 avro_schema_to_record(writer_schema), datum);
		break;

	case AVRO_ENUM:
		rval =
		    write_enum(writer, enc, avro_schema_to_enum(writer_schema),
			       avro_datum_to_enum(datum));
		break;

	case AVRO_FIXED:
		return avro_write(writer, avro_datum_to_fixed(datum)->bytes,
				  avro_datum_to_fixed(datum)->size);

	case AVRO_MAP:
		rval =
		    write_map(writer, enc, avro_schema_to_map(writer_schema),
			      avro_datum_to_map(datum));
		break;
	case AVRO_ARRAY:
		rval =
		    write_array(writer, enc,
				avro_schema_to_array(writer_schema),
				avro_datum_to_array(datum));
		break;

	case AVRO_UNION:
		rval =
		    write_union(writer, enc,
				avro_schema_to_union(writer_schema), datum);
		break;

	case AVRO_LINK:
		rval =
		    avro_write_data(writer,
				    (avro_schema_to_link(writer_schema))->to,
				    datum);
		break;
	}
	return rval;
}
