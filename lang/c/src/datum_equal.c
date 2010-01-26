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

#include <string.h>
#include "datum.h"

static int
array_equal(struct avro_array_datum_t *a, struct avro_array_datum_t *b)
{
	struct avro_array_element_t *a_el, *b_el;
	if (a->num_elements != b->num_elements) {
		return 0;
	}
	for (a_el = STAILQ_FIRST(&a->els),
	     b_el = STAILQ_FIRST(&b->els);
	     !(a_el == NULL && a_el == NULL);
	     a_el = STAILQ_NEXT(a_el, els), b_el = STAILQ_NEXT(b_el, els)) {
		if (a_el == NULL || b_el == NULL) {
			return 0;	/* different number of elements */
		}
		if (!avro_datum_equal(a_el->datum, b_el->datum)) {
			return 0;
		}
	}
	return 1;
}

struct st_equal_args {
	int rval;
	st_table *st;
};

static int
st_equal_foreach(char *key, avro_datum_t datum, struct st_equal_args *args)
{
	union {
		avro_datum_t datum_other;
		st_data_t data;
	} val;
	if (!st_lookup(args->st, (st_data_t) key, &(val.data))) {
		args->rval = 0;
		return ST_STOP;
	}
	if (!avro_datum_equal(datum, val.datum_other)) {
		args->rval = 0;
		return ST_STOP;
	}
	return ST_CONTINUE;
}

static int map_equal(struct avro_map_datum_t *a, struct avro_map_datum_t *b)
{
	struct st_equal_args args = { 1, b->map };
	if (a->map->num_entries != b->map->num_entries) {
		return 0;
	}
	st_foreach(a->map, st_equal_foreach, (st_data_t) & args);
	return args.rval;
}

static int record_equal(struct avro_record_datum_t *a,
			struct avro_record_datum_t *b)
{
	struct st_equal_args args = { 1, b->fields };
	if (a->fields->num_entries != b->fields->num_entries) {
		return 0;
	}
	st_foreach(a->fields, st_equal_foreach, (st_data_t) & args);
	return args.rval;
}

static int enum_equal(struct avro_enum_datum_t *a, struct avro_enum_datum_t *b)
{
	return strcmp(a->name, b->name) == 0
	    && strcmp(a->symbol, b->symbol) == 0;
}

static int fixed_equal(struct avro_fixed_datum_t *a,
		       struct avro_fixed_datum_t *b)
{
	return a->size == b->size && memcmp(a->bytes, b->bytes, a->size) == 0;
}

int avro_datum_equal(avro_datum_t a, avro_datum_t b)
{
	if (!(is_avro_datum(a) && is_avro_datum(b))) {
		return 0;
	}
	if (avro_typeof(a) != avro_typeof(b)) {
		return 0;
	}
	switch (avro_typeof(a)) {
	case AVRO_STRING:
		return strcmp(avro_datum_to_string(a)->s,
			      avro_datum_to_string(b)->s) == 0;
	case AVRO_BYTES:
		return (avro_datum_to_bytes(a)->size ==
			avro_datum_to_bytes(b)->size)
		    && memcmp(avro_datum_to_bytes(a)->bytes,
			      avro_datum_to_bytes(b)->bytes,
			      avro_datum_to_bytes(a)->size) == 0;
	case AVRO_INT32:
		return avro_datum_to_int32(a)->i32 ==
		    avro_datum_to_int32(b)->i32;
	case AVRO_INT64:
		return avro_datum_to_int64(a)->i64 ==
		    avro_datum_to_int64(b)->i64;
	case AVRO_FLOAT:
		return avro_datum_to_float(a)->f == avro_datum_to_float(b)->f;
	case AVRO_DOUBLE:
		return avro_datum_to_double(a)->d == avro_datum_to_double(b)->d;
	case AVRO_BOOLEAN:
		return avro_datum_to_boolean(a)->i ==
		    avro_datum_to_boolean(b)->i;
	case AVRO_NULL:
		return 1;
	case AVRO_ARRAY:
		return array_equal(avro_datum_to_array(a),
				   avro_datum_to_array(b));
	case AVRO_MAP:
		return map_equal(avro_datum_to_map(a), avro_datum_to_map(b));

	case AVRO_RECORD:
		return record_equal(avro_datum_to_record(a),
				    avro_datum_to_record(b));

	case AVRO_ENUM:
		return enum_equal(avro_datum_to_enum(a), avro_datum_to_enum(b));

	case AVRO_FIXED:
		return fixed_equal(avro_datum_to_fixed(a),
				   avro_datum_to_fixed(b));

	case AVRO_UNION:
		break;
	case AVRO_LINK:
		/*
		 * TODO 
		 */
		return 0;
	}
	return 0;
}
