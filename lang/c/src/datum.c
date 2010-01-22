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
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <assert.h>
#include "avro.h"
#include "schema.h"
#include "datum.h"
#include "encoding.h"

static void avro_datum_init(avro_datum_t datum, avro_type_t type)
{
	datum->type = type;
	datum->class_type = AVRO_DATUM;
	datum->refcount = 1;
}

avro_datum_t avro_string(const char *str)
{
	struct avro_string_datum_t *datum =
	    malloc(sizeof(struct avro_string_datum_t));
	if (!datum) {
		return NULL;
	}
	datum->s = strdup(str);

	avro_datum_init(&datum->obj, AVRO_STRING);
	return &datum->obj;
}

avro_datum_t avro_bytes(const char *bytes, int64_t size)
{
	struct avro_bytes_datum_t *datum =
	    malloc(sizeof(struct avro_bytes_datum_t));
	if (!datum) {
		return NULL;
	}
	datum->bytes = malloc(size);
	if (!datum->bytes) {
		free(datum);
		return NULL;
	}
	memcpy(datum->bytes, bytes, size);
	datum->size = size;

	avro_datum_init(&datum->obj, AVRO_BYTES);
	return &datum->obj;
}

avro_datum_t avro_int(int32_t i)
{
	struct avro_int_datum_t *datum =
	    malloc(sizeof(struct avro_int_datum_t));
	if (!datum) {
		return NULL;
	}
	datum->i = i;

	avro_datum_init(&datum->obj, AVRO_INT);
	return &datum->obj;
}

avro_datum_t avro_long(int64_t l)
{
	struct avro_long_datum_t *datum =
	    malloc(sizeof(struct avro_long_datum_t));
	if (!datum) {
		return NULL;
	}
	datum->l = l;

	avro_datum_init(&datum->obj, AVRO_LONG);
	return &datum->obj;
}

avro_datum_t avro_float(float f)
{
	struct avro_float_datum_t *datum =
	    malloc(sizeof(struct avro_float_datum_t));
	if (!datum) {
		return NULL;
	}
	datum->f = f;

	avro_datum_init(&datum->obj, AVRO_FLOAT);
	return &datum->obj;
}

avro_datum_t avro_double(double d)
{
	struct avro_double_datum_t *datum =
	    malloc(sizeof(struct avro_double_datum_t));
	if (!datum) {
		return NULL;
	}
	datum->d = d;

	avro_datum_init(&datum->obj, AVRO_DOUBLE);
	return &datum->obj;
}

avro_datum_t avro_boolean(int8_t i)
{
	struct avro_boolean_datum_t *datum =
	    malloc(sizeof(struct avro_boolean_datum_t));
	if (!datum) {
		return NULL;
	}
	datum->i = i;
	avro_datum_init(&datum->obj, AVRO_BOOLEAN);
	return &datum->obj;
}

avro_datum_t avro_null(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_NULL,
		.class_type = AVRO_DATUM,
		.refcount = 1
	};
	return &obj;
}

avro_datum_t avro_record(const char *name)
{
	struct avro_record_datum_t *datum =
	    malloc(sizeof(struct avro_record_datum_t));
	if (!datum) {
		return NULL;
	}
	datum->name = strdup(name);
	datum->fields = st_init_strtable();

	avro_datum_init(&datum->obj, AVRO_RECORD);
	return &datum->obj;
}

avro_datum_t
avro_record_field_get(const avro_datum_t datum, const char *field_name)
{
	union {
		avro_datum_t field;
		st_data_t data;
	} val;
	if (is_avro_datum(datum) && is_avro_record(datum)) {
		struct avro_record_datum_t *record =
		    avro_datum_to_record(datum);
		if (st_lookup
		    (record->fields, (st_data_t) field_name, &(val.data))) {
			return val.field;
		}
	}
	return NULL;
}

int
avro_record_field_set(const avro_datum_t datum,
		      const char *field_name, const avro_datum_t field_value)
{
	if (is_avro_datum(datum) && is_avro_record(datum)) {
		struct avro_record_datum_t *record =
		    avro_datum_to_record(datum);
		st_insert(record->fields, (st_data_t) field_name,
			  (st_data_t) field_value);
		return 0;
	}
	return EINVAL;
}

avro_datum_t avro_enum(const char *name, const char *symbol)
{
	struct avro_enum_datum_t *datum =
	    malloc(sizeof(struct avro_enum_datum_t));
	if (!datum) {
		return NULL;
	}
	datum->name = strdup(name);
	datum->symbol = strdup(symbol);

	avro_datum_init(&datum->obj, AVRO_ENUM);
	return &datum->obj;
}

avro_datum_t avro_fixed(const char *name, const int64_t size, const char *bytes)
{
	struct avro_fixed_datum_t *datum =
	    malloc(sizeof(struct avro_fixed_datum_t));
	if (!datum) {
		return NULL;
	}
	datum->name = strdup(name);
	datum->size = size;
	datum->bytes = malloc(size);
	if (datum->bytes) {
		free(datum);
		return NULL;
	}
	memcpy(datum->bytes, bytes, size);
	avro_datum_init(&datum->obj, AVRO_FIXED);
	return &datum->obj;
}

avro_datum_t avro_map(void)
{
	struct avro_map_datum_t *datum =
	    malloc(sizeof(struct avro_map_datum_t));
	if (!datum) {
		return NULL;
	}
	datum->map = st_init_strtable();
	avro_datum_init(&datum->obj, AVRO_MAP);
	return &datum->obj;
}

int
avro_map_set(const avro_datum_t datum, const char *key,
	     const avro_datum_t value)
{
	struct avro_map_datum_t *map;
	if (!is_avro_datum(datum) || !is_avro_map(datum) || !key
	    || !is_avro_datum(value)) {
		return EINVAL;
	}
	map = avro_datum_to_map(datum);
	st_insert(map->map, (st_data_t) key, (st_data_t) value);
	return 0;
}

avro_datum_t avro_array(void)
{
	struct avro_array_datum_t *datum =
	    malloc(sizeof(struct avro_array_datum_t));
	if (!datum) {
		return NULL;
	}
	STAILQ_INIT(&datum->els);
	datum->num_elements = 0;

	avro_datum_init(&datum->obj, AVRO_ARRAY);
	return &datum->obj;
}

int
avro_array_append_datum(const avro_datum_t array_datum,
			const avro_datum_t datum)
{
	struct avro_array_datum_t *array;
	struct avro_array_element_t *el;
	if (!is_avro_datum(array_datum) || !is_avro_array(array_datum)
	    || !is_avro_datum(datum)) {
		return EINVAL;
	}
	array = avro_datum_to_array(array_datum);
	el = malloc(sizeof(struct avro_array_element_t));
	if (!el) {
		return ENOMEM;
	}
	el->datum = datum;
	STAILQ_INSERT_TAIL(&array->els, el, els);
	array->num_elements++;
	return 0;
}

avro_datum_t avro_datum_incref(avro_datum_t value)
{
	/*
	 * TODO 
	 */
	return value;
}

void avro_datum_decref(avro_datum_t value)
{

}

void avro_datum_print(avro_datum_t value, FILE * fp)
{

}
