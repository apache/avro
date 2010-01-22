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
#include "avro.h"
#include "datum.h"
#include "encoding.h"

static void avro_datum_init(avro_datum_t datum, avro_type_t type)
{
	datum->type = type;
	datum->class_type = AVRO_DATUM;
	datum->refcount = 1;
}

static avro_datum_t avro_string_private(char *str,
					void (*string_free) (void *ptr))
{
	struct avro_string_datum_t *datum =
	    malloc(sizeof(struct avro_string_datum_t));
	if (!datum) {
		return NULL;
	}
	datum->s = str;
	datum->free = string_free;

	avro_datum_init(&datum->obj, AVRO_STRING);
	return &datum->obj;
}

avro_datum_t avro_string(const char *str)
{
	char *p = strdup(str);
	if (!p) {
		return NULL;
	}
	return avro_string_private(p, free);
}

avro_datum_t avro_givestring(const char *str)
{
	return avro_string_private((char *)str, free);
}

avro_datum_t avro_wrapstring(const char *str)
{
	return avro_string_private((char *)str, NULL);
}

static avro_datum_t avro_bytes_private(char *bytes, int64_t size,
				       void (*bytes_free) (void *ptr))
{
	struct avro_bytes_datum_t *datum;
	datum = malloc(sizeof(struct avro_bytes_datum_t));
	if (!datum) {
		return NULL;
	}
	datum->bytes = bytes;
	datum->size = size;
	datum->free = bytes_free;

	avro_datum_init(&datum->obj, AVRO_BYTES);
	return &datum->obj;
}

avro_datum_t avro_bytes(const char *bytes, int64_t size)
{
	char *bytes_copy = malloc(size);
	if (!bytes_copy) {
		return NULL;
	}
	memcpy(bytes_copy, bytes, size);
	return avro_bytes_private(bytes_copy, size, free);
}

avro_datum_t avro_givebytes(const char *bytes, int64_t size)
{
	return avro_bytes_private((char *)bytes, size, free);
}

avro_datum_t avro_wrapbytes(const char *bytes, int64_t size)
{
	return avro_bytes_private((char *)bytes, size, NULL);
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
	char *key = (char *)field_name;
	union {
		avro_datum_t old_value;
		st_data_t data;
	} val;

	if (is_avro_datum(datum) && is_avro_record(datum)) {
		struct avro_record_datum_t *record =
		    avro_datum_to_record(datum);
		if (st_lookup
		    (record->fields, (st_data_t) field_name, &val.data)) {
			/* Overriding old value */
			avro_datum_decref(val.old_value);
		} else {
			/* Inserting new value */
			key = strdup(field_name);
			if (!key) {
				return ENOMEM;
			}
		}
		st_insert(record->fields, (st_data_t) key,
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

static avro_datum_t avro_fixed_private(const char *name, const char *bytes,
				       const int64_t size,
				       void (*fixed_free) (void *ptr))
{
	struct avro_fixed_datum_t *datum =
	    malloc(sizeof(struct avro_fixed_datum_t));
	if (!datum) {
		return NULL;
	}
	datum->name = strdup(name);
	datum->size = size;
	datum->bytes = (char *)bytes;
	datum->free = fixed_free;

	avro_datum_init(&datum->obj, AVRO_FIXED);
	return &datum->obj;
}

avro_datum_t avro_fixed(const char *name, const char *bytes, const int64_t size)
{
	char *bytes_copy = malloc(size);
	if (!bytes_copy) {
		return NULL;
	}
	memcpy(bytes_copy, bytes, size);
	return avro_fixed_private(name, bytes, size, free);
}

avro_datum_t avro_wrapfixed(const char *name, const char *bytes,
			    const int64_t size)
{
	return avro_fixed_private(name, bytes, size, NULL);
}

avro_datum_t avro_givefixed(const char *name, const char *bytes,
			    const int64_t size)
{
	return avro_fixed_private(name, bytes, size, free);
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
	char *save_key = (char *)key;
	struct avro_map_datum_t *map;
	union {
		st_data_t data;
		avro_datum_t old_datum;
	} val;

	if (!is_avro_datum(datum) || !is_avro_map(datum) || !key
	    || !is_avro_datum(value)) {
		return EINVAL;
	}
	map = avro_datum_to_map(datum);
	if (st_lookup(map->map, (st_data_t) key, &(val.data))) {
		/* Overwriting an old value */
		avro_datum_decref(val.old_datum);
	} else {
		/* Inserting a new value */
		save_key = strdup(key);
		if (!save_key) {
			return ENOMEM;
		}
	}
	st_insert(map->map, (st_data_t) save_key, (st_data_t) value);
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

static int char_datum_free_foreach(char *key, avro_datum_t datum, void *arg)
{
	avro_datum_decref(datum);
	free(key);
	return ST_DELETE;
}

static void avro_datum_free(avro_datum_t datum)
{
	if (is_avro_datum(datum)) {
		switch (avro_typeof(datum)) {
		case AVRO_STRING:{
				struct avro_string_datum_t *string;
				string = avro_datum_to_string(datum);
				if (string->free) {
					string->free(string->s);
				}
				free(string);
			}
			break;
		case AVRO_BYTES:{
				struct avro_bytes_datum_t *bytes;
				bytes = avro_datum_to_bytes(datum);
				if (bytes->free) {
					bytes->free(bytes->bytes);
				}
				free(bytes);
			}
			break;
		case AVRO_INT:{
				struct avro_int_datum_t *i;
				i = avro_datum_to_int(datum);
				free(i);
			}
			break;
		case AVRO_LONG:{
				struct avro_long_datum_t *l;
				l = avro_datum_to_long(datum);
				free(l);
			}
			break;
		case AVRO_FLOAT:{
				struct avro_float_datum_t *f;
				f = avro_datum_to_float(datum);
				free(f);
			}
			break;
		case AVRO_DOUBLE:{
				struct avro_double_datum_t *d;
				d = avro_datum_to_double(datum);
				free(d);
			}
			break;
		case AVRO_BOOLEAN:{
				struct avro_boolean_datum_t *b;
				b = avro_datum_to_boolean(datum);
				free(b);
			}
			break;
		case AVRO_NULL:
			/* Nothing allocated */
			break;

		case AVRO_RECORD:{
				struct avro_record_datum_t *record;
				record = avro_datum_to_record(datum);
				free((void *)record->name);
				st_foreach(record->fields,
					   char_datum_free_foreach, 0);
				st_free_table(record->fields);
				free(record);
			}
			break;
		case AVRO_ENUM:{
				struct avro_enum_datum_t *enump;
				enump = avro_datum_to_enum(datum);
				free((void *)enump->name);
				free((void *)enump->symbol);
				free(enump);
			}
			break;
		case AVRO_FIXED:{
				struct avro_fixed_datum_t *fixed;
				fixed = avro_datum_to_fixed(datum);
				free((void *)fixed->name);
				if (fixed->free) {
					fixed->free((void *)fixed->bytes);
				}
				free(fixed);
			}
			break;
		case AVRO_MAP:{
				struct avro_map_datum_t *map;
				map = avro_datum_to_map(datum);
				st_foreach(map->map, char_datum_free_foreach,
					   0);
				st_free_table(map->map);
				free(map);
			}
			break;
		case AVRO_ARRAY:{
				struct avro_array_datum_t *array;
				array = avro_datum_to_array(datum);
				while (!STAILQ_EMPTY(&array->els)) {
					struct avro_array_element_t *el;
					el = STAILQ_FIRST(&array->els);
					STAILQ_REMOVE_HEAD(&array->els, els);
					avro_datum_decref(el->datum);
					free(el);
				}
				free(array);
			}
			break;
		case AVRO_UNION:{
				/* TODO */
			}
			break;
		case AVRO_LINK:{
				/* TODO */
			}
			break;
		}
	}
}

avro_datum_t avro_datum_incref(avro_datum_t datum)
{
	if (datum && datum->refcount != (unsigned int)-1) {
		++datum->refcount;
	}
	return datum;
}

void avro_datum_decref(avro_datum_t datum)
{
	if (datum && datum->refcount != (unsigned int)-1
	    && --datum->refcount == 0) {
		avro_datum_free(datum);
	}
}

void avro_datum_print(avro_datum_t value, FILE * fp)
{

}
