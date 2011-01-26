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

#include "avro_private.h"
#include "allocation.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "datum.h"
#include "encoding.h"

#define DEFAULT_TABLE_SIZE 32

static void avro_datum_init(avro_datum_t datum, avro_type_t type)
{
	datum->type = type;
	datum->class_type = AVRO_DATUM;
	datum->refcount = 1;
}

static void
avro_str_free_wrapper(void *ptr, size_t sz)
{
	// don't need sz, since the size is stored in the string buffer
	AVRO_UNUSED(sz);
	avro_str_free(ptr);
}

static avro_datum_t avro_string_private(char *str, int64_t size,
					avro_free_func_t string_free)
{
	struct avro_string_datum_t *datum =
	    avro_new(struct avro_string_datum_t);
	if (!datum) {
		return NULL;
	}
	datum->s = str;
	datum->size = size;
	datum->free = string_free;

	avro_datum_init(&datum->obj, AVRO_STRING);
	return &datum->obj;
}

avro_datum_t avro_string(const char *str)
{
	char *p = avro_strdup(str);
	if (!p) {
		return NULL;
	}
	return avro_string_private(p, 0, avro_str_free_wrapper);
}

avro_datum_t avro_givestring(const char *str,
			     avro_free_func_t free)
{
	int64_t  sz = strlen(str)+1;
	return avro_string_private((char *)str, sz, free);
}

int avro_string_get(avro_datum_t datum, char **p)
{
	if (!(is_avro_datum(datum) && is_avro_string(datum)) || !p) {
		return EINVAL;
	}
	*p = avro_datum_to_string(datum)->s;
	return 0;
}

static int avro_string_set_private(avro_datum_t datum,
	       			   const char *p, int64_t size,
				   avro_free_func_t string_free)
{
	struct avro_string_datum_t *string;
	if (!(is_avro_datum(datum) && is_avro_string(datum)) || !p) {
		return EINVAL;
	}
	string = avro_datum_to_string(datum);
	if (string->free) {
		string->free(string->s, string->size);
	}
	string->free = string_free;
	string->s = (char *)p;
	string->size = size;
	return 0;
}

int avro_string_set(avro_datum_t datum, const char *p)
{
	char *string_copy = avro_strdup(p);
	int rval;
	if (!string_copy) {
		return ENOMEM;
	}
	rval = avro_string_set_private(datum, string_copy, 0,
				       avro_str_free_wrapper);
	if (rval) {
		avro_str_free(string_copy);
	}
	return rval;
}

int avro_givestring_set(avro_datum_t datum, const char *p,
			avro_free_func_t free)
{
	int64_t  size = strlen(p)+1;
	return avro_string_set_private(datum, p, size, free);
}

static avro_datum_t avro_bytes_private(char *bytes, int64_t size,
				       avro_free_func_t bytes_free)
{
	struct avro_bytes_datum_t *datum;
	datum = avro_new(struct avro_bytes_datum_t);
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
	char *bytes_copy = avro_malloc(size);
	if (!bytes_copy) {
		return NULL;
	}
	memcpy(bytes_copy, bytes, size);
	avro_datum_t  result =
		avro_bytes_private(bytes_copy, size, avro_alloc_free);
	if (result == NULL) {
		avro_free(bytes_copy, size);
	}
	return result;
}

avro_datum_t avro_givebytes(const char *bytes, int64_t size,
			    avro_free_func_t free)
{
	return avro_bytes_private((char *)bytes, size, free);
}

static int avro_bytes_set_private(avro_datum_t datum, const char *bytes,
				  const int64_t size,
				  avro_free_func_t bytes_free)
{
	struct avro_bytes_datum_t *b;

	if (!(is_avro_datum(datum) && is_avro_bytes(datum))) {
		return EINVAL;
	}

	b = avro_datum_to_bytes(datum);
	if (b->free) {
		b->free(b->bytes, b->size);
	}

	b->free = bytes_free;
	b->bytes = (char *)bytes;
	b->size = size;
	return 0;
}

int avro_bytes_set(avro_datum_t datum, const char *bytes, const int64_t size)
{
	int rval;
	char *bytes_copy = avro_malloc(size);
	if (!bytes_copy) {
		return ENOMEM;
	}
	memcpy(bytes_copy, bytes, size);
	rval = avro_bytes_set_private(datum, bytes_copy, size, avro_alloc_free);
	if (rval) {
		avro_free(bytes_copy, size);
	}
	return rval;
}

int avro_givebytes_set(avro_datum_t datum, const char *bytes,
		       const int64_t size, avro_free_func_t free)
{
	return avro_bytes_set_private(datum, bytes, size, free);
}

int avro_bytes_get(avro_datum_t datum, char **bytes, int64_t * size)
{
	if (!(is_avro_datum(datum) && is_avro_bytes(datum)) || !bytes || !size) {
		return EINVAL;
	}
	*bytes = avro_datum_to_bytes(datum)->bytes;
	*size = avro_datum_to_bytes(datum)->size;
	return 0;
}

avro_datum_t avro_int32(int32_t i)
{
	struct avro_int32_datum_t *datum =
	    avro_new(struct avro_int32_datum_t);
	if (!datum) {
		return NULL;
	}
	datum->i32 = i;

	avro_datum_init(&datum->obj, AVRO_INT32);
	return &datum->obj;
}

int avro_int32_get(avro_datum_t datum, int32_t * i)
{
	if (!(is_avro_datum(datum) && is_avro_int32(datum)) || !i) {
		return EINVAL;
	}
	*i = avro_datum_to_int32(datum)->i32;
	return 0;
}

int avro_int32_set(avro_datum_t datum, const int32_t i)
{
	struct avro_int32_datum_t *intp;
	if (!(is_avro_datum(datum) && is_avro_int32(datum))) {
		return EINVAL;
	}
	intp = avro_datum_to_int32(datum);
	intp->i32 = i;
	return 0;
}

avro_datum_t avro_int64(int64_t l)
{
	struct avro_int64_datum_t *datum =
	    avro_new(struct avro_int64_datum_t);
	if (!datum) {
		return NULL;
	}
	datum->i64 = l;

	avro_datum_init(&datum->obj, AVRO_INT64);
	return &datum->obj;
}

int avro_int64_get(avro_datum_t datum, int64_t * l)
{
	if (!(is_avro_datum(datum) && is_avro_int64(datum)) || !l) {
		return EINVAL;
	}
	*l = avro_datum_to_int64(datum)->i64;
	return 0;
}

int avro_int64_set(avro_datum_t datum, const int64_t l)
{
	struct avro_int64_datum_t *longp;
	if (!(is_avro_datum(datum) && is_avro_int64(datum))) {
		return EINVAL;
	}
	longp = avro_datum_to_int64(datum);
	longp->i64 = l;
	return 0;
}

avro_datum_t avro_float(float f)
{
	struct avro_float_datum_t *datum =
	    avro_new(struct avro_float_datum_t);
	if (!datum) {
		return NULL;
	}
	datum->f = f;

	avro_datum_init(&datum->obj, AVRO_FLOAT);
	return &datum->obj;
}

int avro_float_set(avro_datum_t datum, const float f)
{
	struct avro_float_datum_t *floatp;
	if (!(is_avro_datum(datum) && is_avro_float(datum))) {
		return EINVAL;
	}
	floatp = avro_datum_to_float(datum);
	floatp->f = f;
	return 0;
}

int avro_float_get(avro_datum_t datum, float *f)
{
	if (!(is_avro_datum(datum) && is_avro_float(datum)) || !f) {
		return EINVAL;
	}
	*f = avro_datum_to_float(datum)->f;
	return 0;
}

avro_datum_t avro_double(double d)
{
	struct avro_double_datum_t *datum =
	    avro_new(struct avro_double_datum_t);
	if (!datum) {
		return NULL;
	}
	datum->d = d;

	avro_datum_init(&datum->obj, AVRO_DOUBLE);
	return &datum->obj;
}

int avro_double_set(avro_datum_t datum, const double d)
{
	struct avro_double_datum_t *doublep;
	if (!(is_avro_datum(datum) && is_avro_double(datum))) {
		return EINVAL;
	}
	doublep = avro_datum_to_double(datum);
	doublep->d = d;
	return 0;
}

int avro_double_get(avro_datum_t datum, double *d)
{
	if (!(is_avro_datum(datum) && is_avro_double(datum)) || !d) {
		return EINVAL;
	}
	*d = avro_datum_to_double(datum)->d;
	return 0;
}

avro_datum_t avro_boolean(int8_t i)
{
	struct avro_boolean_datum_t *datum =
	    avro_new(struct avro_boolean_datum_t);
	if (!datum) {
		return NULL;
	}
	datum->i = i;
	avro_datum_init(&datum->obj, AVRO_BOOLEAN);
	return &datum->obj;
}

int avro_boolean_set(avro_datum_t datum, const int8_t i)
{
	struct avro_boolean_datum_t *booleanp;
	if (!(is_avro_datum(datum) && is_avro_boolean(datum))) {
		return EINVAL;
	}
	booleanp = avro_datum_to_boolean(datum);
	booleanp->i = i;
	return 0;
}

int avro_boolean_get(avro_datum_t datum, int8_t * i)
{
	if (!(is_avro_datum(datum) && is_avro_boolean(datum)) || !i) {
		return EINVAL;
	}
	*i = avro_datum_to_boolean(datum)->i;
	return 0;
}

avro_datum_t avro_null(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_NULL,
		.class_type = AVRO_DATUM,
		.refcount = 1
	};
	return avro_datum_incref(&obj);
}

avro_datum_t avro_union(int64_t discriminant, avro_datum_t value)
{
	struct avro_union_datum_t *datum =
	    avro_new(struct avro_union_datum_t);
	if (!datum) {
		return NULL;
	}
	datum->discriminant = discriminant;
	datum->value = avro_datum_incref(value);

	avro_datum_init(&datum->obj, AVRO_UNION);
	return &datum->obj;
}

int64_t avro_union_discriminant(const avro_datum_t datum)
{
	return avro_datum_to_union(datum)->discriminant;
}

avro_datum_t avro_union_current_branch(avro_datum_t datum)
{
	return avro_datum_to_union(datum)->value;
}

int avro_union_set_discriminant(avro_datum_t datum,
				avro_schema_t schema,
				int discriminant,
				avro_datum_t *branch)
{
	if (!is_avro_union(datum) || !is_avro_union(schema)) {
		return EINVAL;
	}

	struct avro_union_datum_t  *unionp =
	    avro_datum_to_union(datum);

	avro_schema_t  branch_schema =
	    avro_schema_union_branch(schema, discriminant);

	if (branch_schema == NULL) {
		// That branch doesn't exist!
		return EINVAL;
	}

	if (unionp->discriminant != discriminant) {
		// If we're changing the branch, throw away any old
		// branch value.
		if (unionp->value != NULL) {
			avro_datum_decref(unionp->value);
			unionp->value = NULL;
		}

		unionp->discriminant = discriminant;
	}

	// Create a new branch value, if there isn't one already.
	if (unionp->value == NULL) {
		unionp->value = avro_datum_from_schema(branch_schema);
	}

	if (branch != NULL) {
		*branch = unionp->value;
	}

	return 0;
}

avro_datum_t avro_record(const char *name, const char *space)
{
	struct avro_record_datum_t *datum =
	    avro_new(struct avro_record_datum_t);
	if (!datum) {
		return NULL;
	}
	datum->name = avro_strdup(name);
	if (!datum->name) {
		avro_freet(struct avro_record_datum_t, datum);
		return NULL;
	}
	datum->space = space ? avro_strdup(space) : NULL;
	if (space && !datum->space) {
		avro_str_free((char *) datum->name);
		avro_freet(struct avro_record_datum_t, datum);
		return NULL;
	}
	datum->field_order = st_init_numtable_with_size(DEFAULT_TABLE_SIZE);
	if (!datum->field_order) {
		if (space) {
			avro_str_free((char *) datum->space);
		}
		avro_str_free((char *) datum->name);
		avro_freet(struct avro_record_datum_t, datum);
		return NULL;
	}
	datum->fields_byname = st_init_strtable_with_size(DEFAULT_TABLE_SIZE);
	if (!datum->fields_byname) {
		st_free_table(datum->field_order);
		if (space) {
			avro_str_free((char *) datum->space);
		}
		avro_str_free((char *) datum->name);
		avro_freet(struct avro_record_datum_t, datum);
		return NULL;
	}

	avro_datum_init(&datum->obj, AVRO_RECORD);
	return &datum->obj;
}

int
avro_record_get(const avro_datum_t datum, const char *field_name,
		avro_datum_t * field)
{
	union {
		avro_datum_t field;
		st_data_t data;
	} val;
	if (is_avro_datum(datum) && is_avro_record(datum) && field_name) {
		if (st_lookup
		    (avro_datum_to_record(datum)->fields_byname,
		     (st_data_t) field_name, &(val.data))) {
			*field = val.field;
			return 0;
		}
	}
	return EINVAL;
}

int
avro_record_set(avro_datum_t datum, const char *field_name,
		const avro_datum_t field_value)
{
	char *key = (char *)field_name;
	avro_datum_t old_field;

	if (is_avro_datum(datum) && is_avro_record(datum) && field_name) {
		if (avro_record_get(datum, field_name, &old_field) == 0) {
			/* Overriding old value */
			avro_datum_decref(old_field);
		} else {
			/* Inserting new value */
			struct avro_record_datum_t *record =
			    avro_datum_to_record(datum);
			key = avro_strdup(field_name);
			if (!key) {
				return ENOMEM;
			}
			st_insert(record->field_order,
				  record->field_order->num_entries,
				  (st_data_t) key);
		}
		avro_datum_incref(field_value);
		st_insert(avro_datum_to_record(datum)->fields_byname,
			  (st_data_t) key, (st_data_t) field_value);
		return 0;
	}
	return EINVAL;
}

avro_datum_t avro_enum(const char *name, int i)
{
	struct avro_enum_datum_t *datum =
	    avro_new(struct avro_enum_datum_t);
	if (!datum) {
		return NULL;
	}
	datum->name = avro_strdup(name);
	datum->value = i;

	avro_datum_init(&datum->obj, AVRO_ENUM);
	return &datum->obj;
}

int avro_enum_get(const avro_datum_t datum)
{
	return avro_datum_to_enum(datum)->value;
}

const char *avro_enum_get_name(const avro_datum_t datum,
			       const avro_schema_t schema)
{
	int  value = avro_enum_get(datum);
	return avro_schema_enum_get(schema, value);
}

int avro_enum_set(avro_datum_t datum, const int symbol_value)
{
	if (!is_avro_enum(datum)) {
		return EINVAL;
	}

	avro_datum_to_enum(datum)->value = symbol_value;
	return 0;
}

int avro_enum_set_name(avro_datum_t datum, avro_schema_t schema,
		       const char *symbol_name)
{
	if (!is_avro_enum(datum) || !is_avro_enum(schema)) {
		return EINVAL;
	}
	int  symbol_value = avro_schema_enum_get_by_name(schema, symbol_name);
	if (symbol_value == -1) {
		return EINVAL;
	}
	avro_datum_to_enum(datum)->value = symbol_value;
	return 0;
}

static avro_datum_t avro_fixed_private(const char *name, const char *bytes,
				       const int64_t size,
				       avro_free_func_t fixed_free)
{
	struct avro_fixed_datum_t *datum =
	    avro_new(struct avro_fixed_datum_t);
	if (!datum) {
		return NULL;
	}
	datum->name = avro_strdup(name);
	datum->size = size;
	datum->bytes = (char *)bytes;
	datum->free = fixed_free;

	avro_datum_init(&datum->obj, AVRO_FIXED);
	return &datum->obj;
}

avro_datum_t avro_fixed(const char *name, const char *bytes, const int64_t size)
{
	char *bytes_copy = avro_malloc(size);
	if (!bytes_copy) {
		return NULL;
	}
	memcpy(bytes_copy, bytes, size);
	return avro_fixed_private(name, bytes_copy, size, avro_alloc_free);
}

avro_datum_t avro_givefixed(const char *name, const char *bytes,
			    const int64_t size, avro_free_func_t free)
{
	return avro_fixed_private(name, bytes, size, free);
}

static int avro_fixed_set_private(avro_datum_t datum, const char *bytes,
				  const int64_t size,
				  avro_free_func_t fixed_free)
{
	struct avro_fixed_datum_t *fixed;

	AVRO_UNUSED(size);

	if (!(is_avro_datum(datum) && is_avro_fixed(datum))) {
		return EINVAL;
	}

	fixed = avro_datum_to_fixed(datum);
	if (fixed->free) {
		fixed->free(fixed->bytes, fixed->size);
	}

	fixed->free = fixed_free;
	fixed->bytes = (char *)bytes;
	fixed->size = size;
	return 0;
}

int avro_fixed_set(avro_datum_t datum, const char *bytes, const int64_t size)
{
	int rval;
	char *bytes_copy = avro_malloc(size);
	if (!bytes_copy) {
		return ENOMEM;
	}
	memcpy(bytes_copy, bytes, size);
	rval = avro_fixed_set_private(datum, bytes_copy, size, avro_alloc_free);
	if (rval) {
		avro_free(bytes_copy, size);
	}
	return rval;
}

int avro_givefixed_set(avro_datum_t datum, const char *bytes,
		       const int64_t size, avro_free_func_t free)
{
	return avro_fixed_set_private(datum, bytes, size, free);
}

int avro_fixed_get(avro_datum_t datum, char **bytes, int64_t * size)
{
	if (!(is_avro_datum(datum) && is_avro_fixed(datum)) || !bytes || !size) {
		return EINVAL;
	}
	*bytes = avro_datum_to_fixed(datum)->bytes;
	*size = avro_datum_to_fixed(datum)->size;
	return 0;
}

avro_datum_t avro_map(void)
{
	struct avro_map_datum_t *datum =
	    avro_new(struct avro_map_datum_t);
	if (!datum) {
		return NULL;
	}
	datum->map = st_init_strtable_with_size(DEFAULT_TABLE_SIZE);
	if (!datum->map) {
		avro_freet(struct avro_map_datum_t, datum);
		return NULL;
	}
	datum->keys_by_index = st_init_numtable_with_size(DEFAULT_TABLE_SIZE);
	if (!datum->keys_by_index) {
		st_free_table(datum->map);
		avro_freet(struct avro_map_datum_t, datum);
		return NULL;
	}

	avro_datum_init(&datum->obj, AVRO_MAP);
	return &datum->obj;
}

size_t
avro_map_size(const avro_datum_t datum)
{
	const struct avro_map_datum_t  *map = avro_datum_to_map(datum);
	return map->map->num_entries;
}

int
avro_map_get(const avro_datum_t datum, const char *key, avro_datum_t * value)
{
	struct avro_map_datum_t *map;
	union {
		avro_datum_t datum;
		st_data_t data;
	} val;

	if (!(is_avro_datum(datum) && is_avro_map(datum) && key && value)) {
		return EINVAL;
	}

	map = avro_datum_to_map(datum);
	if (st_lookup(map->map, (st_data_t) key, &(val.data))) {
		*value = val.datum;
		return 0;
	}
	return EINVAL;
}

int avro_map_get_key(const avro_datum_t datum, int index,
		     const char **key)
{
	if (!(is_avro_datum(datum) && is_avro_map(datum) &&
	      index >= 0 && key)) {
		return EINVAL;
	}

	union {
		st_data_t data;
		char *key;
	} val;
	struct avro_map_datum_t *map = avro_datum_to_map(datum);

	if (st_lookup(map->keys_by_index, (st_data_t) index, &val.data)) {
		*key = val.key;
		return 0;
	}

	return EINVAL;
}

int
avro_map_set(avro_datum_t datum, const char *key,
	     const avro_datum_t value)
{
	char *save_key = (char *)key;
	avro_datum_t old_datum;

	if (!is_avro_datum(datum) || !is_avro_map(datum) || !key
	    || !is_avro_datum(value)) {
		return EINVAL;
	}

	struct avro_map_datum_t  *map = avro_datum_to_map(datum);

	if (avro_map_get(datum, key, &old_datum) == 0) {
		/* Overwriting an old value */
		avro_datum_decref(old_datum);
	} else {
		/* Inserting a new value */
		save_key = avro_strdup(key);
		if (!save_key) {
			return ENOMEM;
		}
		int  new_index = map->map->num_entries;
		st_insert(map->keys_by_index, (st_data_t) new_index,
			  (st_data_t) save_key);
	}
	avro_datum_incref(value);
	st_insert(map->map, (st_data_t) save_key, (st_data_t) value);
	return 0;
}

avro_datum_t avro_array(void)
{
	struct avro_array_datum_t *datum =
	    avro_new(struct avro_array_datum_t);
	if (!datum) {
		return NULL;
	}
	datum->els = st_init_numtable_with_size(DEFAULT_TABLE_SIZE);
	if (!datum->els) {
		avro_freet(struct avro_array_datum_t, datum);
		return NULL;
	}

	avro_datum_init(&datum->obj, AVRO_ARRAY);
	return &datum->obj;
}

int
avro_array_get(const avro_datum_t array_datum, int64_t index, avro_datum_t * value)
{
    union {
        st_data_t data;
        avro_datum_t datum;
    } val;
	if (is_avro_datum(array_datum) && is_avro_array(array_datum)) {
        const struct avro_array_datum_t * array = avro_datum_to_array(array_datum);
        if (st_lookup(array->els, index, &val.data)) {
            *value = val.datum;
            return 0;
        }
    }
    return EINVAL;
}

size_t
avro_array_size(const avro_datum_t datum)
{
	const struct avro_array_datum_t  *array = avro_datum_to_array(datum);
	return array->els->num_entries;
}

int
avro_array_append_datum(avro_datum_t array_datum,
			const avro_datum_t datum)
{
	struct avro_array_datum_t *array;
	if (!is_avro_datum(array_datum) || !is_avro_array(array_datum)
	    || !is_avro_datum(datum)) {
		return EINVAL;
	}
	array = avro_datum_to_array(array_datum);
	st_insert(array->els, array->els->num_entries,
		  (st_data_t) avro_datum_incref(datum));
	return 0;
}

static int char_datum_free_foreach(char *key, avro_datum_t datum, void *arg)
{
	AVRO_UNUSED(arg);

	avro_datum_decref(datum);
	avro_str_free(key);
	return ST_DELETE;
}

static int array_free_foreach(int i, avro_datum_t datum, void *arg)
{
	AVRO_UNUSED(i);
	AVRO_UNUSED(arg);

	avro_datum_decref(datum);
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
					string->free(string->s, string->size);
				}
				avro_freet(struct avro_string_datum_t, string);
			}
			break;
		case AVRO_BYTES:{
				struct avro_bytes_datum_t *bytes;
				bytes = avro_datum_to_bytes(datum);
				if (bytes->free) {
					bytes->free(bytes->bytes, bytes->size);
				}
				avro_freet(struct avro_bytes_datum_t, bytes);
			}
			break;
		case AVRO_INT32:{
				avro_freet(struct avro_int32_datum_t, datum);
			}
			break;
		case AVRO_INT64:{
				avro_freet(struct avro_int64_datum_t, datum);
			}
			break;
		case AVRO_FLOAT:{
				avro_freet(struct avro_float_datum_t, datum);
			}
			break;
		case AVRO_DOUBLE:{
				avro_freet(struct avro_double_datum_t, datum);
			}
			break;
		case AVRO_BOOLEAN:{
				avro_freet(struct avro_boolean_datum_t, datum);
			}
			break;
		case AVRO_NULL:
			/* Nothing allocated */
			break;

		case AVRO_RECORD:{
				struct avro_record_datum_t *record;
				record = avro_datum_to_record(datum);
				avro_str_free((char *) record->name);
				if (record->space) {
					avro_str_free((char *) record->space);
				}
				st_foreach(record->fields_byname,
					   char_datum_free_foreach, 0);
				st_free_table(record->field_order);
				st_free_table(record->fields_byname);
				avro_freet(struct avro_record_datum_t, record);
			}
			break;
		case AVRO_ENUM:{
				struct avro_enum_datum_t *enump;
				enump = avro_datum_to_enum(datum);
				avro_str_free((char *) enump->name);
				avro_freet(struct avro_enum_datum_t, enump);
			}
			break;
		case AVRO_FIXED:{
				struct avro_fixed_datum_t *fixed;
				fixed = avro_datum_to_fixed(datum);
				avro_str_free((char *) fixed->name);
				if (fixed->free) {
					fixed->free((void *)fixed->bytes,
						    fixed->size);
				}
				avro_freet(struct avro_fixed_datum_t, fixed);
			}
			break;
		case AVRO_MAP:{
				struct avro_map_datum_t *map;
				map = avro_datum_to_map(datum);
				st_foreach(map->map, char_datum_free_foreach,
					   0);
				st_free_table(map->map);
				st_free_table(map->keys_by_index);
				avro_freet(struct avro_map_datum_t, map);
			}
			break;
		case AVRO_ARRAY:{
				struct avro_array_datum_t *array;
				array = avro_datum_to_array(datum);
				st_foreach(array->els, array_free_foreach, 0);
				st_free_table(array->els);
				avro_freet(struct avro_array_datum_t, array);
			}
			break;
		case AVRO_UNION:{
				struct avro_union_datum_t *unionp;
				unionp = avro_datum_to_union(datum);
				avro_datum_decref(unionp->value);
				avro_freet(struct avro_union_datum_t, unionp);
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
	AVRO_UNUSED(value);
	AVRO_UNUSED(fp);
}
