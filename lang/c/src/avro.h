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
#ifndef AVRO_H
#define AVRO_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <stdio.h>
#include <stdint.h>

/*
 * Allocation interface.  You can provide a custom allocator for the
 * library, should you wish.  The allocator is provided as a single
 * generic function, which can emulate the standard malloc, realloc, and
 * free functions.  The design of this allocation interface is inspired
 * by the implementation of the Lua interpreter.
 *
 * The ptr parameter will be the location of any existing memory
 * buffer.  The osize parameter will be the size of this existing
 * buffer.  If ptr is NULL, then osize will be 0.  The nsize parameter
 * will be the size of the new buffer, or 0 if the new buffer should be
 * freed.
 *
 * If nsize is 0, then the allocation function must return NULL.  If
 * nsize is not 0, then it should return NULL if the allocation fails.
 */

typedef void *
(*avro_allocator_t)(void *user_data, void *ptr, size_t osize, size_t nsize);

void avro_set_allocator(avro_allocator_t alloc, void *user_data);

/*
 * Returns a textual description of the last error condition returned by
 * an Avro function.
 */

const char *avro_strerror(void);


enum avro_type_t {
	AVRO_STRING,
	AVRO_BYTES,
	AVRO_INT32,
	AVRO_INT64,
	AVRO_FLOAT,
	AVRO_DOUBLE,
	AVRO_BOOLEAN,
	AVRO_NULL,
	AVRO_RECORD,
	AVRO_ENUM,
	AVRO_FIXED,
	AVRO_MAP,
	AVRO_ARRAY,
	AVRO_UNION,
	AVRO_LINK
};
typedef enum avro_type_t avro_type_t;

enum avro_class_t {
	AVRO_SCHEMA,
	AVRO_DATUM
};
typedef enum avro_class_t avro_class_t;

struct avro_obj_t {
	avro_type_t type;
	avro_class_t class_type;
	unsigned long refcount;
};

#define avro_classof(obj)     ((obj)->class_type)
#define is_avro_schema(obj)   (obj && avro_classof(obj) == AVRO_SCHEMA)
#define is_avro_datum(obj)    (obj && avro_classof(obj) == AVRO_DATUM)

#define avro_typeof(obj)      ((obj)->type)
#define is_avro_string(obj)   (obj && avro_typeof(obj) == AVRO_STRING)
#define is_avro_bytes(obj)    (obj && avro_typeof(obj) == AVRO_BYTES)
#define is_avro_int32(obj)    (obj && avro_typeof(obj) == AVRO_INT32)
#define is_avro_int64(obj)    (obj && avro_typeof(obj) == AVRO_INT64)
#define is_avro_float(obj)    (obj && avro_typeof(obj) == AVRO_FLOAT)
#define is_avro_double(obj)   (obj && avro_typeof(obj) == AVRO_DOUBLE)
#define is_avro_boolean(obj)  (obj && avro_typeof(obj) == AVRO_BOOLEAN)
#define is_avro_null(obj)     (obj && avro_typeof(obj) == AVRO_NULL)
#define is_avro_primitive(obj)(is_avro_string(obj) \
                             ||is_avro_bytes(obj) \
                             ||is_avro_int32(obj) \
                             ||is_avro_int64(obj) \
                             ||is_avro_float(obj) \
                             ||is_avro_double(obj) \
                             ||is_avro_boolean(obj) \
                             ||is_avro_null(obj))
#define is_avro_record(obj)   (obj && avro_typeof(obj) == AVRO_RECORD)
#define is_avro_enum(obj)     (obj && avro_typeof(obj) == AVRO_ENUM)
#define is_avro_fixed(obj)    (obj && avro_typeof(obj) == AVRO_FIXED)
#define is_avro_named_type(obj)(is_avro_record(obj) \
                              ||is_avro_enum(obj) \
                              ||is_avro_fixed(obj))
#define is_avro_map(obj)      (obj && avro_typeof(obj) == AVRO_MAP)
#define is_avro_array(obj)    (obj && avro_typeof(obj) == AVRO_ARRAY)
#define is_avro_union(obj)    (obj && avro_typeof(obj) == AVRO_UNION)
#define is_avro_complex_type(obj) (!(is_avro_primitive(obj))
#define is_avro_link(obj)     (obj && avro_typeof(obj) == AVRO_LINK)

typedef struct avro_reader_t_ *avro_reader_t;
typedef struct avro_writer_t_ *avro_writer_t;

/*
 * schema 
 */
typedef struct avro_obj_t *avro_schema_t;

avro_schema_t avro_schema_string(void);
avro_schema_t avro_schema_bytes(void);
avro_schema_t avro_schema_int(void);
avro_schema_t avro_schema_long(void);
avro_schema_t avro_schema_float(void);
avro_schema_t avro_schema_double(void);
avro_schema_t avro_schema_boolean(void);
avro_schema_t avro_schema_null(void);

avro_schema_t avro_schema_record(const char *name, const char *space);
avro_schema_t avro_schema_record_field_get(const avro_schema_t
					   record, const char *field_name);
const char *avro_schema_record_field_name(const avro_schema_t schema, int index);
int avro_schema_record_field_get_index(const avro_schema_t schema,
				       const char *field_name);
avro_schema_t avro_schema_record_field_get_by_index
(const avro_schema_t record, int index);
int avro_schema_record_field_append(const avro_schema_t record,
				    const char *field_name,
				    const avro_schema_t type);
size_t avro_schema_record_size(const avro_schema_t record);

avro_schema_t avro_schema_enum(const char *name);
const char *avro_schema_enum_get(const avro_schema_t enump,
				 int index);
int avro_schema_enum_get_by_name(const avro_schema_t enump,
				 const char *symbol_name);
int avro_schema_enum_symbol_append(const avro_schema_t
				   enump, const char *symbol);

avro_schema_t avro_schema_fixed(const char *name, const int64_t len);
int64_t avro_schema_fixed_size(const avro_schema_t fixed);

avro_schema_t avro_schema_map(const avro_schema_t values);
avro_schema_t avro_schema_map_values(avro_schema_t map);

avro_schema_t avro_schema_array(const avro_schema_t items);
avro_schema_t avro_schema_array_items(avro_schema_t array);

avro_schema_t avro_schema_union(void);
size_t avro_schema_union_size(const avro_schema_t union_schema);
int avro_schema_union_append(const avro_schema_t
			     union_schema, const avro_schema_t schema);
avro_schema_t avro_schema_union_branch(avro_schema_t union_schema,
				       int branch_index);
avro_schema_t avro_schema_union_branch_by_name
(avro_schema_t union_schema, int *branch_index, const char *name);

avro_schema_t avro_schema_link(avro_schema_t schema);

typedef struct avro_schema_error_t_ *avro_schema_error_t;
int avro_schema_from_json(const char *jsontext,
			  const int32_t len,
			  avro_schema_t * schema, avro_schema_error_t * error);
int avro_schema_to_json(const avro_schema_t schema, avro_writer_t out);

int avro_schema_to_specific(avro_schema_t schema, const char *prefix);

avro_schema_t avro_schema_get_subschema(const avro_schema_t schema,
         const char *name);
const char *avro_schema_name(const avro_schema_t schema);
const char *avro_schema_type_name(const avro_schema_t schema);
avro_schema_t avro_schema_copy(avro_schema_t schema);
int avro_schema_equal(avro_schema_t a, avro_schema_t b);

avro_schema_t avro_schema_incref(avro_schema_t schema);
void avro_schema_decref(avro_schema_t schema);

/*
 * io 
 */
avro_reader_t avro_reader_file(FILE * fp);
avro_writer_t avro_writer_file(FILE * fp);
avro_reader_t avro_reader_memory(const char *buf, int64_t len);
avro_writer_t avro_writer_memory(const char *buf, int64_t len);

int avro_read(avro_reader_t reader, void *buf, int64_t len);
int avro_skip(avro_reader_t reader, int64_t len);
int avro_write(avro_writer_t writer, void *buf, int64_t len);

void avro_writer_reset(avro_writer_t writer);
int64_t avro_writer_tell(avro_writer_t writer);
void avro_writer_flush(avro_writer_t writer);

void avro_writer_dump(avro_writer_t writer, FILE * fp);
void avro_reader_dump(avro_reader_t reader, FILE * fp);

void avro_reader_free(avro_reader_t reader);
void avro_writer_free(avro_writer_t writer);

/*
 * datum 
 */

/**
 * A function used to free a bytes, string, or fixed buffer once it is
 * no longer needed by the datum that wraps it.
 */

typedef void
(*avro_free_func_t)(void *ptr, size_t sz);

/**
 * An avro_free_func_t that frees the buffer using the custom allocator
 * provided to avro_set_allocator.
 */

void
avro_alloc_free(void *ptr, size_t sz);

/*
 * Datum constructors.  Each datum stores a reference to the schema that
 * the datum is an instance of.  The primitive datum constructors don't
 * need to take in an explicit avro_schema_t parameter, since there's
 * only one schema that they could be an instance of.  The complex
 * constructors do need an explicit schema parameter.
 */

typedef struct avro_obj_t *avro_datum_t;
avro_datum_t avro_string(const char *str);
avro_datum_t avro_givestring(const char *str,
			     avro_free_func_t free);
avro_datum_t avro_bytes(const char *buf, int64_t len);
avro_datum_t avro_givebytes(const char *buf, int64_t len,
			    avro_free_func_t free);
avro_datum_t avro_int32(int32_t i);
avro_datum_t avro_int64(int64_t l);
avro_datum_t avro_float(float f);
avro_datum_t avro_double(double d);
avro_datum_t avro_boolean(int8_t i);
avro_datum_t avro_null(void);
avro_datum_t avro_record(avro_schema_t schema);
avro_datum_t avro_enum(avro_schema_t schema, int i);
avro_datum_t avro_fixed(avro_schema_t schema,
			const char *bytes, const int64_t size);
avro_datum_t avro_givefixed(avro_schema_t schema,
			    const char *bytes, const int64_t size,
			    avro_free_func_t free);
avro_datum_t avro_map(avro_schema_t schema);
avro_datum_t avro_array(avro_schema_t schema);
avro_datum_t avro_union(avro_schema_t schema,
			int64_t discriminant, const avro_datum_t datum);

/**
 * Returns the schema that the datum is an instance of.
 */

avro_schema_t avro_datum_get_schema(const avro_datum_t datum);

/*
 * Constructs a new avro_datum_t instance that's appropriate for holding
 * values of the given schema.
 */

avro_datum_t avro_datum_from_schema(const avro_schema_t schema);

/* getters */
int avro_string_get(avro_datum_t datum, char **p);
int avro_bytes_get(avro_datum_t datum, char **bytes, int64_t * size);
int avro_int32_get(avro_datum_t datum, int32_t * i);
int avro_int64_get(avro_datum_t datum, int64_t * l);
int avro_float_get(avro_datum_t datum, float *f);
int avro_double_get(avro_datum_t datum, double *d);
int avro_boolean_get(avro_datum_t datum, int8_t * i);

int avro_enum_get(const avro_datum_t datum);
const char *avro_enum_get_name(const avro_datum_t datum);
int avro_fixed_get(avro_datum_t datum, char **bytes, int64_t * size);
int avro_record_get(const avro_datum_t record, const char *field_name,
		    avro_datum_t * value);

/*
 * A helper macro that extracts the value of the given field of a
 * record.
 */

#define avro_record_get_field_value(rc, rec, typ, fname, ...)	\
	do {							\
		avro_datum_t  field = NULL;			\
		(rc) = avro_record_get((rec), (fname), &field);	\
		if (rc) break;					\
		(rc) = avro_##typ##_get(field, __VA_ARGS__);	\
	} while (0)


int avro_map_get(const avro_datum_t datum, const char *key,
		 avro_datum_t * value);
/*
 * For maps, the "index" for each entry is based on the order that they
 * were added to the map.
 */
int avro_map_get_key(const avro_datum_t datum, int index,
		     const char **key);
size_t avro_map_size(const avro_datum_t datum);
int avro_array_get(const avro_datum_t datum, int64_t index, avro_datum_t * value);
size_t avro_array_size(const avro_datum_t datum);

/*
 * These accessors allow you to query the current branch of a union
 * value, returning either the branch's discriminant value or the
 * avro_datum_t of the branch.  A union value can be uninitialized, in
 * which case the discriminant will be -1 and the datum NULL.
 */

int64_t avro_union_discriminant(const avro_datum_t datum);
avro_datum_t avro_union_current_branch(avro_datum_t datum);

/* setters */
int avro_string_set(avro_datum_t datum, const char *p);
int avro_givestring_set(avro_datum_t datum, const char *p,
			avro_free_func_t free);

int avro_bytes_set(avro_datum_t datum, const char *bytes, const int64_t size);
int avro_givebytes_set(avro_datum_t datum, const char *bytes,
		       const int64_t size,
		       avro_free_func_t free);

int avro_int32_set(avro_datum_t datum, const int32_t i);
int avro_int64_set(avro_datum_t datum, const int64_t l);
int avro_float_set(avro_datum_t datum, const float f);
int avro_double_set(avro_datum_t datum, const double d);
int avro_boolean_set(avro_datum_t datum, const int8_t i);

int avro_enum_set(avro_datum_t datum, const int symbol_value);
int avro_enum_set_name(avro_datum_t datum, const char *symbol_name);
int avro_fixed_set(avro_datum_t datum, const char *bytes, const int64_t size);
int avro_givefixed_set(avro_datum_t datum, const char *bytes,
		       const int64_t size,
		       avro_free_func_t free);

int avro_record_set(avro_datum_t record, const char *field_name,
		    avro_datum_t value);

/*
 * A helper macro that sets the value of the given field of a record.
 */

#define avro_record_set_field_value(rc, rec, typ, fname, ...)	\
	do {							\
		avro_datum_t  field = NULL;			\
		(rc) = avro_record_get((rec), (fname), &field);	\
		if (rc) break;					\
		(rc) = avro_##typ##_set(field, __VA_ARGS__);	\
	} while (0)

int avro_map_set(avro_datum_t map, const char *key,
		 avro_datum_t value);
int avro_array_append_datum(avro_datum_t array_datum,
			    avro_datum_t datum);

/*
 * This function selects the active branch of a union value, and can be
 * safely called on an existing union to change the current branch.  If
 * the branch changes, we'll automatically construct a new avro_datum_t
 * for the new branch's schema type.  If the desired branch is already
 * the active branch of the union, we'll leave the existing datum
 * instance as-is.  The branch datum will be placed into the "branch"
 * parameter, regardless of whether we have to create a new datum
 * instance or not.
 */

int avro_union_set_discriminant(avro_datum_t unionp,
				int discriminant,
				avro_datum_t *branch);

/* reference counting */
avro_datum_t avro_datum_incref(avro_datum_t value);
void avro_datum_decref(avro_datum_t value);

void avro_datum_print(avro_datum_t value, FILE * fp);

int avro_datum_equal(avro_datum_t a, avro_datum_t b);

/*
 * Returns a string containing the JSON encoding of an Avro value.  You
 * must free this string when you're done with it, using the standard
 * free() function.  (*Not* using the custom Avro allocator.)
 */

int avro_datum_to_json(const avro_datum_t datum,
		       int one_line, char **json_str);

int avro_schema_match(avro_schema_t writers_schema,
		      avro_schema_t readers_schema);

int avro_schema_datum_validate(avro_schema_t
			       expected_schema, avro_datum_t datum);

int avro_read_data(avro_reader_t reader,
		   avro_schema_t writer_schema,
		   avro_schema_t reader_schema, avro_datum_t * datum);
int avro_skip_data(avro_reader_t reader, avro_schema_t writer_schema);
int avro_write_data(avro_writer_t writer,
		    avro_schema_t writer_schema, avro_datum_t datum);
int64_t avro_size_data(avro_writer_t writer,
		       avro_schema_t writer_schema, avro_datum_t datum);

/* File object container */
typedef struct avro_file_reader_t_ *avro_file_reader_t;
typedef struct avro_file_writer_t_ *avro_file_writer_t;

int avro_file_writer_create(const char *path, avro_schema_t schema,
			    avro_file_writer_t * writer);
int avro_file_writer_open(const char *path, avro_file_writer_t * writer);
int avro_file_reader(const char *path, avro_file_reader_t * reader);

int avro_file_writer_append(avro_file_writer_t writer, avro_datum_t datum);
int avro_file_writer_sync(avro_file_writer_t writer);
int avro_file_writer_flush(avro_file_writer_t writer);
int avro_file_writer_close(avro_file_writer_t writer);

int avro_file_reader_read(avro_file_reader_t reader,
			  avro_schema_t readers_schema, avro_datum_t * datum);
int avro_file_reader_close(avro_file_reader_t reader);

CLOSE_EXTERN
#endif
