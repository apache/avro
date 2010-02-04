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
#define is_avro_union(obj)    (obj && avro_classof(obj) == AVRO_SCHEMA && avro_typeof(obj) == AVRO_UNION)
#define is_avro_complex_type(obj) (!(is_avro_primitive(obj))
#define is_avro_link(obj)     (obj && avro_typeof(obj) == AVRO_LINK)

typedef struct avro_reader_t *avro_reader_t;
typedef struct avro_writer_t *avro_writer_t;

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

avro_schema_t avro_schema_record(const char *name);
avro_schema_t avro_schema_record_field_get(const avro_schema_t
					   record, const char *field_name);
int avro_schema_record_field_append(const avro_schema_t record,
				    const char *field_name,
				    const avro_schema_t type);

avro_schema_t avro_schema_enum(const char *name);
int avro_schema_enum_symbol_append(const avro_schema_t
				   enump, const char *symbol);

avro_schema_t avro_schema_fixed(const char *name, const int64_t len);
avro_schema_t avro_schema_map(const avro_schema_t values);
avro_schema_t avro_schema_array(const avro_schema_t items);

avro_schema_t avro_schema_union(void);
int avro_schema_union_append(const avro_schema_t
			     union_schema, const avro_schema_t schema);

avro_schema_t avro_schema_link(avro_schema_t schema);

typedef struct avro_schema_error_t *avro_schema_error_t;
int avro_schema_from_json(const char *jsontext,
			  const int32_t len,
			  avro_schema_t * schema, avro_schema_error_t * error);
int avro_schema_to_json(avro_schema_t schema, avro_writer_t out);

int avro_schema_to_specific(avro_schema_t schema, const char *prefix);

const char *avro_schema_name(const avro_schema_t schema);
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
int avro_flush(avro_writer_t writer);

void avro_writer_dump(avro_writer_t writer, FILE * fp);
void avro_reader_dump(avro_reader_t reader, FILE * fp);

void avro_reader_free(avro_reader_t reader);
void avro_writer_free(avro_writer_t writer);

/*
 * datum 
 */

/* constructors */
typedef struct avro_obj_t *avro_datum_t;
avro_datum_t avro_string(const char *str);
avro_datum_t avro_wrapstring(const char *str);
avro_datum_t avro_givestring(const char *str);
avro_datum_t avro_bytes(const char *buf, int64_t len);
avro_datum_t avro_wrapbytes(const char *buf, int64_t len);
avro_datum_t avro_givebytes(const char *buf, int64_t len);
avro_datum_t avro_int32(int32_t i);
avro_datum_t avro_int64(int64_t l);
avro_datum_t avro_float(float f);
avro_datum_t avro_double(double d);
avro_datum_t avro_boolean(int8_t i);
avro_datum_t avro_null(void);
avro_datum_t avro_record(const char *name);
avro_datum_t avro_enum(const char *name, const char *symbol);
avro_datum_t avro_fixed(const char *name, const char *bytes,
			const int64_t size);
avro_datum_t avro_wrapfixed(const char *name, const char *bytes,
			    const int64_t size);
avro_datum_t avro_givefixed(const char *name, const char *bytes,
			    const int64_t size);
avro_datum_t avro_map(void);
avro_datum_t avro_array(void);
avro_datum_t avro_union(const avro_schema_t schema, const avro_datum_t datum);

/* getters */
int avro_string_get(avro_datum_t datum, char **p);
int avro_bytes_get(avro_datum_t datum, char **bytes, int64_t * size);
int avro_int32_get(avro_datum_t datum, int32_t * i);
int avro_int64_get(avro_datum_t datum, int64_t * l);
int avro_float_get(avro_datum_t datum, float *f);
int avro_double_get(avro_datum_t datum, double *d);
int avro_boolean_get(avro_datum_t datum, int8_t * i);

int avro_fixed_get(avro_datum_t datum, char **bytes, int64_t * size);
int avro_record_get(const avro_datum_t record, const char *field_name,
		    avro_datum_t * value);
int avro_map_get(const avro_datum_t datum, const char *key,
		 avro_datum_t * value);

/* setters */
int avro_string_set(avro_datum_t datum, const char *p);
int avro_givestring_set(avro_datum_t datum, const char *p);
int avro_wrapstring_set(avro_datum_t datum, const char *p);

int avro_bytes_set(avro_datum_t datum, const char *bytes, const int64_t size);
int avro_givebytes_set(avro_datum_t datum, const char *bytes,
		       const int64_t size);
int avro_wrapbytes_set(avro_datum_t datum, const char *bytes,
		       const int64_t size);

int avro_int32_set(avro_datum_t datum, const int32_t i);
int avro_int64_set(avro_datum_t datum, const int64_t l);
int avro_float_set(avro_datum_t datum, const float f);
int avro_double_set(avro_datum_t datum, const double d);
int avro_boolean_set(avro_datum_t datum, const int8_t i);

int avro_fixed_set(avro_datum_t datum, const char *bytes, const int64_t size);
int avro_givefixed_set(avro_datum_t datum, const char *bytes,
		       const int64_t size);
int avro_wrapfixed_set(avro_datum_t datum, const char *bytes,
		       const int64_t size);

int avro_record_set(const avro_datum_t record, const char *field_name,
		    const avro_datum_t value);
int avro_map_set(const avro_datum_t map, const char *key,
		 const avro_datum_t value);
int avro_array_append_datum(const avro_datum_t array_datum,
			    const avro_datum_t datum);

/* reference counting */
avro_datum_t avro_datum_incref(avro_datum_t value);
void avro_datum_decref(avro_datum_t value);

void avro_datum_print(avro_datum_t value, FILE * fp);

int avro_datum_equal(avro_datum_t a, avro_datum_t b);

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

CLOSE_EXTERN
#endif
