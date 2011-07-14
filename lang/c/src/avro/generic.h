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

#ifndef AVRO_GENERIC_H
#define AVRO_GENERIC_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include <avro/schema.h>
#include <avro/value.h>

/*
 * This file contains an avro_value_t implementation that can store
 * values of any Avro schema.  It replaces the old avro_datum_t class.
 */


/*
 * Return a generic avro_value_iface_t implementation for the given
 * schema, regardless of what type it is.
 */

avro_value_iface_t *avro_generic_class_from_schema(avro_schema_t schema);


/*
 * These functions return an avro_value_iface_t implementation for each
 * scalar schema type.
 */

avro_value_iface_t *avro_generic_boolean_class(void);
avro_value_iface_t *avro_generic_bytes_class(void);
avro_value_iface_t *avro_generic_double_class(void);
avro_value_iface_t *avro_generic_float_class(void);
avro_value_iface_t *avro_generic_int_class(void);
avro_value_iface_t *avro_generic_long_class(void);
avro_value_iface_t *avro_generic_null_class(void);
avro_value_iface_t *avro_generic_string_class(void);

avro_value_iface_t *avro_generic_enum_class(avro_schema_t schema);
avro_value_iface_t *avro_generic_fixed_class(avro_schema_t schema);

/*
 * These functions instantiate a new generic scalar value.
 */

int avro_generic_boolean_new(avro_value_t *value, bool val);
int avro_generic_bytes_new(avro_value_t *value, void *buf, size_t size);
int avro_generic_double_new(avro_value_t *value, double val);
int avro_generic_float_new(avro_value_t *value, float val);
int avro_generic_int_new(avro_value_t *value, int32_t val);
int avro_generic_long_new(avro_value_t *value, int64_t val);
int avro_generic_null_new(avro_value_t *value);
int avro_generic_string_new(avro_value_t *value, char *val);
int avro_generic_string_new_length(avro_value_t *value, char *val, size_t size);


/*
 * These functions return an avro_value_iface_t implementation for each
 * compound schema type.
 */

/*
 * The generic classes for compound schemas work with any avro_value_t
 * implementation for their subschemas.  They'll use the given function
 * pointer to instantiate a value implementation for each child schema.
 */

typedef avro_value_iface_t *
(*avro_value_iface_creator_t)(avro_schema_t schema, void *user_data);

avro_value_iface_t *
avro_generic_array_class(avro_schema_t schema,
			 avro_value_iface_creator_t creator,
			 void *user_data);

avro_value_iface_t *
avro_generic_map_class(avro_schema_t schema,
		       avro_value_iface_creator_t creator,
		       void *user_data);

avro_value_iface_t *
avro_generic_record_class(avro_schema_t schema,
			  avro_value_iface_creator_t creator,
			  void *user_data);

avro_value_iface_t *
avro_generic_union_class(avro_schema_t schema,
			 avro_value_iface_creator_t creator,
			 void *user_data);


CLOSE_EXTERN
#endif
