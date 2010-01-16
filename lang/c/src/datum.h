/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

#ifndef AVRO_DATUM_H
#define AVRO_DATUM_H
#include "avro.h"		/* for avro_schema_t */
#include "container_of.h"

struct avro_string_datum_t
{
  struct avro_obj_t obj;
  char *s;
};

struct avro_bytes_datum_t
{
  struct avro_obj_t obj;
  char *buf;
  size_t len;
};

struct avro_int_datum_t
{
  struct avro_obj_t obj;
  int32_t i;
};

struct avro_long_datum_t
{
  struct avro_obj_t obj;
  int64_t l;
};

struct avro_float_datum_t
{
  struct avro_obj_t obj;
  float f;
};

struct avro_double_datum_t
{
  struct avro_obj_t obj;
  double d;
};

struct avro_boolean_datum_t
{
  struct avro_obj_t obj;
  int8_t i;
};

struct avro_fixed_datum_t
{
  struct avro_obj_t obj;
  struct schema_fixed_t *schema;
  char *bytes;
};

#define avro_datum_to_string(datum_)    container_of(datum_, struct avro_string_datum_t, obj)
#define avro_datum_to_bytes(datum_)     container_of(datum_, struct avro_bytes_datum_t, obj)
#define avro_datum_to_int(datum_)       container_of(datum_, struct avro_int_datum_t, obj)
#define avro_datum_to_long(datum_)      container_of(datum_, struct avro_long_datum_t, obj)
#define avro_datum_to_float(datum_)     container_of(datum_, struct avro_float_datum_t, obj)
#define avro_datum_to_double(datum_)    container_of(datum_, struct avro_double_datum_t, obj)
#define avro_datum_to_boolean(datum_)   container_of(datum_, struct avro_boolean_datum_t, obj)
#define avro_datum_to_fixed(datum_)     container_of(datum_, struct avro_fixed_datum_t, obj)

#endif
