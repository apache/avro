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
#ifndef AVRO_PRIVATE_H
#define AVRO_PRIVATE_H

/* Private internal structures. This is not part of the public API so never program to it. */
#include <stdio.h>
#include <stdarg.h>
#include "avro.h"
#include "json.h"
#include "apr_hash.h"
#include "apr_tables.h"
#include "apr_pools.h"
#include "apr_file_io.h"
#include "apr_network_io.h"

/* Util function */
char *avro_util_file_read_full (apr_pool_t * pool, const char *fname,
				apr_size_t * len);

/* string */
typedef wchar_t *avro_string_t;
/* int */
typedef int32_t avro_int_t;
/* long */
typedef int64_t avro_long_t;

/* functions for reading and writing bytes */
struct avro_io
{
  avro_status_t (*read) (struct avro_io * io, char *addr, uint64_t len);
  avro_status_t (*skip) (struct avro_io * io, uint64_t len);
  avro_status_t (*write) (struct avro_io * io, char *addr, uint64_t len);
};
typedef struct avro_io avro_io;

/* list of avro formats */
enum avro_format
{
  AVRO_BINARY_FORMAT,
  AVRO_JSON_IMPORT_EXPORT_FORMAT,
  /* NOTE: the following must always be last */
  AVRO_NUM_DATA_FORMATS
};
typedef enum avro_format avro_format;

/* generic communication channel for sending/receiving avro data */
struct avro_channel
{
  avro_format format;
  avro_io *io;
};
typedef struct avro_channel avro_channel;

/* information that all values have */
struct avro_value
{
  avro_type_t type;
  apr_pool_t *pool;
  const JSON_value *schema;
  struct avro_value *parent;
};
typedef struct avro_value avro_value;

struct avro_value_methods
{
  avro_status_t (*read_data) (struct avro_value * value,
			      struct avro_channel * channel);
  avro_status_t (*skip_data) (struct avro_value * value,
			      struct avro_channel * channel);
  avro_status_t (*write_data) (struct avro_value * value,
			       struct avro_channel * channel);
};

/* Globals used during schema creation */
struct avro_value_ctx
{
  apr_hash_t *named_objects;
};

struct avro_value_info
{
  avro_string_t name;
  avro_type_t type;
  int private;
  struct avro_value *(*create) (struct avro_value_ctx * ctx,
				struct avro_value * parent, apr_pool_t * pool,
				const JSON_value * json);
  void (*print_info) (struct avro_value * value, FILE * fp);
  struct avro_value_methods formats[AVRO_NUM_DATA_FORMATS];
};

extern const struct avro_value_info *avro_value_registry[];

avro_status_t avro_value_read_data (struct avro_value *value,
				    struct avro_channel *channel);
avro_status_t avro_value_skip_data (struct avro_value *value,
				    struct avro_channel *channel);
avro_status_t avro_value_write_data (struct avro_value *value,
				     struct avro_channel *channel);
void avro_value_print_info (struct avro_value *value, FILE * fp);


/* Create a new avro value from json */
struct avro_value *avro_value_create (apr_pool_t * pool, char *jsontext,
				      apr_size_t textlen);
struct avro_value *avro_value_from_json (struct avro_value_ctx *ctx,
					 struct avro_value *parent,
					 const JSON_value * json);

/* Helper utility for reading an attribute from a JSON object */
const JSON_value *json_attr_get (const JSON_value * obj, const wchar_t * key);
/* Helper utility for reading an attribute from JSON w/type checking */
const JSON_value *json_attr_get_check_type (const JSON_value * obj,
					    const wchar_t * key,
					    JSON_type type);
/* Helper utility for printing the indent */
void avro_value_indent (struct avro_value *value, FILE * fp);

struct avro_io *avro_io_file_create (apr_file_t * file);
struct avro_io *avro_io_socket_create (apr_socket_t * socket);
struct avro_io *avro_io_memory_create (apr_pool_t * pool, char *addr,
				       int64_t len);

struct avro_channel *avro_file_container_create_from_file (apr_file_t * file);
struct avro_channel *avro_file_container_create (apr_pool_t * pool,
						 const char *fname,
						 apr_int32_t flag,
						 apr_fileperms_t perm);

/* Endian helpers */
avro_status_t avro_getint32_le (struct avro_io *io, int32_t * value);
avro_status_t avro_putint32_le (struct avro_io *io, const int32_t value);
avro_status_t avro_getint32_be (struct avro_io *io, int32_t * value);
avro_status_t avro_putint32_be (struct avro_io *io, const int32_t value);
avro_status_t avro_getint64_le (struct avro_io *io, int64_t * value);
avro_status_t avro_putint64_le (struct avro_io *io, const int64_t value);
avro_status_t avro_getint64_be (struct avro_io *io, int64_t * value);
avro_status_t avro_putint64_be (struct avro_io *io, const int64_t value);

/* Primitive IO Functions */
avro_status_t avro_putstring (struct avro_io *io, apr_pool_t * pool,
			      avro_string_t string);
avro_status_t avro_getstring (struct avro_io *io, apr_pool_t * pool,
			      avro_string_t * string);
avro_status_t avro_putbytes (struct avro_io *io, char *data, int64_t len);
avro_status_t avro_getbytes (struct avro_io *io, apr_pool_t * pool,
			     char **data, int64_t * len);
avro_status_t avro_putbool (struct avro_io *io, int boolean);
avro_status_t avro_getbool (struct avro_io *io, int *boolean);
avro_status_t avro_putint (struct avro_io *io, avro_int_t * ip);
avro_status_t avro_putlong (struct avro_io *io, avro_long_t * lp);
avro_status_t avro_getint (struct avro_io *io, avro_int_t * ip);
avro_status_t avro_getlong (struct avro_io *io, avro_long_t * lp);

#define DEBUGGING 0

#if DEBUGGING
#define DEBUG(__cmd) __cmd
#else
#define DEBUG(__cmd)
#endif

#include "container_of.h"
#include "json.h"

#endif
