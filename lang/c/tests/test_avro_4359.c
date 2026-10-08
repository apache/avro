/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "avro.h"
#include "jansson.h"

#define try(call, msg) \
	do { \
		if (call) { \
			fprintf(stderr, msg ":\n  %s\n", avro_strerror()); \
			return EXIT_FAILURE; \
		} \
	} while (0)

static const char  SCHEMA_JSON[] =
"{ \"type\": \"record\", \"name\": \"R\","
"  \"fields\": ["
"    { \"name\": \"i\", \"type\": \"int\", \"default\": 42 },"
"    { \"name\": \"e\", \"type\": { \"type\": \"enum\", \"name\": \"E\","
"      \"symbols\": [\"A\", \"B\"], \"default\": \"B\" } }"
"  ]"
"}";

/* Checks that the defaults are in the JSON the schema is written as */
static int
check_defaults(avro_schema_t schema)
{
	char  buf[1024] = {0};
	avro_writer_t  writer = avro_writer_memory(buf, sizeof(buf));
	try(avro_schema_to_json(schema, writer), "Cannot write schema");
	try(avro_write(writer, (void *) "", 1), "Cannot terminate schema");
	avro_writer_free(writer);

	json_t  *json = json_loads(buf, 0, NULL);
	json_t  *fields = json_object_get(json, "fields");
	json_t  *field_default =
	    json_object_get(json_array_get(fields, 0), "default");
	json_t  *enum_default = json_object_get
	    (json_object_get(json_array_get(fields, 1), "type"), "default");

	int  ok = json_integer_value(field_default) == 42 &&
	    json_is_string(enum_default) &&
	    strcmp(json_string_value(enum_default), "B") == 0;
	json_decref(json);
	if (!ok) {
		fprintf(stderr, "Defaults are missing from %s\n", buf);
		return EXIT_FAILURE;
	}
	return 0;
}

int main(void)
{
	avro_schema_t  schema;
	try(avro_schema_from_json_literal(SCHEMA_JSON, &schema),
	    "Cannot parse schema");
	avro_schema_t  copy = avro_schema_copy(schema);

	if (check_defaults(schema) || check_defaults(copy)) {
		return EXIT_FAILURE;
	}

	avro_schema_decref(copy);
	avro_schema_decref(schema);
	return EXIT_SUCCESS;
}
