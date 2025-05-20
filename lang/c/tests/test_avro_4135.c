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
#include <string.h>
#include <avro.h>

#define check_exit(call) \
	do { \
		int  __rc = call; \
		if (__rc != 0) { \
			fprintf(stderr, "Unexpected error:\n  %s\n  %s\n", \
				avro_strerror(), #call); \
			exit(EXIT_FAILURE); \
		} \
	} while (0)

void check_json_encoding(const avro_value_t * val, const char * expected) {
	char * actual;

	check_exit(avro_value_to_json(val, 1, &actual));
	if (strcmp(expected, actual) != 0) {
		fprintf(stderr, "json encoding mismatch\n expected: %s\n actual:  %s\n", expected, actual);
		exit(EXIT_FAILURE);
	}
	free(actual);
}

int main(int argc, char **argv)
{
	const char  *json =
		"{"
		"  \"type\": \"record\","
		"  \"name\": \"r\","
		"  \"fields\": ["
		"    { \"name\": \"field\", \"type\": ["
		"       \"null\","
		"       \"int\","
		"       {"
		"         \"type\": \"record\","
		"         \"name\": \"r1\","
		"         \"fields\": []"
	    "       },"
		"       {"
		"         \"type\": \"record\","
		"         \"name\": \"r2\","
		"         \"namespace\": \"space\","
		"         \"fields\": []"
	    "       }"
		"     ]}"
		"  ]"
		"}";

	avro_schema_t schema = NULL;
	avro_schema_error_t error;

	(void) argc;
	(void) argv;

	check_exit(avro_schema_from_json(json, strlen(json), &schema, &error));

	avro_value_iface_t  *iface = avro_generic_class_from_schema(schema);
	avro_value_t  val;
	check_exit(avro_generic_value_new(iface, &val));

#define TEST_AVRO_4135 (1)
       #if TEST_AVRO_4135
	{
		avro_value_t  field;
		avro_value_t  branch;
		char  *json_encoding;

		avro_value_get_by_index(&val, 0, &field, NULL);
		check_exit(avro_value_set_branch(&field, 0, &branch));
		avro_value_set_null(&branch);
		check_json_encoding(&val, "{\"field\": null}");

		check_exit(avro_value_set_branch(&field, 1, &branch));
		avro_value_set_int(&branch, 42);
		check_exit(avro_value_to_json(&val, 1, &json_encoding));
		check_json_encoding(&val, "{\"field\": {\"int\": 42}}");

		check_exit(avro_value_set_branch(&field, 2, &branch));
		check_exit(avro_value_to_json(&val, 1, &json_encoding));
		check_json_encoding(&val, "{\"field\": {\"r1\": {}}}");

		check_exit(avro_value_set_branch(&field, 3, &branch));
		check_exit(avro_value_to_json(&val, 3, &json_encoding));
		check_json_encoding(&val, "{\"field\": {\"space.r2\": {}}}");
	}
       #endif

	avro_value_decref(&val);
	avro_schema_decref(schema);
	return 0;
}
