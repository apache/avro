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

#include "avro.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static avro_schema_t parse_schema(const char* json)
{
	avro_schema_t schema;
	int rval = avro_schema_from_json(json, 0, &schema, NULL);
	if (rval != 0) {
		fprintf(stderr, "Failed to parse decimal json: %d\n", rval);
		exit(EXIT_FAILURE);
	}

	if (!is_avro_schema(schema)) {
		fprintf(stderr, "Parsed json is not a schema\n");
		exit(EXIT_FAILURE);
	}

	if (!is_avro_decimal(schema)) {
		fprintf(stderr, "Parsed schema is not a decimal\n");
		exit(EXIT_FAILURE);
	}

	return schema;
}

static void test_parse_decimal_bytes_success(void)
{
	static const char json[] =
	    "{"
	    "  \"type\": \"bytes\","
	    "  \"logicalType\": \"decimal\","
	    "  \"precision\": 4,"
	    "  \"scale\": 2"
	    "}";

	avro_schema_t schema = parse_schema(json);

	size_t precision = avro_schema_decimal_precision(schema);
	if (precision != 4) {
		fprintf(stderr,
			"Decimal precision is %zu, not %d", precision, 4);
		exit(EXIT_FAILURE);
	}

	size_t scale = avro_schema_decimal_scale(schema);
	if (scale != 2) {
		fprintf(stderr, "Decimal scale is %zu, not %d", scale, 2);
		exit(EXIT_FAILURE);
	}

	avro_schema_t underlying = avro_schema_logical_underlying(schema);
	if (!is_avro_bytes(underlying)) {
		fprintf(stderr, "Underlying decimal schema is not bytes");
		exit(EXIT_FAILURE);
	}

	avro_schema_decref(schema);
}

static void test_parse_decimal_fixed_success(void)
{
	static const char json[] =
	    "{"
	    "  \"type\": \"fixed\","
	    "  \"size\": 16,"
	    "  \"name\": \"md5\","
	    "  \"logicalType\": \"decimal\","
	    "  \"precision\": 5,"
	    "  \"scale\": 3"
	    "}";

	avro_schema_t schema = parse_schema(json);

	size_t precision = avro_schema_decimal_precision(schema);
	if (precision != 5) {
		fprintf(stderr,
			"Decimal precision is %zu, not %d", precision, 5);
		exit(EXIT_FAILURE);
	}

	size_t scale = avro_schema_decimal_scale(schema);
	if (scale != 3) {
		fprintf(stderr, "Decimal scale is %zu, not %d", scale, 3);
		exit(EXIT_FAILURE);
	}

	avro_schema_t underlying = avro_schema_logical_underlying(schema);
	if (!is_avro_fixed(underlying)) {
		fprintf(stderr, "Underlying decimal schema is not fixed");
		exit(EXIT_FAILURE);
	}

	size_t size = avro_schema_fixed_size(underlying);
	if (size != 16) {
		fprintf(stderr, "Fixed size is %zu, not %d\n", size, 16);
		exit(EXIT_FAILURE);
	}

	const char* name = avro_schema_name(underlying);
	if (strcmp(name, "md5") != 0) {
		fprintf(stderr, "Fixed name is %s, not %s", name, "md5");
		exit(EXIT_FAILURE);
	}

	avro_schema_decref(schema);
}

static void test_parse_decimal_bytes_scale_too_high(void)
{
	static const char json[] =
	    "{"
	    "  \"type\": \"bytes\","
	    "  \"logicalType\": \"decimal\","
	    "  \"precision\": 4,"
	    "  \"scale\": 5"
	    "}";

	avro_schema_t schema;
	int rval = avro_schema_from_json(json, 0, &schema, NULL);
	if (rval != 0) {
		fprintf(stderr, "Failed to parse invalid decimal json: %d\n",
			rval);
		exit(EXIT_FAILURE);
	}

	if (!is_avro_schema(schema)) {
		fprintf(stderr, "Parsed json is not a schema\n");
		exit(EXIT_FAILURE);
	}

	if (!is_avro_bytes(schema)) {
		fprintf(stderr, "Parsed schema did not fallback to bytes\n");
		exit(EXIT_FAILURE);
	}

	avro_schema_decref(schema);
}

static void test_parse_decimal_fixed_size_too_low(void)
{
	static const char json[] =
	    "{"
	    "  \"type\": \"fixed\","
	    "  \"size\": 1,"
	    "  \"name\": \"single\","
	    "  \"logicalType\": \"decimal\","
	    "  \"precision\": 4,"
	    "  \"scale\": 2"
	    "}";

	avro_schema_t schema;
	int rval = avro_schema_from_json(json, 0, &schema, NULL);
	if (rval != 0) {
		fprintf(stderr, "Failed to parse invalid decimal json: %d\n",
			rval);
		exit(EXIT_FAILURE);
	}

	if (!is_avro_schema(schema)) {
		fprintf(stderr, "Parsed json is not a schema\n");
		exit(EXIT_FAILURE);
	}

	if (!is_avro_fixed(schema)) {
		fprintf(stderr, "Parsed schema did not fallback to fixed\n");
		exit(EXIT_FAILURE);
	}

	size_t size = avro_schema_fixed_size(schema);
	if (size != 1) {
		fprintf(stderr, "Fixed size is %zu, not %d\n", size, 1);
		exit(EXIT_FAILURE);
	}

	const char* name = avro_schema_name(schema);
	if (strcmp(name, "single") != 0) {
		fprintf(stderr, "Fixed name is %s, not %s", name, "single");
		exit(EXIT_FAILURE);
	}

	avro_schema_decref(schema);
}

int main(void)
{
	test_parse_decimal_bytes_success();
	test_parse_decimal_fixed_success();
	test_parse_decimal_bytes_scale_too_high();
	test_parse_decimal_fixed_size_too_low();
	return EXIT_SUCCESS;
}
