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
#include <inttypes.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

char buf[4096];
avro_reader_t reader;
avro_writer_t writer;

typedef int (*avro_test) (void);

/*
 * Use a custom allocator that verifies that the size that we use to
 * free an object matches the size that we use to allocate it.
 */

static void *
test_allocator(void *ud, void *ptr, size_t osize, size_t nsize)
{
	AVRO_UNUSED(ud);
	AVRO_UNUSED(osize);

	if (nsize == 0) {
		size_t  *size = ((size_t *) ptr) - 1;
		if (osize != *size) {
			fprintf(stderr,
				"Error freeing %p:\n"
				"Size passed to avro_free (%zu) "
				"doesn't match size passed to "
				"avro_malloc (%zu)\n",
				ptr, osize, *size);
			exit(EXIT_FAILURE);
		}
		free(size);
		return NULL;
	} else {
		size_t  real_size = nsize + sizeof(size_t);
		size_t  *old_size = ptr? ((size_t *) ptr)-1: NULL;
		size_t  *size = realloc(old_size, real_size);
		*size = nsize;
		return (size + 1);
	}
}

void init_rand(void)
{
	srand(time(NULL));
}

double rand_number(double from, double to)
{
	double range = to - from;
	return from + ((double)rand() / (RAND_MAX + 1.0)) * range;
}

int64_t rand_int64(void)
{
	return (int64_t) rand_number(LONG_MIN, LONG_MAX);
}

int32_t rand_int32(void)
{
	return (int32_t) rand_number(INT_MIN, INT_MAX);
}

void
write_read_check(avro_schema_t writers_schema,
		 avro_schema_t readers_schema, avro_datum_t datum, char *type)
{
	avro_datum_t datum_out;
	int validate;

	for (validate = 0; validate <= 1; validate++) {

		reader = avro_reader_memory(buf, sizeof(buf));
		writer = avro_writer_memory(buf, sizeof(buf));

		/* Validating read/write */
		if (avro_write_data
		    (writer, validate ? writers_schema : NULL, datum)) {
			fprintf(stderr, "Unable to write %s validate=%d\n",
				type, validate);
			exit(EXIT_FAILURE);
		}
		int64_t size =
		    avro_size_data(writer, validate ? writers_schema : NULL,
				   datum);
		if (size != avro_writer_tell(writer)) {
			fprintf(stderr,
				"Unable to calculate size %s validate=%d (%"PRId64" != %"PRId64")\n",
				type, validate, size, avro_writer_tell(writer));
			exit(EXIT_FAILURE);
		}
		if (avro_read_data
		    (reader, writers_schema, readers_schema, &datum_out)) {
			fprintf(stderr, "Unable to read %s validate=%d\n", type,
				validate);
			exit(EXIT_FAILURE);
		}
		if (!avro_datum_equal(datum, datum_out)) {
			fprintf(stderr,
				"Unable to encode/decode %s validate=%d\n",
				type, validate);
			exit(EXIT_FAILURE);
		}

		avro_reader_dump(reader, stderr);
		avro_datum_decref(datum_out);
		avro_reader_free(reader);
		avro_writer_free(writer);
	}
}

static void test_json(avro_datum_t datum, avro_schema_t schema,
		      const char *expected)
{
	char  *json = NULL;
	avro_datum_to_json(datum, schema, 1, &json);
	if (strcmp(json, expected) != 0) {
		fprintf(stderr, "Unexpected JSON encoding: %s\n", json);
		exit(EXIT_FAILURE);
	}
	free(json);
}

static int test_string(void)
{
	unsigned int i;
	const char *strings[] = { "Four score and seven years ago",
		"our father brought forth on this continent",
		"a new nation", "conceived in Liberty",
		"and dedicated to the proposition that all men are created equal."
	};
	avro_schema_t writer_schema = avro_schema_string();
	for (i = 0; i < sizeof(strings) / sizeof(strings[0]); i++) {
		avro_datum_t datum = avro_wrapstring(strings[i]);
		write_read_check(writer_schema, NULL, datum, "string");
		avro_datum_decref(datum);
	}

	avro_datum_t  datum = avro_wrapstring(strings[0]);
	test_json(datum, writer_schema,
		  "\"Four score and seven years ago\"");
	avro_datum_decref(datum);

	avro_schema_decref(writer_schema);
	return 0;
}

static int test_bytes(void)
{
	char bytes[] = { 0xDE, 0xAD, 0xBE, 0xEF };
	avro_schema_t writer_schema = avro_schema_bytes();
	avro_datum_t datum;
	avro_datum_t expected_datum;

	datum = avro_wrapbytes(bytes, sizeof(bytes));
	write_read_check(writer_schema, NULL, datum, "bytes");
	test_json(datum, writer_schema,
		  "\"\\u00de\\u00ad\\u00be\\u00ef\"");
	avro_datum_decref(datum);
	avro_schema_decref(writer_schema);

	datum = avro_wrapbytes(NULL, 0);
    avro_wrapbytes_set(datum, bytes, sizeof(bytes));
	expected_datum = avro_wrapbytes(bytes, sizeof(bytes));
	if (!avro_datum_equal(datum, expected_datum)) {
		fprintf(stderr,
		        "Expected equal bytes instances.\n");
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(datum);
	avro_datum_decref(expected_datum);
	avro_schema_decref(writer_schema);

	return 0;
}

static int test_int32(void)
{
	int i;
	avro_schema_t writer_schema = avro_schema_int();
	for (i = 0; i < 100; i++) {
		avro_datum_t datum = avro_int32(rand_int32());
		write_read_check(writer_schema, NULL, datum, "int");
		avro_datum_decref(datum);
	}

	avro_datum_t  datum = avro_int32(10000);
	test_json(datum, writer_schema, "10000");
	avro_datum_decref(datum);

	avro_schema_decref(writer_schema);
	return 0;
}

static int test_int64(void)
{
	int i;
	avro_schema_t writer_schema = avro_schema_long();
	for (i = 0; i < 100; i++) {
		avro_datum_t datum = avro_int64(rand_int64());
		write_read_check(writer_schema, NULL, datum, "long");
		avro_datum_decref(datum);
	}

	avro_datum_t  datum = avro_int64(10000);
	test_json(datum, writer_schema, "10000");
	avro_datum_decref(datum);

	avro_schema_decref(writer_schema);
	return 0;
}

static int test_double(void)
{
	int i;
	avro_schema_t schema = avro_schema_double();
	for (i = 0; i < 100; i++) {
		avro_datum_t datum = avro_double(rand_number(-1.0E10, 1.0E10));
		write_read_check(schema, NULL, datum, "double");
		avro_datum_decref(datum);
	}

	avro_datum_t  datum = avro_double(2000.0);
	test_json(datum, schema, "2000.0");
	avro_datum_decref(datum);

	avro_schema_decref(schema);
	return 0;
}

static int test_float(void)
{
	int i;
	avro_schema_t schema = avro_schema_double();
	for (i = 0; i < 100; i++) {
		avro_datum_t datum = avro_double(rand_number(-1.0E10, 1.0E10));
		write_read_check(schema, NULL, datum, "float");
		avro_datum_decref(datum);
	}

	avro_datum_t  datum = avro_float(2000.0);
	test_json(datum, schema, "2000.0");
	avro_datum_decref(datum);

	avro_schema_decref(schema);
	return 0;
}

static int test_boolean(void)
{
	int i;
	const char  *expected_json[] = { "false", "true" };
	avro_schema_t schema = avro_schema_boolean();
	for (i = 0; i <= 1; i++) {
		avro_datum_t datum = avro_boolean(i);
		write_read_check(schema, NULL, datum, "boolean");
		test_json(datum, schema, expected_json[i]);
		avro_datum_decref(datum);
	}
	avro_schema_decref(schema);
	return 0;
}

static int test_null(void)
{
	avro_schema_t schema = avro_schema_null();
	avro_datum_t datum = avro_null();
	write_read_check(schema, NULL, datum, "null");
	test_json(datum, schema, "null");
	avro_datum_decref(datum);
	return 0;
}

static int test_record(void)
{
	avro_schema_t schema = avro_schema_record("person", NULL);
	avro_datum_t datum = avro_record("person", NULL);
	avro_datum_t name_datum, age_datum;

	avro_schema_record_field_append(schema, "name", avro_schema_string());
	avro_schema_record_field_append(schema, "age", avro_schema_int());

	name_datum = avro_wrapstring("Joseph Campbell");
	age_datum = avro_int32(83);

	avro_record_set(datum, "name", name_datum);
	avro_record_set(datum, "age", age_datum);

	write_read_check(schema, NULL, datum, "record");
	test_json(datum, schema,
		  "{\"name\": \"Joseph Campbell\", \"age\": 83}");

	avro_datum_decref(name_datum);
	avro_datum_decref(age_datum);
	avro_datum_decref(datum);
	avro_schema_decref(schema);
	return 0;
}

static int test_enum(void)
{
	enum avro_languages {
		AVRO_C,
		AVRO_CPP,
		AVRO_PYTHON,
		AVRO_RUBY,
		AVRO_JAVA
	};
	avro_schema_t schema = avro_schema_enum("language");
	avro_datum_t datum = avro_enum("language", AVRO_C);

	avro_schema_enum_symbol_append(schema, "C");
	avro_schema_enum_symbol_append(schema, "C++");
	avro_schema_enum_symbol_append(schema, "Python");
	avro_schema_enum_symbol_append(schema, "Ruby");
	avro_schema_enum_symbol_append(schema, "Java");

	if (avro_enum_get(datum) != AVRO_C) {
		fprintf(stderr, "Unexpected enum value AVRO_C\n");
		exit(EXIT_FAILURE);
	}

	if (strcmp(avro_enum_get_name(datum, schema), "C") != 0) {
		fprintf(stderr, "Unexpected enum value name C\n");
		exit(EXIT_FAILURE);
	}

	write_read_check(schema, NULL, datum, "enum");
	test_json(datum, schema, "\"C\"");

	avro_enum_set(datum, AVRO_CPP);
	if (strcmp(avro_enum_get_name(datum, schema), "C++") != 0) {
		fprintf(stderr, "Unexpected enum value name C++\n");
		exit(EXIT_FAILURE);
	}

	write_read_check(schema, NULL, datum, "enum");
	test_json(datum, schema, "\"C++\"");

	avro_enum_set_name(datum, schema, "Python");
	if (avro_enum_get(datum) != AVRO_PYTHON) {
		fprintf(stderr, "Unexpected enum value AVRO_PYTHON\n");
		exit(EXIT_FAILURE);
	}

	write_read_check(schema, NULL, datum, "enum");
	test_json(datum, schema, "\"Python\"");

	avro_datum_decref(datum);
	avro_schema_decref(schema);
	return 0;
}

static int test_array(void)
{
	int i, rval;
	avro_schema_t schema = avro_schema_array(avro_schema_int());
	avro_datum_t datum = avro_array();

	for (i = 0; i < 10; i++) {
		avro_datum_t i32_datum = avro_int32(i);
		rval = avro_array_append_datum(datum, i32_datum);
		avro_datum_decref(i32_datum);
		if (rval) {
			exit(EXIT_FAILURE);
		}
	}

	if (avro_array_size(datum) != 10) {
		fprintf(stderr, "Unexpected array size");
		exit(EXIT_FAILURE);
	}

	write_read_check(schema, NULL, datum, "array");
	test_json(datum, schema, "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]");
	avro_datum_decref(datum);
	avro_schema_decref(schema);
	return 0;
}

static int test_map(void)
{
	avro_schema_t schema = avro_schema_map(avro_schema_long());
	avro_datum_t datum = avro_map();
	int64_t i = 0;
	char *nums[] =
	    { "zero", "one", "two", "three", "four", "five", "six", NULL };
	while (nums[i]) {
		avro_datum_t i_datum = avro_int64(i);
		avro_map_set(datum, nums[i], i_datum);
		avro_datum_decref(i_datum);
		i++;
	}

	if (avro_array_size(datum) != 7) {
		fprintf(stderr, "Unexpected map size\n");
		exit(EXIT_FAILURE);
	}

	avro_datum_t value;
	const char  *key;
	avro_map_get_key(datum, 2, &key);
	avro_map_get(datum, key, &value);
	int64_t  val;
	avro_int64_get(value, &val);

	if (val != 2) {
		fprintf(stderr, "Unexpected map value 2\n");
		exit(EXIT_FAILURE);
	}

	write_read_check(schema, NULL, datum, "map");
	test_json(datum, schema,
		  "{\"zero\": 0, \"one\": 1, \"two\": 2, \"three\": 3, "
		  "\"four\": 4, \"five\": 5, \"six\": 6}");
	avro_datum_decref(datum);
	avro_schema_decref(schema);
	return 0;
}

static int test_union(void)
{
	avro_schema_t schema = avro_schema_union();
	avro_datum_t union_datum;
	avro_datum_t datum;
	avro_datum_t union_datum1;
	avro_datum_t datum1;

	avro_schema_union_append(schema, avro_schema_string());
	avro_schema_union_append(schema, avro_schema_int());
	avro_schema_union_append(schema, avro_schema_null());

	datum = avro_wrapstring("Follow your bliss.");
	union_datum = avro_union(0, datum);

	if (avro_union_discriminant(union_datum) != 0) {
		fprintf(stderr, "Unexpected union discriminant\n");
		exit(EXIT_FAILURE);
	}

	if (avro_union_current_branch(union_datum) != datum) {
		fprintf(stderr, "Unexpected union branch datum\n");
		exit(EXIT_FAILURE);
	}

	union_datum1 = avro_datum_from_schema(schema);
	avro_union_set_discriminant(union_datum1, schema, 0, &datum1);
	avro_wrapstring_set(datum1, "Follow your bliss.");

	if (!avro_datum_equal(datum, datum1)) {
		fprintf(stderr, "Union values should be equal\n");
		exit(EXIT_FAILURE);
	}

	write_read_check(schema, NULL, union_datum, "union");
	test_json(union_datum, schema,
		  "{\"string\": \"Follow your bliss.\"}");

	avro_datum_decref(datum);
	avro_union_set_discriminant(union_datum, schema, 2, &datum);
	test_json(union_datum, schema, "null");

	avro_datum_decref(union_datum);
	avro_datum_decref(datum);
	avro_datum_decref(union_datum1);
	avro_schema_decref(schema);
	return 0;
}

static int test_fixed(void)
{
	char bytes[] = { 0xD, 0xA, 0xD, 0xA, 0xB, 0xA, 0xB, 0xA };
	avro_schema_t schema = avro_schema_fixed("msg", sizeof(bytes));
	avro_datum_t datum;
	avro_datum_t expected_datum;

	datum = avro_wrapfixed("msg", bytes, sizeof(bytes));
	write_read_check(schema, NULL, datum, "fixed");
	test_json(datum, schema, "\"\\r\\n\\r\\n\\u000b\\n\\u000b\\n\"");
	avro_datum_decref(datum);
	avro_schema_decref(schema);

	datum = avro_wrapfixed("msg", NULL, 0);
    avro_wrapfixed_set(datum, bytes, sizeof(bytes));
	expected_datum = avro_wrapfixed("msg", bytes, sizeof(bytes));
	if (!avro_datum_equal(datum, expected_datum)) {
		fprintf(stderr,
		        "Expected equal fixed instances.\n");
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(datum);
	avro_datum_decref(expected_datum);
	avro_schema_decref(schema);

	return 0;
}

int main(void)
{
	avro_set_allocator(test_allocator, NULL);

	unsigned int i;
	struct avro_tests {
		char *name;
		avro_test func;
	} tests[] = {
		{
		"string", test_string}, {
		"bytes", test_bytes}, {
		"int", test_int32}, {
		"long", test_int64}, {
		"float", test_float}, {
		"double", test_double}, {
		"boolean", test_boolean}, {
		"null", test_null}, {
		"record", test_record}, {
		"enum", test_enum}, {
		"array", test_array}, {
		"map", test_map}, {
		"fixed", test_fixed}, {
		"union", test_union}
	};

	init_rand();
	for (i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		struct avro_tests *test = tests + i;
		fprintf(stderr, "**** Running %s tests ****\n", test->name);
		if (test->func() != 0) {
			return EXIT_FAILURE;
		}
	}
	return EXIT_SUCCESS;
}
