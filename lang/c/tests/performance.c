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

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "avro.h"
#include "avro_private.h"

/*
 * A series of performance tests.
 */

typedef void
(*test_func_t)(unsigned long);


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


/**
 * Tests the single-threaded performance of our reference counting
 * mechanism.  We create a single datum, and then reference and
 * deference it many many times.
 */

static void
test_refcount(unsigned long num_tests)
{
	unsigned long  i;

	avro_datum_t  datum = avro_int32(42);
	for (i = 0; i < num_tests; i++) {
		avro_datum_incref(datum);
		avro_datum_decref(datum);
	}
	avro_datum_decref(datum);
}


/**
 * Tests the performance of serializing and deserializing a somewhat
 * complex record type using the legacy datum API.
 */

static void
test_nested_record_datum(unsigned long num_tests)
{
	static const char  *schema_json =
		"{"
		"  \"type\": \"record\","
		"  \"name\": \"test\","
		"  \"fields\": ["
		"    { \"name\": \"i\", \"type\": \"int\" },"
		"    { \"name\": \"l\", \"type\": \"long\" },"
		"    { \"name\": \"s\", \"type\": \"string\" },"
		"    {"
		"      \"name\": \"subrec\","
		"      \"type\": {"
		"        \"type\": \"record\","
		"        \"name\": \"sub\","
		"        \"fields\": ["
		"          { \"name\": \"f\", \"type\": \"float\" },"
		"          { \"name\": \"d\", \"type\": \"double\" }"
		"        ]"
		"      }"
		"    }"
		"  ]"
		"}";

	static const char *strings[] = {
		"Four score and seven years ago",
		"our father brought forth on this continent",
		"a new nation", "conceived in Liberty",
		"and dedicated to the proposition that all men are created equal."
	};
	static const unsigned int  NUM_STRINGS =
	    sizeof(strings) / sizeof(strings[0]);

	int  rc;
	static char  buf[4096];
	avro_reader_t  reader;
	avro_writer_t  writer;

	avro_schema_t  schema = NULL;
	avro_schema_error_t  error = NULL;
	avro_schema_from_json(schema_json, strlen(schema_json),
			      &schema, &error);

	unsigned long  i;

	avro_datum_t  in = avro_datum_from_schema(schema);

	for (i = 0; i < num_tests; i++) {
		avro_record_set_field_value(rc, in, int32, "i", rand_int32());
		avro_record_set_field_value(rc, in, int64, "l", rand_int64());
		avro_record_set_field_value(rc, in, givestring, "s",
					    strings[i % NUM_STRINGS], NULL);

		avro_datum_t  subrec = NULL;
		avro_record_get(in, "subrec", &subrec);
		avro_record_set_field_value(rc, in, float, "f", rand_number(-1e10, 1e10));
		avro_record_set_field_value(rc, in, double, "d", rand_number(-1e10, 1e10));

		writer = avro_writer_memory(buf, sizeof(buf));
		avro_write_data(writer, schema, in);
		avro_writer_free(writer);

		avro_datum_t  out = NULL;

		reader = avro_reader_memory(buf, sizeof(buf));
		avro_read_data(reader, schema, schema, &out);
		avro_reader_free(reader);

		avro_datum_equal(in, out);
		avro_datum_decref(out);
	}

	avro_datum_decref(in);
	avro_schema_decref(schema);
}


/**
 * Tests the performance of serializing and deserializing a somewhat
 * complex record type using the new value API, retrieving record fields
 * by index.
 */

static void
test_nested_record_value_by_index(unsigned long num_tests)
{
	static const char  *schema_json =
		"{"
		"  \"type\": \"record\","
		"  \"name\": \"test\","
		"  \"fields\": ["
		"    { \"name\": \"i\", \"type\": \"int\" },"
		"    { \"name\": \"l\", \"type\": \"long\" },"
		"    { \"name\": \"s\", \"type\": \"string\" },"
		"    {"
		"      \"name\": \"subrec\","
		"      \"type\": {"
		"        \"type\": \"record\","
		"        \"name\": \"sub\","
		"        \"fields\": ["
		"          { \"name\": \"f\", \"type\": \"float\" },"
		"          { \"name\": \"d\", \"type\": \"double\" }"
		"        ]"
		"      }"
		"    }"
		"  ]"
		"}";

	static char *strings[] = {
		"Four score and seven years ago",
		"our father brought forth on this continent",
		"a new nation", "conceived in Liberty",
		"and dedicated to the proposition that all men are created equal."
	};
	static const unsigned int  NUM_STRINGS =
	    sizeof(strings) / sizeof(strings[0]);

	static char  buf[4096];
	avro_reader_t  reader;
	avro_writer_t  writer;

	avro_schema_t  schema = NULL;
	avro_schema_error_t  error = NULL;
	avro_schema_from_json(schema_json, strlen(schema_json),
			      &schema, &error);

	unsigned long  i;

	avro_value_iface_t  *iface = avro_generic_class_from_schema(schema);

	avro_value_t  val;
	avro_value_new(iface, &val);

	avro_value_t  out;
	avro_value_new(iface, &out);

	for (i = 0; i < num_tests; i++) {
		avro_value_t  field;

		avro_value_get_by_index(&val, 0, &field, NULL);
		avro_value_set_int(&field, rand_int32());

		avro_value_get_by_index(&val, 1, &field, NULL);
		avro_value_set_long(&field, rand_int64());

		avro_wrapped_buffer_t  wbuf;
		avro_wrapped_buffer_new_string(&wbuf, strings[i % NUM_STRINGS]);
		avro_value_get_by_index(&val, 2, &field, NULL);
		avro_value_give_string_len(&field, &wbuf);

		avro_value_t  subrec;
		avro_value_get_by_index(&val, 3, &subrec, NULL);

		avro_value_get_by_index(&subrec, 0, &field, NULL);
		avro_value_set_float(&field, rand_number(-1e10, 1e10));

		avro_value_get_by_index(&subrec, 1, &field, NULL);
		avro_value_set_double(&field, rand_number(-1e10, 1e10));

		writer = avro_writer_memory(buf, sizeof(buf));
		avro_value_write(writer, &val);
		avro_writer_free(writer);

		reader = avro_reader_memory(buf, sizeof(buf));
		avro_value_read(reader, &out);
		avro_reader_free(reader);

		avro_value_equal_fast(&val, &out);
	}

	avro_value_free(&val);
	avro_value_free(&out);
	avro_value_iface_decref(iface);
	avro_schema_decref(schema);
}


/**
 * Tests the performance of serializing and deserializing a somewhat
 * complex record type using the new value API, retrieving record fields
 * by name.
 */

static void
test_nested_record_value_by_name(unsigned long num_tests)
{
	static const char  *schema_json =
		"{"
		"  \"type\": \"record\","
		"  \"name\": \"test\","
		"  \"fields\": ["
		"    { \"name\": \"i\", \"type\": \"int\" },"
		"    { \"name\": \"l\", \"type\": \"long\" },"
		"    { \"name\": \"s\", \"type\": \"string\" },"
		"    {"
		"      \"name\": \"subrec\","
		"      \"type\": {"
		"        \"type\": \"record\","
		"        \"name\": \"sub\","
		"        \"fields\": ["
		"          { \"name\": \"f\", \"type\": \"float\" },"
		"          { \"name\": \"d\", \"type\": \"double\" }"
		"        ]"
		"      }"
		"    }"
		"  ]"
		"}";

	static char *strings[] = {
		"Four score and seven years ago",
		"our father brought forth on this continent",
		"a new nation", "conceived in Liberty",
		"and dedicated to the proposition that all men are created equal."
	};
	static const unsigned int  NUM_STRINGS =
	    sizeof(strings) / sizeof(strings[0]);

	static char  buf[4096];
	avro_reader_t  reader;
	avro_writer_t  writer;

	avro_schema_t  schema = NULL;
	avro_schema_error_t  error = NULL;
	avro_schema_from_json(schema_json, strlen(schema_json),
			      &schema, &error);

	unsigned long  i;

	avro_value_iface_t  *iface = avro_generic_class_from_schema(schema);

	avro_value_t  val;
	avro_value_new(iface, &val);

	avro_value_t  out;
	avro_value_new(iface, &out);

	for (i = 0; i < num_tests; i++) {
		avro_value_t  field;

		avro_value_get_by_name(&val, "i", &field, NULL);
		avro_value_set_int(&field, rand_int32());

		avro_value_get_by_name(&val, "l", &field, NULL);
		avro_value_set_long(&field, rand_int64());

		avro_wrapped_buffer_t  wbuf;
		avro_wrapped_buffer_new_string(&wbuf, strings[i % NUM_STRINGS]);
		avro_value_get_by_name(&val, "s", &field, NULL);
		avro_value_give_string_len(&field, &wbuf);

		avro_value_t  subrec;
		avro_value_get_by_name(&val, "subrec", &subrec, NULL);

		avro_value_get_by_name(&subrec, "f", &field, NULL);
		avro_value_set_float(&field, rand_number(-1e10, 1e10));

		avro_value_get_by_name(&subrec, "d", &field, NULL);
		avro_value_set_double(&field, rand_number(-1e10, 1e10));

		writer = avro_writer_memory(buf, sizeof(buf));
		avro_value_write(writer, &val);
		avro_writer_free(writer);

		reader = avro_reader_memory(buf, sizeof(buf));
		avro_value_read(reader, &out);
		avro_reader_free(reader);

		avro_value_equal_fast(&val, &out);
	}

	avro_value_free(&val);
	avro_value_free(&out);
	avro_value_iface_decref(iface);
	avro_schema_decref(schema);
}


/**
 * Test harness
 */

#define NUM_RUNS  3

int
main(int argc, char **argv)
{
	AVRO_UNUSED(argc);
	AVRO_UNUSED(argv);

	init_rand();

	unsigned int  i;
	struct avro_tests {
		const char  *name;
		unsigned long  num_tests;
		test_func_t  func;
	} tests[] = {
		{ "refcount", 100000000,
		  test_refcount },
		{ "nested record (legacy)", 100000,
		  test_nested_record_datum },
		{ "nested record (value by index)", 1000000,
		  test_nested_record_value_by_index },
		{ "nested record (value by name)", 1000000,
		  test_nested_record_value_by_name },
	};

	for (i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		fprintf(stderr, "**** Running %s ****\n  %lu tests per run\n",
			tests[i].name, tests[i].num_tests);
		unsigned int  run;

		double  sum = 0.0;

		for (run = 1; run <= NUM_RUNS; run++) {
			fprintf(stderr, "  Run %u\n", run);

			clock_t  before = clock();
			tests[i].func(tests[i].num_tests);
			clock_t  after = clock();
			double  secs = ((double) after-before) / CLOCKS_PER_SEC;
			sum += secs;
		}

		fprintf(stderr, "  Average time: %.03lfs\n", sum / NUM_RUNS);
		fprintf(stderr, "  Tests/sec:    %.0lf\n",
			tests[i].num_tests / (sum / NUM_RUNS));
	}

	return EXIT_SUCCESS;
}
