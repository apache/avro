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
(*test_func_t)(void);


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
test_refcount(void)
{
	const unsigned long  NUM_TESTS = 100000000;
	unsigned long  i;

	avro_datum_t  datum = avro_int32(42);
	for (i = 0; i < NUM_TESTS; i++) {
		avro_datum_incref(datum);
		avro_datum_decref(datum);
	}
	avro_datum_decref(datum);
}


/**
 * Tests the performance of serializing and deserializing a somewhat
 * complex record type.
 */

static void
test_nested_record(void)
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

	const unsigned long  NUM_TESTS = 100000;
	unsigned long  i;

	avro_datum_t  in = avro_datum_from_schema(schema);

	for (i = 0; i < NUM_TESTS; i++) {
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
		test_func_t  func;
	} tests[] = {
		{ "refcount", test_refcount },
		{ "nested record", test_nested_record }
	};

	for (i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		fprintf(stderr, "**** Running %s ****\n", tests[i].name);
		unsigned int  run;

		double  sum = 0.0;

		for (run = 1; run <= NUM_RUNS; run++) {
			fprintf(stderr, "  Run %u\n", run);

			clock_t  before = clock();
			tests[i].func();
			clock_t  after = clock();
			double  secs = ((double) after-before) / CLOCKS_PER_SEC;
			sum += secs;
		}

		fprintf(stderr, "  Average time: %.03lf seconds\n", sum / NUM_RUNS);
	}

	return EXIT_SUCCESS;
}
