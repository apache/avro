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
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <string.h>
#include "avro.h"

char buf[4096];
avro_reader_t reader;
avro_writer_t writer;

typedef int (*avro_test) (void);

void
write_read_check (avro_schema_t writers_schema, avro_schema_t readers_schema,
		  avro_datum_t datum, char *type)
{
  avro_datum_t datum_out;
  reader = avro_reader_memory (buf, sizeof (buf));
  writer = avro_writer_memory (buf, sizeof (buf));

  if (avro_write_data (writer, writers_schema, datum))
    {
      fprintf (stderr, "Unable to write %s\n", type);
      exit (EXIT_FAILURE);
    }
  if (avro_read_data (reader, writers_schema, readers_schema, &datum_out))
    {
      fprintf (stderr, "Unable to read %s\n", type);
      exit (EXIT_FAILURE);
    }
  if (!avro_datum_equal (datum, datum_out))
    {
      fprintf (stderr, "Unable to encode/decode %s\n", type);
      exit (EXIT_FAILURE);
    }

  avro_datum_decref (datum_out);
  avro_reader_free (reader);
  avro_writer_free (writer);
}

static int
test_string (void)
{
  int i;
  const char *strings[] = { "Four score and seven years ago",
    "our father brought forth on this continent",
    "a new nation", "conceived in Liberty",
    "and dedicated to the proposition that all men are created equal."
  };
  for (i = 0; i < sizeof (strings) / sizeof (strings[0]); i++)
    {
      avro_schema_t writer_schema = avro_schema_string ();
      avro_datum_t datum = avro_string (strings[i]);
      write_read_check (writer_schema, NULL, datum, "string");
      avro_datum_decref (datum);
    }
  return 0;
}

static int
test_bytes (void)
{
  char bytes[] = { 0xDE, 0xAD, 0xBE, 0xEF };
  avro_schema_t writer_schema = avro_schema_bytes ();
  avro_datum_t datum = avro_bytes (bytes, sizeof (bytes));

  write_read_check (writer_schema, NULL, datum, "bytes");
  avro_datum_decref (datum);
  return 0;
}

static int
test_int (void)
{
  int i;
  for (i = 0; i < 100; i++)
    {
      avro_schema_t writer_schema = avro_schema_int ();
      avro_datum_t datum = avro_int (rand ());
      write_read_check (writer_schema, NULL, datum, "int");
      avro_datum_decref (datum);
    }
  return 0;
}

static int
test_long (void)
{
  int i;
  for (i = 0; i < 100; i++)
    {
      avro_schema_t writer_schema = avro_schema_long ();
      avro_datum_t datum = avro_long (rand ());
      write_read_check (writer_schema, NULL, datum, "long");
      avro_datum_decref (datum);
    }
  return 0;
}

static
test_double (void)
{
  int i;
  for (i = 0; i < 100; i++)
    {
      avro_schema_t schema = avro_schema_double ();
      avro_datum_t datum = avro_double ((double) (rand ()));
      write_read_check (schema, NULL, datum, "double");
      avro_datum_decref (datum);
    }
  return 0;
}

static int
test_float (void)
{
  int i;
  for (i = 0; i < 100; i++)
    {
      avro_schema_t schema = avro_schema_double ();
      avro_datum_t datum = avro_double ((double) (rand ()));
      write_read_check (schema, NULL, datum, "float");
      avro_datum_decref (datum);
    }
  return 0;
}

static int
test_boolean (void)
{
  int i;
  for (i = 0; i < 100; i++)
    {
      avro_schema_t schema = avro_schema_boolean ();
      avro_datum_t datum = avro_boolean (rand () % 2);
      write_read_check (schema, NULL, datum, "boolean");
      avro_datum_decref (datum);
    }
  return 0;
}

static int
test_null (void)
{
  avro_schema_t schema = avro_schema_null ();
  avro_datum_t datum = avro_null ();
  write_read_check (schema, NULL, datum, "null");
  avro_datum_decref (datum);
  return 0;
}

int
test_record (void)
{
  /* TODO */
  return 0;
}

int
test_enum (void)
{
  /* TODO */
  return 0;
}

int
test_array (void)
{
  int i, rval;
  avro_schema_t schema = avro_schema_array (avro_schema_int ());
  avro_datum_t datum = avro_array ();

  for (i = 0; i < 10; i++)
    {
      rval = avro_array_append_datum (datum, avro_int (i));
      if (rval)
	{
	  exit (rval);
	}
    }
  write_read_check (schema, NULL, datum, "array");
  avro_datum_decref (datum);
  return 0;
}

int
test_map (void)
{
  avro_schema_t schema = avro_schema_map (avro_schema_long ());
  avro_datum_t datum = avro_map ();
  int64_t i = 0;
  char *nums[] =
    { "zero", "one", "two", "three", "four", "five", "six", NULL };
  while (nums[i])
    {
      avro_map_set (datum, nums[i], avro_long (i));
      i++;
    }
  write_read_check (schema, NULL, datum, "map");
  avro_datum_decref (datum);
  return 0;
}

int
test_union (void)
{
  /* TODO */
  return 0;
}

int
test_fixed (void)
{
  /* TODO */
  return 0;
}

int
main (void)
{
  int i;
  struct avro_tests
  {
    char *name;
    avro_test func;
  } tests[] =
  {
    {
    "string", test_string},
    {
    "bytes", test_bytes},
    {
    "int", test_int},
    {
    "long", test_long},
    {
    "float", test_float},
    {
    "double", test_double},
    {
    "boolean", test_boolean},
    {
    "null", test_null},
    {
    "record", test_record},
    {
    "enum", test_enum},
    {
    "array", test_array},
    {
    "map", test_map},
    {
    "fixed", test_fixed}
  };

  srandom (time (NULL));
  for (i = 0; i < sizeof (tests) / sizeof (tests[0]); i++)
    {
      struct avro_tests *test = tests + i;
      fprintf (stderr, "Running %s tests...\n", test->name);
      if (test->func () != 0)
	{
	  fprintf (stderr, "failed!\n");
	  return EXIT_FAILURE;
	}
      fprintf (stderr, "\t... %s tests passed!\n", test->name);
    }
  return EXIT_SUCCESS;
}
