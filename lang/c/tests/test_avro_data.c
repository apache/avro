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
#include <assert.h>
#include "avro.h"
#include "dump.h"
#include "datum.h"

char buf[4096];

typedef int (*avro_test) (void);

struct test_case
{
  long value;
  unsigned len;
  uint8_t bytes[16];
};
typedef struct test_case test_case;

struct test_case test_cases[] = {
  {.value = 0,.len = 1,.bytes = {0x0}},
  {.value = -1,.len = 1,.bytes = {0x1}},
  {.value = 1,.len = 1,.bytes = {0x2}},
  {.value = -2,.len = 1,.bytes = {0x3}},
  {.value = 2,.len = 1,.bytes = {0x4}},
  {.value = -64,.len = 1,.bytes = {0x7f}},
  {.value = 64,.len = 2,.bytes = {0x80, 0x01}},
  {.value = -65,.len = 2,.bytes = {0x81, 0x01}},
  {.value = 65,.len = 2,.bytes = {0x82, 0x01}}
};

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
      avro_reader_t reader;
      avro_writer_t writer;
      avro_schema_t writer_schema = avro_schema_string ();
      avro_datum_t datum_in = avro_string (strings[i]);
      avro_datum_t datum_out;

      reader = avro_reader_memory (buf, sizeof (buf));
      if (!reader)
	{
	  assert (0 && "Can't create a memory reader");
	}
      writer = avro_writer_memory (buf, sizeof (buf));
      if (!writer)
	{
	  assert (0 && "Can't create a memory writer");
	}
      if (avro_write_data (writer, writer_schema, datum_in))
	{
	  assert (0 && "Can't write string");
	}
      if (avro_read_data (reader, writer_schema, NULL, &datum_out))
	{
	  assert (0 && "Can't read string");
	}
      if (!avro_datum_equal (datum_in, datum_out))
	{
	  assert (0 && "String didn't survive encoding/decoding");
	}
      avro_datum_decref (datum_in);
      avro_datum_decref (datum_out);
      avro_reader_free (reader);
      avro_writer_free (writer);
    }
  return 0;
}

static int
test_bytes (void)
{
  char bytes[] = { 0xDE, 0xAD, 0xBE, 0xEF };
  avro_reader_t reader = avro_reader_memory (buf, sizeof (buf));
  avro_writer_t writer = avro_writer_memory (buf, sizeof (buf));
  avro_schema_t writer_schema = avro_schema_bytes ();
  avro_datum_t datum_in = avro_bytes (bytes, sizeof (bytes));
  avro_datum_t datum_out;

  if (avro_write_data (writer, writer_schema, datum_in))
    {
      assert (0 && "Unable to write bytes");
    }
  if (avro_read_data (reader, writer_schema, NULL, &datum_out))
    {
      assert (0 && "Unable to read bytes");
    }
  if (!avro_datum_equal (datum_in, datum_out))
    {
      assert (0 && "Byte did not encode/decode correctly");
    }
  avro_datum_decref (datum_in);
  avro_datum_decref (datum_out);
  avro_reader_free (reader);
  avro_writer_free (writer);
  return 0;
}

static int
test_int_long (int long_test)
{
  int i;
  for (i = 0; i < 100; i++)
    {
      avro_reader_t reader = avro_reader_memory (buf, sizeof (buf));
      avro_writer_t writer = avro_writer_memory (buf, sizeof (buf));
      avro_schema_t writer_schema =
	long_test ? avro_schema_long () : avro_schema_int ();
      avro_datum_t datum_in = long_test ? avro_long (rand ()) :
	avro_int (rand ());
      avro_datum_t datum_out;

      if (avro_write_data (writer, writer_schema, datum_in))
	{
	  assert (0 && "Unable to write int/long");
	}
      if (avro_read_data (reader, writer_schema, NULL, &datum_out))
	{
	  assert (0 && "Unable to read int/long");
	}
      if (!avro_datum_equal (datum_in, datum_out))
	{
	  assert (0 && "Unable to encode/decode int/long");
	}
      avro_datum_decref (datum_in);
      avro_datum_decref (datum_out);
      avro_reader_free (reader);
      avro_writer_free (writer);
    }
  return 0;
}

static int
test_int (void)
{
  return test_int_long (0);
}

static int
test_long (void)
{
  return test_int_long (1);
}

static
test_float_double (int double_test)
{
  int i;

  for (i = 0; i < 100; i++)
    {
      avro_reader_t reader = avro_reader_memory (buf, sizeof (buf));
      avro_writer_t writer = avro_writer_memory (buf, sizeof (buf));
      avro_schema_t schema =
	double_test ? avro_schema_double () : avro_schema_float ();
      avro_datum_t datum_in =
	double_test ? avro_double ((double) (rand ())) :
	avro_float ((float) (rand ()));
      avro_datum_t datum_out;

      if (avro_write_data (writer, schema, datum_in))
	{
	  assert (0 && "Unable to write float/double");
	}
      if (avro_read_data (reader, schema, NULL, &datum_out))
	{
	  assert (0 && "Unable to read float/double");
	}
      if (!avro_datum_equal (datum_in, datum_out))
	{
	  assert (0 && "Unable to encode/decode float/double");
	}

      avro_datum_decref (datum_in);
      avro_datum_decref (datum_out);
      avro_reader_free (reader);
      avro_writer_free (writer);
    }
  return 0;
}

static int
test_float (void)
{
  return test_float_double (0);
}

static int
test_double (void)
{
  return test_float_double (1);
}

static int
test_boolean (void)
{
  int i;
  for (i = 0; i < 1000; i++)
    {
      avro_reader_t reader = avro_reader_memory (buf, sizeof (buf));
      avro_writer_t writer = avro_writer_memory (buf, sizeof (buf));
      avro_schema_t schema = avro_schema_boolean ();
      avro_datum_t datum_in = avro_boolean (rand () % 2);
      avro_datum_t datum_out;

      if (avro_write_data (writer, schema, datum_in))
	{
	  assert (0 && "Unable to write boolean");
	}
      if (avro_read_data (reader, schema, schema, &datum_out))
	{
	  assert (0 && "Unable to read boolean");
	}
      if (!avro_datum_equal (datum_in, datum_out))
	{
	  assert (0 && "Unable to encode/decode boolean");
	}

      avro_reader_free (reader);
      avro_writer_free (writer);
    }
  return 0;
}

static int
test_null (void)
{
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
  /* TODO */
  return 0;
}

int
test_map (void)
{
  /* TODO */
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
