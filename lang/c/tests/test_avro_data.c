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
#include "dump.h"

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
  int status;
  const char *strings[] = { "Four score and seven years ago",
    "our father brought forth on this continent",
    "a new nation", "conceived in Liberty",
    "and dedicated to the proposition that all men are created equal."
  };
  for (i = 0; i < sizeof (strings) / sizeof (strings[0]); i++)
    {
      avro_binary_encode_to_memory (buf, sizeof (buf), avro_schema_string (),
				    avro_string (strings[i]));
    }
  return 0;
}

static int
test_bytes (void)
{
  int status;
  char bytes[] = { 0xDE, 0xAD, 0xBE, 0xEF };
  char *read_bytes;

#if 0
  status = avro_write_bytes (encoder, bytes, sizeof (bytes));
  if (status != 0)
    {
      return status;
    }
  status = avro_read_bytes (decoder, pool, (void *) &read_bytes, &len);
  if (status != 0)
    {
      return status;
    }
  if (len != sizeof (bytes))
    {
      return 1;
    }
  if (memcmp (bytes, read_bytes, len))
    {
      return 1;
    }
#endif
  return 0;
}

static int
test_int (void)
{
  return 0;
}

static int
test_long (void)
{
  return 0;
}

static int
test_float (void)
{
  int status;
  float input, output;
  int i;
  int const num_rand_tests = 25;

#if 0
  for (i = 0; i < num_rand_tests; i++)
    {
      input = random () * 1.1;
      status = avro_write_float (encoder, input);
      if (status != 0)
	{
	  return status;
	}
      status = avro_read_float (decoder, &output);
      if (status != 0)
	{
	  return status;
	}
      if (input != output)
	{
	  fprintf (stderr, "%f != %f\n", input, output);
	  return 1;
	}
    }
#endif
  return 0;
}

static int
test_double (void)
{
  int status;
  double input, output;
  int i;
  int const num_rand_tests = 25;

#if 0
  for (i = 0; i < num_rand_tests; i++)
    {
      input = random () * 1.1;
      status = avro_write_double (encoder, input);
      if (status != 0)
	{
	  return status;
	}
      status = avro_read_double (decoder, &output);
      if (status != 0)
	{
	  return status;
	}
      if (input != output)
	{
	  fprintf (stderr, "%f != %f\n", input, output);
	  return 1;
	}
    }
#endif
  return 0;
}

static int
test_boolean (void)
{
  int status;
  int i, bool_in, bool_out;

#if 0
  for (i = 0; i < 2; i++)
    {
      bool_in = i;
      status = avro_write_bool (encoder, bool_in);
      if (status != 0)
	{
	  return status;
	}
      status = avro_read_bool (decoder, &bool_out);
      if (status != 0)
	{
	  return status;
	}
      if (bool_in != bool_out)
	{
	  fprintf (stderr, "%d != %d\n", bool_in, bool_out);
	  return 1;
	}
    }
#endif
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
  return 0;
}

int
test_enum (void)
{
  return 0;
}

int
test_array (void)
{
  return 0;
}

int
test_map (void)
{
  return 0;
}

int
test_union (void)
{
  return 0;
}

int
test_fixed (void)
{
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
