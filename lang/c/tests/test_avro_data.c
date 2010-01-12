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
#include <time.h>
#include <string.h>
#include "avro_private.h"
#include "util/dump.h"

char buf[1024];

typedef avro_status_t (*avro_test) (apr_pool_t * pool,
				    avro_io_reader * reader,
				    avro_io_writer * writer);
struct test_case
{
  avro_long_t value;
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

static avro_status_t
test_string (apr_pool_t * pool, avro_io_reader * reader,
	     avro_io_writer * writer)
{
  int i;
  avro_status_t status;
  avro_string_t strings[] = { L"\u0123\u4567\u89AB\uCDEF\uabcd\uef4A",
    L"\\\"\uCAFE\uBABE\uAB98\uFCDE\ubcda\uef4A\b\f\n\r\t`1~!@#$%^&*()_+-=[]{}|;:',./<>?",
    L"foo", L"bar", L"baz"
  };
  avro_string_t read_str;

  for (i = 0; i < sizeof (strings) / sizeof (strings[0]); i++)
    {
      avro_string_t str = strings[i];
      status = avro_write_string (writer, pool, str);
      if (status != AVRO_OK)
	{
	  return status;
	}
      status = avro_read_string (reader, pool, &read_str);
      if (status != AVRO_OK)
	{
	  return status;
	}
      if (wcscmp (str, read_str))
	{
	  /* string didn't survive encoding/decoding */
	  fprintf (stderr, "%ls != %ls\n", str, read_str);
	  return AVRO_FAILURE;
	}
      else
	{
	  fprintf (stderr, "%ls = %ls\n", str, read_str);
	}
    }
  return AVRO_OK;
}

static avro_status_t
test_bytes (apr_pool_t * pool, avro_io_reader * reader,
	    avro_io_writer * writer)
{
  avro_status_t status;
  char bytes[] = { 0xDE, 0xAD, 0xBE, 0xEF };
  char *read_bytes;
  avro_long_t len;

  status = avro_write_bytes (writer, bytes, sizeof (bytes));
  if (status != AVRO_OK)
    {
      return status;
    }
  status = avro_read_bytes (reader, pool, (void *) &read_bytes, &len);
  if (status != AVRO_OK)
    {
      return status;
    }
  if (len != sizeof (bytes))
    {
      return AVRO_FAILURE;
    }
  if (memcmp (bytes, read_bytes, len))
    {
      return AVRO_FAILURE;
    }
  return AVRO_OK;
}

static avro_status_t
test_int_long (apr_pool_t * pool, avro_io_reader * reader,
	       avro_io_writer * writer, int is_long)
{
  apr_pool_t *subpool;
  avro_status_t status;
  int i;
  const int num_rand_tests = 100;
  uint8_t my_buf[64];
  avro_io_writer *mywriter;
  long rand_val;

  apr_pool_create (&subpool, pool);
  for (i = 0; i < sizeof (test_cases) / sizeof (test_cases[0]); i++)
    {
      struct test_case *test = test_cases + i;
      avro_int_t int_test_val;
      avro_long_t long_test_val;

      mywriter =
	avro_io_writer_from_memory (subpool, (void *) my_buf,
				    sizeof (my_buf));
      if (!mywriter)
	{
	  return AVRO_FAILURE;
	}

      if (is_long)
	{
	  long_test_val = test->value;
	  DEBUG (fprintf
		 (stderr, "Running long test val=%lld\n", long_test_val));
	  status = avro_write_long (mywriter, &long_test_val);
	}
      else
	{
	  int_test_val = (avro_int_t) test->value;
	  DEBUG (fprintf (stderr, "Running int test val=%d\n", int_test_val));
	  status = avro_write_int (mywriter, &int_test_val);
	}
      if (status != AVRO_OK)
	{
	  return status;
	}
      if (memcmp ((void *) my_buf, (void *) test->bytes, test->len))
	{
	  fprintf (stderr, "Received bytes");
	  dump (stderr, (void *) my_buf, test->len);
	  fprintf (stderr, "Expected bytes");
	  dump (stderr, (void *) test->bytes, test->len);
	  return AVRO_FAILURE;
	}
      apr_pool_clear (subpool);
    }

  for (i = 0; i < num_rand_tests; i++)
    {
      avro_long_t long_in, long_out;
      avro_int_t int_in, int_out;
      rand_val = random ();
      if (is_long)
	{
	  avro_long_t a, b;
	  a = random ();
	  b = random ();
	  long_in = (a << 32) | b;
	}
      else
	{
	  int_in = random ();
	}
      if (rand_val % 2)
	{
	  long_in -= (long_in * 2);
	  int_in -= (int_in * 2);
	}
      if (is_long)
	{
	  DEBUG (fprintf
		 (stderr, "Running long test %d val=%lld\n", i, long_in));
	  status = avro_write_long (writer, &long_in);
	  if (status != AVRO_OK)
	    {
	      fprintf (stderr, "Unable to write long %lld\n", long_in);
	      return status;
	    }
	  status = avro_read_long (reader, &long_out);
	  if (status != AVRO_OK)
	    {
	      fprintf (stderr, "Unable to read long %lld\n", long_in);
	      return status;
	    }
	  if (long_in != long_out)
	    {
	      fprintf (stderr, "%lld != %lld\n", long_in, long_out);
	      return AVRO_FAILURE;
	    }
	}
      else
	{
	  DEBUG (fprintf (stderr, "Running int test %d val=%d\n", i, int_in));
	  status = avro_write_int (writer, &int_in);
	  if (status != AVRO_OK)
	    {
	      fprintf (stderr, "Unable to write int %d\n", int_in);
	      return status;
	    }
	  status = avro_read_int (reader, &int_out);
	  if (status != AVRO_OK)
	    {
	      fprintf (stderr, "Unable to read int\n");
	      return status;
	    }
	  if (int_in != int_out)
	    {
	      fprintf (stderr, "%d != %d\n", int_in, int_out);
	      return AVRO_FAILURE;
	    }
	}
    }
  return AVRO_OK;
}

static avro_status_t
test_int (apr_pool_t * pool, avro_io_reader * reader, avro_io_writer * writer)
{
  return test_int_long (pool, reader, writer, 0);
}

static avro_status_t
test_long (apr_pool_t * pool, avro_io_reader * reader,
	   avro_io_writer * writer)
{
  return test_int_long (pool, reader, writer, 1);
}

static avro_status_t
test_float (apr_pool_t * pool, avro_io_reader * reader,
	    avro_io_writer * writer)
{
  avro_status_t status;
  float input, output;
  int i;
  int const num_rand_tests = 25;

  for (i = 0; i < num_rand_tests; i++)
    {
      input = random () * 1.1;
      status = avro_write_float (writer, input);
      if (status != AVRO_OK)
	{
	  return status;
	}
      status = avro_read_float (reader, &output);
      if (status != AVRO_OK)
	{
	  return status;
	}
      if (input != output)
	{
	  fprintf (stderr, "%f != %f\n", input, output);
	  return AVRO_FAILURE;
	}
    }
  return AVRO_OK;
}

static avro_status_t
test_double (apr_pool_t * pool, avro_io_reader * reader,
	     avro_io_writer * writer)
{
  avro_status_t status;
  double input, output;
  int i;
  int const num_rand_tests = 25;

  for (i = 0; i < num_rand_tests; i++)
    {
      input = random () * 1.1;
      status = avro_write_double (writer, input);
      if (status != AVRO_OK)
	{
	  return status;
	}
      status = avro_read_double (reader, &output);
      if (status != AVRO_OK)
	{
	  return status;
	}
      if (input != output)
	{
	  fprintf (stderr, "%f != %f\n", input, output);
	  return AVRO_FAILURE;
	}
    }
  return AVRO_OK;
}

static avro_status_t
test_boolean (apr_pool_t * pool, avro_io_reader * reader,
	      avro_io_writer * writer)
{
  avro_status_t status;
  int i, bool_in, bool_out;

  for (i = 0; i < 2; i++)
    {
      bool_in = i;
      status = avro_write_bool (writer, bool_in);
      if (status != AVRO_OK)
	{
	  return status;
	}
      status = avro_read_bool (reader, &bool_out);
      if (status != AVRO_OK)
	{
	  return status;
	}
      if (bool_in != bool_out)
	{
	  fprintf (stderr, "%d != %d\n", bool_in, bool_out);
	  return AVRO_FAILURE;
	}
    }
  return AVRO_OK;
}

static avro_status_t
test_null (apr_pool_t * pool, avro_io_reader * reader,
	   avro_io_writer * writer)
{
  return AVRO_OK;
}

avro_status_t
test_record (apr_pool_t * pool, avro_io_reader * reader,
	     avro_io_writer * writer)
{
  return AVRO_OK;
}

avro_status_t
test_enum (apr_pool_t * pool, avro_io_reader * reader,
	   avro_io_writer * writer)
{
  return AVRO_OK;
}

avro_status_t
test_array (apr_pool_t * pool, avro_io_reader * reader,
	    avro_io_writer * writer)
{
  return AVRO_OK;
}

avro_status_t
test_map (apr_pool_t * pool, avro_io_reader * reader, avro_io_writer * writer)
{
  return AVRO_OK;
}

avro_status_t
test_union (apr_pool_t * pool, avro_io_reader * reader,
	    avro_io_writer * writer)
{
  return AVRO_OK;
}

avro_status_t
test_fixed (apr_pool_t * pool, avro_io_reader * reader,
	    avro_io_writer * writer)
{
  return AVRO_OK;
}


int
main (void)
{
  int i;
  apr_pool_t *pool, *subpool;
  avro_status_t status;
  avro_io_reader *reader;
  avro_io_writer *writer;
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

  avro_initialize ();
  apr_pool_create (&pool, NULL);
  for (i = 0; i < sizeof (tests) / sizeof (tests[0]); i++)
    {
      struct avro_tests *test = tests + i;
      apr_pool_create (&subpool, pool);
      reader = avro_io_reader_from_memory (subpool, buf, sizeof (buf));
      if (!reader)
	{
	  return AVRO_FAILURE;
	}
      writer = avro_io_writer_from_memory (subpool, buf, sizeof (buf));
      if (!writer)
	{
	  return AVRO_FAILURE;
	}
      fprintf (stderr, "Running %s tests...\n", test->name);
      status = test->func (subpool, reader, writer);
      if (status != AVRO_OK)
	{
	  fprintf (stderr, "failed!\n");
	  return EXIT_FAILURE;
	}
      fprintf (stderr, "\t... %s tests passed!\n", test->name);
    }
  apr_pool_destroy (pool);
  return EXIT_SUCCESS;
}
