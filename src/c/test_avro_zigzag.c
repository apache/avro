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

#include <apr.h>
#include <apr_pools.h>
#include <apr_buckets.h>
#include <apr_file_io.h>
#include <stdlib.h>
#include "avro.h"
#include "error.h"

struct test_case
{
  int64_t value;
  unsigned len;
  uint8_t bytes[16];
};
typedef struct test_case test_case;

static const test_case test_cases[] = {
  {0, 1, {0x0}},
  {-1, 1, {0x1}},
  {1, 1, {0x2}},
  {-2, 1, {0x3}},
  {2, 1, {0x4}},
  {-64, 1, {0x7f}},
  {64, 2, {0x80, 0x01}},
  {-65, 2, {0x81, 0x01}},
  {65, 2, {0x82, 0x01}}
};

int
main (void)
{
  apr_pool_t *pool;
  AVRO avro_in, avro_out;
  avro_status_t avro_status;
  char buf[1024];
  int64_t value_in, value_out;
  int i, j;

  apr_initialize ();
  atexit (apr_terminate);

  apr_pool_create (&pool, NULL);

  for (i = 0; i < sizeof (test_cases) / sizeof (test_cases[0]); i++)
    {
      const test_case *tc = test_cases + i;
      avro_status =
	avro_create_memory (&avro_in, pool, buf, sizeof (buf), AVRO_ENCODE);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to create AVRO encoder");
	}

      value_in = (int64_t) tc->value;
      avro_status = avro_int64 (&avro_in, &value_in);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to encode long value=%lld", value_in);
	}

      if (avro_in.used != tc->len)
	{
	  err_quit ("Long value=%lld encoded to the wrong length %d != %d",
		    value_in, avro_in.used, tc->len);
	}
      for (j = 0; j < tc->len; j++)
	{
	  if ((uint8_t) avro_in.addr[j] != tc->bytes[j])
	    {
	      err_quit ("Invalid byte %d encoding the value=%lld", j,
			value_in);
	    }
	}

      avro_status =
	avro_create_memory (&avro_out, pool, buf, sizeof (buf), AVRO_DECODE);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to create AVRO decoder");
	}

      avro_status = avro_int64 (&avro_out, &value_out);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to decode AVRO long");
	}

      if (value_out != value_in)
	{
	  err_msg ("Decoder error %lld decoded as %lld", value_in, value_out);
	}

    }

  apr_pool_destroy (pool);
  return EXIT_SUCCESS;
}
