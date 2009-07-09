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
#include <time.h>
#include "avro.h"
#include "error.h"

int
main (void)
{
  apr_pool_t *pool;
  AVRO avro_in, avro_out;
  avro_status_t avro_status;
  char buf[1024];
  char in[10];
  char *input = &in;
  char *output;
  int i, j;
  int64_t len_in, len_out;

  apr_initialize ();
  atexit (apr_terminate);

  srand (time (NULL));

  apr_pool_create (&pool, NULL);

  for (i = 0; i < 10; i++)
    {

      avro_status =
	avro_create_memory (&avro_in, pool, buf, sizeof (buf), AVRO_ENCODE);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to create AVRO encoder");
	}

      for (j = 0; j < sizeof (input); j++)
	{
	  input[j] = (char) rand ();
	}

      len_in = sizeof (in);
      avro_status = avro_bytes (&avro_in, &input, &len_in, -1);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to encode bytes");
	}

      avro_status =
	avro_create_memory (&avro_out, pool, buf, sizeof (buf), AVRO_DECODE);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to create AVRO decoder");
	}

      avro_status = avro_bytes (&avro_out, &output, &len_out, -1);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to decode bytes");
	}

      if (len_out != len_in)
	{
	  err_quit ("Error decoding bytes out len=%d != in len=%d", len_out,
		    len_in);
	}

      if (memcmp (input, output, sizeof (input)))
	{
	  err_quit ("Output bytes do not equal input bytes");
	  avro_dump_memory (&avro_in, stderr);
	  avro_dump_memory (&avro_out, stderr);
	}

      apr_pool_clear (pool);
    }
  apr_pool_destroy (pool);

  return EXIT_SUCCESS;
}
