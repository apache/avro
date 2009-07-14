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

int
main (void)
{
  apr_pool_t *pool;
  AVRO avro_in, avro_out;
  avro_status_t avro_status;
  char buf[1024];
  char *str_in, *str_out;
  int i, len;
  char *test_strings[] = {
    "This",
    "Is",
    "A",
    "Test"
  };

  avro_initialize ();

  for (i = 0; i < sizeof (test_strings) / sizeof (test_strings[0]); i++)
    {
      char *test_string = test_strings[i];

      apr_pool_create (&pool, NULL);
      avro_status =
	avro_create_memory (&avro_in, pool, buf, sizeof (buf), AVRO_ENCODE);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to create AVRO encoder");
	}

      str_in = test_string;
      avro_status = avro_string (&avro_in, &str_in, -1);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to encode string value=%s", str_in);
	}

      avro_status =
	avro_create_memory (&avro_out, pool, buf, sizeof (buf), AVRO_DECODE);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to create AVRO decoder");
	}

      avro_status = avro_string (&avro_out, &str_out, -1);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to decode AVRO long");
	}

      len = strlen (str_in);
      if (len != strlen (str_out))
	{
	  err_quit ("Error decoding string");
	}
      if (memcmp (str_in, str_out, len))
	{
	  err_quit ("Error decoding string");
	}
      apr_pool_destroy (pool);

    }

  return EXIT_SUCCESS;
}
