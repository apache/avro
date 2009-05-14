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

  apr_initialize ();
  atexit (apr_terminate);

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
