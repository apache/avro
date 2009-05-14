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
  long long_in, long_out;
  char *bytes_in, *bytes_out;
  int i;
  int64_t len_in, len_out;

  apr_initialize ();
  atexit (apr_terminate);

  srand (time (NULL));

  for (i = 0; i < 10; i++)
    {
      apr_pool_create (&pool, NULL);
      avro_status =
	avro_create_memory (&avro_in, pool, buf, sizeof (buf), AVRO_ENCODE);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to create AVRO encoder");
	}

      long_in = rand ();
      bytes_in = (char *) &long_in;
      len_in = sizeof (bytes_in);
      avro_status = avro_bytes (&avro_in, &bytes_in, &len_in, -1);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to encode bytes value=%s", long_in);
	}

      avro_status =
	avro_create_memory (&avro_out, pool, buf, sizeof (buf), AVRO_DECODE);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to create AVRO decoder");
	}

      avro_status = avro_bytes (&avro_out, &bytes_out, &len_out, -1);
      if (avro_status != AVRO_OK)
	{
	  err_quit ("Unable to decode AVRO long");
	}

      if (len_out != len_in)
	{
	  err_quit ("Error decoding bytes out len=%d != in len=%d", len_out,
		    len_in);
	}
      long_out = *((long *) bytes_out);
      if (long_out != long_in)
	{
	  avro_dump_memory (&avro_in, stderr);
	  avro_dump_memory (&avro_out, stderr);
	  err_quit ("Error decoding bytes long_in=%d != long_out = %d",
		    long_in, long_out);
	}
      apr_pool_destroy (pool);
    }

  return EXIT_SUCCESS;
}
