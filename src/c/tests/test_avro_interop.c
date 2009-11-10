#include "avro_private.h"
#include <apr_strings.h>
#include <stdlib.h>

apr_pool_t *pool;

/*
$ ant generate-test-data
needs to be called to make sure that test data is created.

The test files are then in ./build/test/data-files 
*/

int
main (void)
{
  char *srcdir = getenv ("srcdir");
  char *path;
  char *jsontext;
  apr_size_t jsonlen;
  struct avro_value *value;
  struct avro_reader *reader;
  char *suffixes[] = { "py", "java" };
  char *suffix;
  int i;

  if (!srcdir)
    {
      srcdir = ".";
    }

  avro_initialize ();
  apr_pool_create (&pool, NULL);

  path = apr_pstrcat (pool, srcdir, "/../test/schemata/interop.avsc", NULL);
  if (!path)
    {
      fprintf (stderr, "Couldn't allocate memory for file path\n");
      return EXIT_FAILURE;
    }
  jsontext = avro_util_file_read_full (pool, path, &jsonlen);
  if (!jsontext)
    {
      fprintf (stderr,
	       "Couldn't find file: %s. Can't run interop test out of tree\n",
	       path);
      return EXIT_SUCCESS;
    }
  value = avro_value_create (pool, jsontext, jsonlen);
  if (!value)
    {
      fprintf (stderr, "Unable to parse the Avro schema in file %s\n", path);
      return EXIT_FAILURE;
    }

  for (i = 0; i < sizeof (suffixes) / sizeof (suffixes[0]); i++)
    {
      suffix = suffixes[i];

      path =
	apr_pstrcat (pool, srcdir, "/../../build/test/data-files/test.",
		     suffix, ".avro", NULL);
      if (!path)
	{
	  return EXIT_FAILURE;
	}
      fprintf (stderr, "Running test on %s...\n", path);
      reader =
	avro_reader_file_container_create (pool, path, APR_READ,
					   APR_OS_DEFAULT);
      if (!reader)
	{
	  fprintf (stderr, "Failed to open data file %s\n", path);
	  return EXIT_FAILURE;
	}
      avro_value_read_data (value, reader);
      avro_value_print_info (value, stderr);
    }
  apr_pool_destroy (pool);
  return EXIT_SUCCESS;
}
