#include "avro_private.h"
#include <apr_strings.h>
#include <stdlib.h>

apr_pool_t *pool;

/*
TODO:

We still need to create a File Object container to finish this test.
For now, we're only validating that we can parse the schema.

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
      fprintf (stderr, "Failed to parse the JSON in file %s\n", path);
      return EXIT_FAILURE;
    }
  value = avro_value_create (pool, jsontext, jsonlen);
  if (!value)
    {
      fprintf (stderr, "Unable to parse the Avro schema in file %s\n", path);
      return EXIT_FAILURE;
    }

  path =
    apr_pstrcat (pool, srcdir, "/../../build/test/data-files/test.py.avro",
		 NULL);
  if (!path)
    {
      return EXIT_FAILURE;
    }
  reader =
    avro_reader_file_container_create (pool, path, APR_READ, APR_OS_DEFAULT);
  if (!reader)
    {
      fprintf (stderr, "Failed to open data file %s\n", path);
      return EXIT_FAILURE;
    }

  avro_value_read_data (value, reader);
  avro_value_print_info (value, stderr);
  apr_pool_destroy (pool);
  return EXIT_SUCCESS;
}
