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
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>

#include "avro.h"
#include "avro_private.h"
#include "apr_strings.h"

apr_pool_t *pool;

static void
run_tests (char *dirpath, int should_pass)
{
  char *jsontext;
  apr_size_t jsonlen;
  struct avro_value *value;
  DIR *dir;
  struct dirent *dent;
  char *filepath;

  dir = opendir (dirpath);
  if (dir == NULL)
    {
      fprintf (stderr, "Unable to open '%s'\n", dirpath);
      exit (EXIT_FAILURE);
    }
  do
    {
      dent = readdir (dir);
      if (dent && dent->d_name[0] != '.')
	{
	  filepath = apr_pstrcat (pool, dirpath, "/", dent->d_name, NULL);
	  fprintf (stderr, "TEST %s...", filepath);
	  jsontext = avro_util_file_read_full (pool, filepath, &jsonlen);
	  if (!jsontext)
	    {
	      fprintf (stderr, "Can't read the file\n");
	      exit (EXIT_FAILURE);
	    }

	  value = avro_value_create (pool, jsontext, jsonlen);
	  if (value && should_pass)
	    {
	      avro_value_print_info (value, stderr);
	    }
	  else if (!value && !should_pass)
	    {
	      /* failure expected */
	    }
	  else
	    {
	      exit (EXIT_FAILURE);
	    }
	  fprintf (stderr, "ok!\n");
	}
    }
  while (dent != NULL);
}

int
main (int argc, char *argv[])
{
  char *srcdir = getenv ("srcdir");
  char *path;

  if (!srcdir)
    {
      srcdir = ".";
    }

  avro_initialize ();
  apr_pool_create (&pool, NULL);

  /* Run the tests that should pass */
  path = apr_pstrcat (pool, srcdir, "/tests/schema_tests/pass", NULL);
  fprintf (stderr, "RUNNING %s\n", path);
  run_tests (path, 1);
  path = apr_pstrcat (pool, srcdir, "/tests/schema_tests/fail", NULL);
  fprintf (stderr, "RUNNING %s\n", path);
  run_tests (path, 0);

  apr_pool_destroy (pool);
  return EXIT_SUCCESS;
}
