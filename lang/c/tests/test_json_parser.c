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
#include <string.h>
#include <sys/types.h>
#include <dirent.h>

#include <apr.h>
#include <apr_pools.h>
#include <apr_strings.h>

#include "avro.h"
#include "avro_private.h"
#include "json/json.h"

apr_pool_t *pool;

void
run_tests (char *dirpath, int should_pass)
{
  FILE *file;
  DIR *dir;
  struct dirent *dent;
  JSON_value *value;
  char *filepath;
  char *text;
  apr_size_t len;

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
	  file = fopen (filepath, "r");
	  if (file == NULL)
	    {
	      fprintf (stderr, "Can't open file %s", filepath);
	      exit (EXIT_FAILURE);
	    }

	  text = avro_util_file_read_full (pool, filepath, &len);
	  if (!text)
	    {
	      fprintf (stderr, "Can't read the file %s\n", filepath);
	      exit (EXIT_FAILURE);
	    }

	  value = JSON_parse (pool, text, len);
	  if (!value && should_pass)
	    {
	      exit (EXIT_FAILURE);
	    }
	  else if (value && !should_pass)
	    {
	      exit (EXIT_FAILURE);
	    }
	  else if (value)
	    {
#if TRACING
	      JSONParserTrace (trace, buf);
#endif
/*
	      JSON_print (stderr, value);
*/
	    }
	}
    }
  while (dent != NULL);
  closedir (dir);
}

int
main (int argc, char *argv[], char *envp[])
{
  char *dirpath;
  char *srcdir = getenv ("srcdir");
  if (!srcdir)
    {
      srcdir = ".";
    }

#if TRACING
  trace = fopen ("trace.txt", "w");
  if (trace == NULL)
    {
      return EXIT_FAILURE;
    }
#endif

  avro_initialize ();

  apr_pool_create (&pool, NULL);

  dirpath = apr_pstrcat (pool, srcdir, "/tests/json_tests/pass", NULL);
  run_tests (dirpath, 1);
  dirpath = apr_pstrcat (pool, srcdir, "/tests/json_tests/fail", NULL);
  run_tests (dirpath, 0);

  return EXIT_SUCCESS;
}
