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

#include "avro.h"
#include "json.h"

#define TRACING 1

static struct test_dir
{
  char *path;
  int shouldFail;
} test_dirs[] =
{
  {
  "json/pass", 0},
  {
  "json/fail", 1}
};

#define NUM_TEST_DIRS (sizeof(test_dirs)/sizeof(test_dirs[0]))

FILE *trace = NULL;

int
main (int argc, char *argv[], char *envp[])
{
  FILE *file;
  DIR *dir;
  struct dirent *dent;
  int i, processed;
  char buf[4096];
  apr_pool_t *pool;
  JSON_value *value;
  char path[256];
  char *srcdir = getenv ("srcdir");
  if (!srcdir)
    {
      srcdir = ".";
    }

  avro_initialize ();

  apr_pool_create (&pool, NULL);

#if TRACING
  trace = fopen ("trace.txt", "w");
  if (trace == NULL)
    {
      return EXIT_FAILURE;
    }
#endif

  for (i = 0; i < NUM_TEST_DIRS; i++)
    {
      struct test_dir *td = test_dirs + i;
      snprintf (path, sizeof (path), "%s/%s", srcdir, td->path);
      dir = opendir (path);
      if (dir == NULL)
	{
	  fprintf (stderr, "Unable to open '%s'\n", path);
	  return EXIT_FAILURE;
	}
      dent = readdir (dir);
      while (dent != NULL)
	{
	  if (dent->d_name[0] != '.')
	    {
	      char filepath[256];
	      snprintf (filepath, sizeof (filepath), "%s/%s", path,
			dent->d_name);

	      file = fopen (filepath, "r");
	      if (file == NULL)
		{
		  fprintf (stderr, "Can't open file");
		  return EXIT_FAILURE;
		}

	      processed = 0;
	      while (!feof (file))
		{
		  if (fread (buf + processed, 1, 1, file) == 1)
		    {
		      processed++;
		    }
		}
	      buf[processed] = '\0';
	      fclose (file);

	      value = JSON_parse (pool, buf, processed);
	      if (!value)
		{
		  if (!td->shouldFail)
		    {
		      return EXIT_FAILURE;
		    }
		}
	      else
		{
		  if (td->shouldFail)
		    {
		      return EXIT_FAILURE;
		    }
		}
	      /* JSONParserTrace (trace, buf); */
	      /* JSON_print (stderr, value); */
	    }
	  dent = readdir (dir);
	}
      closedir (dir);
    }
  return EXIT_SUCCESS;
}
