#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>

#include <apr.h>
#include <apr_pools.h>

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
main (int argc, char *argv[])
{
  FILE *file;
  DIR *dir;
  struct dirent *dent;
  int i, fd, processed;
  char buf[1024];
  apr_pool_t *pool;
  JSON_value *value;

  apr_initialize ();
  atexit (apr_terminate);

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
      dir = opendir (td->path);
      if (dir == NULL)
	{
	  fprintf (stderr, "Unable to open '%s'\n", td->path);
	  return EXIT_FAILURE;
	}
      dent = readdir (dir);
      while (dent != NULL)
	{
	  if (dent->d_name[0] != '.')
	    {
	      char filepath[256];
	      snprintf (filepath, sizeof (filepath), "%s/%s", td->path,
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
	      JSONParserTrace (trace, buf);
	      if (!value && !td->shouldFail)
		{
		  return EXIT_FAILURE;
		}
	      /* JSON_print (stderr, value); */
	    }
	  dent = readdir (dir);
	}
      closedir (dir);
    }
  return EXIT_SUCCESS;
}
