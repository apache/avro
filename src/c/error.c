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
#include <stdarg.h>		/* ANSI C header file */
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <syslog.h>		/* for syslog() */
#include "error.h"

int daemon_proc = 0;		/* set nonzero by daemon_init() */

static void err_doit (int, int, const char *, va_list);

/* Nonfatal error related to a system call.
 * Print a message and return. */

void
err_ret (const char *fmt, ...)
{
  va_list ap;

  va_start (ap, fmt);
  err_doit (1, LOG_INFO, fmt, ap);
  va_end (ap);
  return;
}

/* Fatal error related to a system call.
 * Print a message and terminate. */

void
err_sys (const char *fmt, ...)
{
  va_list ap;

  va_start (ap, fmt);
  err_doit (1, LOG_ERR, fmt, ap);
  va_end (ap);
  exit (EXIT_FAILURE);
}

/* Fatal error related to a system call.
 * Print a message, dump core, and terminate. */

void
err_dump (const char *fmt, ...)
{
  va_list ap;

  va_start (ap, fmt);
  err_doit (1, LOG_ERR, fmt, ap);
  va_end (ap);
  abort ();			/* dump core and terminate */
  exit (EXIT_FAILURE);		/* shouldn't get here */
}

/* Nonfatal error unrelated to a system call.
 * Print a message and return. */

void
err_msg (const char *fmt, ...)
{
  va_list ap;

  va_start (ap, fmt);
  err_doit (0, LOG_INFO, fmt, ap);
  va_end (ap);
  return;
}

/* Fatal error unrelated to a system call.
 * Print a message and terminate. */

void
err_quit (const char *fmt, ...)
{
  va_list ap;

  va_start (ap, fmt);
  err_doit (0, LOG_ERR, fmt, ap);
  va_end (ap);
  exit (EXIT_FAILURE);
}

/* Print a message and return to caller.
 * Caller specifies "errnoflag" and "level". */

static void
err_doit (int errnoflag, int level, const char *fmt, va_list ap)
{
  int errno_save, n;
  char buf[1024];

  errno_save = errno;		/* value caller might want printed */
  vsnprintf (buf, sizeof (buf), fmt, ap);	/* this is safe */
  n = strlen (buf);
  if (errnoflag)
    snprintf (buf + n, sizeof (buf) - n, ": %s", strerror (errno_save));
  strcat (buf, "\n");

  if (daemon_proc)
    {
      syslog (level, buf);
    }
  else
    {
      fflush (stdout);		/* in case stdout and stderr are the same */
      fputs (buf, stderr);
      fflush (stderr);
    }
  return;
}
