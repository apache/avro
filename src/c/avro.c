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

#include <stdlib.h>
#include <locale.h>
#include "avro.h"

avro_status_t
avro_null (void)
{
  /* Do nothing */
  return AVRO_OK;
}

avro_status_t
avro_bool (AVRO * avro, bool_t * bp)
{
  avro_status_t status;
  char b;
  if (!avro || !bp)
    {
      return AVRO_FAILURE;
    }
  switch (avro->a_op)
    {
    case AVRO_ENCODE:
      {
	b = *bp ? 1 : 0;
	return AVRO_PUTBYTES (avro, &b, 1);
      }
    case AVRO_DECODE:
      {
	status = AVRO_GETBYTES (avro, &b, 1);
	CHECK_ERROR (status);
	*bp = b ? 1 : 0;
      }
    default:
      return AVRO_FAILURE;
    }
  return AVRO_OK;
}

avro_status_t
avro_initialize (void)
{
  apr_initialize ();
  atexit (apr_terminate);

  /* Set the locale to UTF-8 */
  if (!setlocale (LC_CTYPE, "en_US.UTF-8"))
    {
      return AVRO_FAILURE;
    }

  return AVRO_OK;
}
