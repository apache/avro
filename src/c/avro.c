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

#include "avro.h"
#include "avro_private.h"
#include <locale.h>

avro_status_t
avro_value_type (avro_value value, avro_type_t * type)
{
  if (!value || !type)
    {
      return AVRO_FAILURE;
    }
  *type = value->type;
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
