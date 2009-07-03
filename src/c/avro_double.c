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

avro_status_t
avro_float (AVRO * avro, float *fp)
{
  if (!avro || !fp)
    {
      return AVRO_FAILURE;
    }
  switch (avro->a_op)
    {
    case AVRO_ENCODE:
      return avro_putint32_raw (avro, *(int32_t *) fp);
    case AVRO_DECODE:
      return avro_getint32_raw (avro, (int32_t *) fp);
    default:
      return AVRO_FAILURE;
    }
  return AVRO_OK;
}

avro_status_t
avro_double (AVRO * avro, double *dp)
{
  if (!avro || !dp)
    {
      return AVRO_FAILURE;
    }
  switch (avro->a_op)
    {
    case AVRO_ENCODE:
      return avro_putint64_raw (avro, *(int64_t *) dp);
    case AVRO_DECODE:
      return avro_getint64_raw (avro, (int64_t *) dp);
    default:
      return AVRO_FAILURE;
    }
  return AVRO_OK;
}
