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
#include <string.h>
#include "dump.h"

/**
@todo We could likely speedup file i/o by reading ahead
*/
static avro_status_t
file_get_bytes (struct AVRO *avro, caddr_t addr, const int64_t len)
{
  apr_status_t apr_status;
  if ((avro->len - avro->used) < len || len < 0)
    {
      return AVRO_FAILURE;
    }
  if (len > 0)
    {
      apr_status =
	apr_file_read_full (avro->file, addr, (apr_size_t) len, NULL);
      if (apr_status != APR_SUCCESS)
	{
	  return AVRO_FAILURE;
	}
    }
  return AVRO_OK;
}

static avro_status_t
file_put_bytes (struct AVRO *avro, const char *addr, const int64_t len)
{
  apr_status_t apr_status;
  if ((avro->len - avro->used) < len || len < 0)
    {
      return AVRO_FAILURE;
    }
  if (len > 0)
    {
      apr_status =
	apr_file_write_full (avro->file, addr, (apr_size_t) len, NULL);
      if (apr_status != APR_SUCCESS)
	{
	  return AVRO_FAILURE;
	}
    }
  return AVRO_OK;
}

static const struct avro_ops avro_file_ops = {
  file_get_bytes,
  file_put_bytes
};

avro_status_t
avro_create_file (AVRO * avro, apr_pool_t * pool, apr_file_t * file,
		  avro_op op)
{
  if (!avro || !pool || !file)
    {
      return AVRO_FAILURE;
    }
  avro->pool = pool;
  avro->a_op = op;
  avro->a_ops = (struct avro_ops *) &avro_file_ops;
  avro->file = file;
  return AVRO_OK;
}
