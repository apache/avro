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

#include "avro_private.h"
#include "apr_file_io.h"

struct avro_io_file
{
  apr_file_t *file;
  struct avro_io io;
};

static avro_status_t
avro_io_file_read (struct avro_io *io, char *addr, uint64_t len)
{
  struct avro_io_file *self = container_of (io, struct avro_io_file, io);
  apr_status_t status =
    apr_file_read_full (self->file, addr, (apr_size_t) len, NULL);
  return status == APR_SUCCESS ? AVRO_OK : AVRO_FAILURE;
}

static avro_status_t
avro_io_file_skip (struct avro_io *io, uint64_t len)
{
  /* TODO: file seek */
  return AVRO_OK;
}

static avro_status_t
avro_io_file_write (struct avro_io *io, char *addr, uint64_t len)
{
  struct avro_io_file *self = container_of (io, struct avro_io_file, io);
  apr_status_t status =
    apr_file_write_full (self->file, addr, (apr_size_t) len, NULL);
  return status == APR_SUCCESS ? AVRO_OK : AVRO_FAILURE;
}

struct avro_io *
avro_io_file_create (apr_file_t * file)
{
  struct avro_io_file *file_io;
  apr_pool_t *pool;

  if (!file)
    {
      return NULL;
    }
  pool = apr_file_pool_get (file);
  if (!pool)
    {
      return NULL;
    }
  file_io = apr_pcalloc (pool, sizeof (struct avro_io_file));
  if (!file_io)
    {
      return NULL;
    }
  file_io->io.read = avro_io_file_read;
  file_io->io.skip = avro_io_file_skip;
  file_io->io.write = avro_io_file_write;
  file_io->file = file;
  return &file_io->io;
}
