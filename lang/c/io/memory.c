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

struct avro_io_memory_reader
{
  void *addr;
  avro_long_t used;
  avro_long_t len;
  struct avro_io_reader io;
};

struct avro_io_memory_writer
{
  void *addr;
  avro_long_t used;
  avro_long_t len;
  struct avro_io_writer io;
};

static avro_status_t
avro_io_memory_read (struct avro_io_reader *io, void *addr, avro_long_t len)
{
  struct avro_io_memory_reader *self =
    container_of (io, struct avro_io_memory_reader, io);
  if (len < 0 || (self->len - self->used) < len)
    {
      return AVRO_FAILURE;
    }
  memcpy (addr, self->addr + self->used, len);
  self->used += len;
  DEBUG (fprintf
	 (stderr, "avro_io_memory_read %ld bytes, %ld/%ld used\n", len,
	  self->used, self->len));
  return AVRO_OK;
}

static avro_status_t
avro_io_memory_skip (struct avro_io_reader *io, avro_long_t len)
{
  struct avro_io_memory_reader *self =
    container_of (io, struct avro_io_memory_reader, io);
  if (len < 0 || (self->len - self->used) < len)
    {
      return AVRO_FAILURE;
    }
  self->used += len;
  DEBUG (fprintf
	 (stderr, "avro_io_memory_skip %ld bytes, %ld/%ld used\n", len,
	  self->used, self->len));
  return AVRO_OK;
}

static avro_status_t
avro_io_memory_write (struct avro_io_writer *io, void *addr, avro_long_t len)
{
  struct avro_io_memory_writer *self =
    container_of (io, struct avro_io_memory_writer, io);
  if (len < 0 || (self->len - self->used) < len)
    {
      return AVRO_FAILURE;
    }
  memcpy (self->addr + self->used, addr, len);
  self->used += len;
  DEBUG (fprintf
	 (stderr, "avro_io_memory_write %d bytes, %d/%d used\n", len,
	  self->used, self->len));
  return AVRO_OK;
}

struct avro_io_reader *
avro_io_reader_from_memory (apr_pool_t * pool, void *addr, avro_long_t len)
{
  struct avro_io_memory_reader *io_memory;

  if (!pool || !addr || len < 0)
    {
      return NULL;
    }

  io_memory = apr_pcalloc (pool, sizeof (struct avro_io_memory_reader));
  if (!io_memory)
    {
      return NULL;
    }
  io_memory->addr = addr;
  io_memory->used = 0;
  io_memory->len = len;
  io_memory->io.read = avro_io_memory_read;
  io_memory->io.skip = avro_io_memory_skip;
  return &io_memory->io;
}

struct avro_io_writer *
avro_io_writer_from_memory (apr_pool_t * pool, void *addr, avro_long_t len)
{
  struct avro_io_memory_writer *io_memory;

  if (!pool || !addr || len < 0)
    {
      return NULL;
    }

  io_memory = apr_pcalloc (pool, sizeof (struct avro_io_memory_writer));
  if (!io_memory)
    {
      return NULL;
    }
  io_memory->addr = addr;
  io_memory->used = 0;
  io_memory->len = len;
  io_memory->io.write = avro_io_memory_write;
  return &io_memory->io;
}
