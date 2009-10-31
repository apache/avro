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
#include <apr_network_io.h>

struct avro_io_socket_reader
{
  apr_socket_t *socket;
  struct avro_io_reader io;
};

struct avro_io_socket_writer
{
  apr_socket_t *socket;
  struct avro_io_writer io;
};

static avro_status_t
avro_io_socket_read (struct avro_io_reader *io, void *addr, avro_long_t len)
{
  apr_status_t status;
  apr_size_t bytes_wanted = len;
  apr_size_t bytes_recvd = 0;
  struct avro_io_socket_reader *self =
    container_of (io, struct avro_io_socket_reader, io);
  while (bytes_recvd < bytes_wanted)
    {
      apr_size_t bytes_requested = bytes_wanted - bytes_recvd;
      status = apr_socket_recv (self->socket, addr, &bytes_requested);
      if (status != APR_SUCCESS)
	{
	  return AVRO_FAILURE;
	}
      bytes_recvd += bytes_requested;
    }
  return AVRO_OK;
}

static avro_status_t
avro_io_socket_skip (struct avro_io_reader *io, avro_long_t len)
{
  /* TODO */
  return AVRO_OK;
}

static avro_status_t
avro_io_socket_write (struct avro_io_writer *io, void *addr, avro_long_t len)
{
  apr_status_t status;
  struct avro_io_socket_writer *self =
    container_of (io, struct avro_io_socket_writer, io);
  apr_size_t bytes_wanted = len;
  apr_size_t bytes_sent = 0;
  while (bytes_sent < bytes_wanted)
    {
      apr_size_t bytes_requested = bytes_wanted - bytes_sent;
      status = apr_socket_send (self->socket, addr, &bytes_requested);
      if (status != APR_SUCCESS)
	{
	  return AVRO_FAILURE;
	}
      bytes_sent += bytes_requested;
    }
  return AVRO_OK;
}

struct avro_io_reader *
avro_io_reader_from_socket (apr_socket_t * socket)
{
  struct avro_io_socket_reader *socket_io;
  apr_pool_t *pool;

  if (!socket)
    {
      return NULL;
    }

  pool = apr_socket_pool_get (socket);
  if (!pool)
    {
      return NULL;
    }

  socket_io = apr_pcalloc (pool, sizeof (struct avro_io_socket_reader));
  if (!socket_io)
    {
      return NULL;
    }
  socket_io->socket = socket;
  socket_io->io.read = avro_io_socket_read;
  socket_io->io.skip = avro_io_socket_skip;
  return &socket_io->io;
}

struct avro_io_writer *
avro_io_writer_from_socket (apr_socket_t * socket)
{
  struct avro_io_socket_writer *socket_io;
  apr_pool_t *pool;

  if (!socket)
    {
      return NULL;
    }

  pool = apr_socket_pool_get (socket);
  if (!pool)
    {
      return NULL;
    }

  socket_io = apr_pcalloc (pool, sizeof (struct avro_io_socket_writer));
  if (!socket_io)
    {
      return NULL;
    }
  socket_io->socket = socket;
  socket_io->io.write = avro_io_socket_write;
  return &socket_io->io;
}
