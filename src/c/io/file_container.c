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
#include <apr_file_io.h>
#include "util/dump.h"

struct avro_metadata
{
  avro_string_t key;
  void *value;
  avro_long_t len;
};

struct avro_file_container
{
  char uuid[16];
  apr_hash_t *meta;
  JSON_value *schema;
  avro_long_t size;
  struct avro_reader reader;
};

struct avro_reader *
avro_reader_file_container_from_file (apr_file_t * file)
{
  apr_status_t apr_status;
  avro_status_t status;
  struct avro_file_container *container;
  struct apr_pool_t *pool;
  struct avro_io_reader *io;
  char buf[4];
  int i;
  apr_off_t offset, len;
  int32_t footersize;
  avro_long_t metalength;
  struct avro_metadata *metadata;

  if (!file)
    {
      return NULL;
    }

  pool = apr_file_pool_get (file);
  if (!pool)
    {
      return NULL;
    }

  container = apr_pcalloc (pool, sizeof (struct avro_file_container));
  container->reader.format = AVRO_BINARY_FORMAT;
  container->reader.io = io = avro_io_reader_from_file (file);
  if (!io)
    {
      return NULL;
    }

  status = io->read (io, (void *) buf, 4);
  if (status != AVRO_OK)
    {
      return NULL;
    }

  /* Assuming version zero here */
  if (buf[0] != 'O' || buf[1] != 'b' || buf[2] != 'j' || buf[3] != 0)
    {
      /* Not an Avro file */
      return NULL;
    }

  /* Grab the UUID */
  io->read (io, container->uuid, 16);
  DEBUG (dump (stderr, container->uuid, 16));

  /* Jump to the end of the file */
  offset = 0;
  apr_status = apr_file_seek (file, APR_END, &offset);
  if (apr_status != APR_SUCCESS)
    {
      return NULL;
    }
  len = offset;

  /* Move back four bytes to get the footersize */
  offset = -4;
  apr_status = apr_file_seek (file, APR_CUR, &offset);
  if (apr_status != APR_SUCCESS)
    {
      return NULL;
    }
  /* Read in the footersize */
  status = avro_read_int32_be (io, &footersize);
  if (status != AVRO_OK)
    {
      return NULL;
    }

  /* Seek in the file to metadata */
  offset = -footersize;
  apr_status = apr_file_seek (file, APR_END, &offset);
  if (apr_status != APR_SUCCESS)
    {
      return NULL;
    }

  /* Read the metadata length */
  status = avro_read_long (io, &metalength);
  if (status != AVRO_OK)
    {
      return NULL;
    }

  if (metalength < 0)
    {
      avro_long_t ignore;
      avro_read_long (io, &ignore);
    }

  /* Create a hash table for the meta data */
  container->meta = apr_hash_make (pool);
  for (i = 0; i < metalength; i++)
    {
      avro_string_t key;

      metadata = apr_pcalloc (pool, sizeof (struct avro_metadata));
      if (!metadata)
	{
	  return NULL;
	}
      avro_read_string (io, pool, &key);
      avro_read_bytes (io, pool, &metadata->value, &metadata->len);
      apr_hash_set (container->meta, key, wcslen (key) * sizeof (wchar_t),
		    metadata);
      DEBUG (fprintf (stderr, "%ls = ", key));
      DEBUG (dump (stderr, metadata->value, metadata->len));
    }

  /* Save the schema */
  metadata = apr_hash_get (container->meta, L"schema", sizeof (wchar_t) * 6);
  if (!metadata)
    {
      return NULL;
    }
  /* Validate the schema */
  container->schema = JSON_parse (pool, metadata->value, metadata->len);
  if (!container->schema)
    {
      /* Invalid schema */
      return NULL;
    }
  DEBUG (JSON_print (stderr, container->schema));
  metadata = apr_hash_get (container->meta, L"sync", sizeof (wchar_t) * 4);
  if (!metadata)
    {
      return NULL;
    }
  if (memcmp (container->uuid, metadata->value, 16) != 0)
    {
      /* TODO: find the correct schema.. */
      return NULL;
    }

  /* Seek back to the start of the file */
  offset = 20;			/* Obj0 + UUID */
  apr_status = apr_file_seek (file, APR_SET, &offset);
  if (apr_status != APR_SUCCESS)
    {
      return NULL;
    }

  /* Read the size of the block */
  status = avro_read_long (io, &container->size);
  if (status != AVRO_OK)
    {
      return NULL;
    }
  return &container->reader;
}

struct avro_reader *
avro_reader_file_container_create (apr_pool_t * pool, const char *fname,
				   apr_int32_t flag, apr_fileperms_t perm)
{
  apr_status_t status;
  apr_file_t *file;

  if (!pool || !fname)
    {
      return NULL;
    }

  status = apr_file_open (&file, fname, flag, perm, pool);
  if (status != APR_SUCCESS)
    {
      return NULL;
    }

  return avro_reader_file_container_from_file (file);
}
