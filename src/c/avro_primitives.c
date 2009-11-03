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
#include <stdlib.h>

avro_status_t
avro_write_string (struct avro_io_writer *io, apr_pool_t * pool,
		   avro_string_t string)
{
  avro_status_t status;
  size_t wc_len, converted;
  avro_long_t len;
  char *s;
  if (!io)
    {
      return AVRO_FAILURE;
    }
  wc_len = wcslen ((const wchar_t *) string);

  /* TODO: how to calculate the number of char needed for wc_len 
     assuming max for now */
  s = apr_pcalloc (pool, wc_len * 4);
  if (!s)
    {
      return AVRO_FAILURE;
    }

  converted = wcstombs (s, string, wc_len * 4);
  if (converted < 0)
    {
      return AVRO_FAILURE;
    }
  len = converted;
  status = avro_write_long (io, &len);
  if (status != AVRO_OK)
    {
      return status;
    }
  return io->write (io, s, len);
}

avro_status_t
avro_read_string (struct avro_io_reader * io, apr_pool_t * pool,
		  avro_string_t * string)
{
  avro_status_t status;
  avro_long_t len;
  size_t converted;
  char *s;

  if (!io || !pool || !string)
    {
      return AVRO_FAILURE;
    }
  status = avro_read_long (io, &len);
  if (status != AVRO_OK)
    {
      return status;
    }
  if (len < 0)
    {
      return AVRO_FAILURE;
    }
  s = apr_pcalloc (pool, len + 1);
  if (!s)
    {
      return AVRO_FAILURE;
    }
  status = io->read (io, (void *) s, len);
  if (status != AVRO_OK)
    {
      return status;
    }
  s[len] = '\0';

  /* TODO: calculate this more exactly... assuming char len == wchar len is wrong */
  *string = apr_pcalloc (pool, (len + 1) * sizeof (wchar_t));
  if (!*string)
    {
      return AVRO_FAILURE;
    }
  converted = mbstowcs (*string, s, len);
  if (converted < 0)
    {
      return AVRO_FAILURE;
    }
  (*string)[converted] = '\0';
  return AVRO_OK;
}

avro_status_t
avro_write_bytes (struct avro_io_writer * io, void *data, avro_long_t len)
{
  avro_status_t status;
  if (!io || !data || len < 0)
    {
      return AVRO_FAILURE;
    }
  status = avro_write_long (io, &len);
  if (status != AVRO_OK)
    {
      return status;
    }
  return io->write (io, data, len);
}

avro_status_t
avro_read_bytes (struct avro_io_reader * io, apr_pool_t * pool, void **data,
		 avro_long_t * len)
{
  avro_status_t status;
  if (!io || !pool || !data || !len)
    {
      return AVRO_FAILURE;
    }
  status = avro_read_long (io, len);
  if (status != AVRO_OK)
    {
      return status;
    }
  *data = apr_pcalloc (pool, *len);
  if (!*data)
    {
      return AVRO_FAILURE;
    }
  return io->read (io, *data, *len);
}

avro_status_t
avro_write_bool (struct avro_io_writer * io, int boolean)
{
  char b;
  if (!io)
    {
      return AVRO_FAILURE;
    }
  b = boolean;
  return io->write (io, &b, 1);
}

avro_status_t
avro_read_bool (struct avro_io_reader * io, int *boolean)
{
  avro_status_t status;
  char b;
  if (!io || !boolean)
    {
      return AVRO_FAILURE;
    }
  status = io->read (io, &b, 1);
  if (status != AVRO_OK)
    {
      return status;
    }
  *boolean = b;
  return AVRO_OK;
}

avro_status_t
avro_write_int (struct avro_io_writer * io, avro_int_t * ip)
{
  avro_status_t status;
  uint32_t n;
  uint8_t b;

  if (!io || !ip)
    {
      return AVRO_FAILURE;
    }

  n = (*ip << 1) ^ (*ip >> 31);
  while ((n & ~0x7F) != 0)
    {
      b = ((((uint8_t) n) & 0x7F) | 0x80);
      status = io->write (io, (void *) &b, 1);
      if (status != AVRO_OK)
	{
	  return status;
	}
      n >>= 7;
    }

  b = (uint8_t) n;
  return io->write (io, (void *) &b, 1);
}

avro_status_t
avro_write_long (struct avro_io_writer *io, avro_long_t * lp)
{
  avro_status_t status;
  uint64_t n;
  uint8_t b;

  if (!io || !lp)
    {
      return AVRO_FAILURE;
    }

  n = (*lp << 1) ^ (*lp >> 63);
  while ((n & ~0x7F) != 0)
    {
      b = ((((uint8_t) n) & 0x7F) | 0x80);
      status = io->write (io, (void *) &b, 1);
      if (status != AVRO_OK)
	{
	  return status;
	}
      n >>= 7;
    }
  b = (uint8_t) n;
  return io->write (io, (void *) &b, 1);
}

avro_status_t
avro_read_int (struct avro_io_reader *io, avro_int_t * ip)
{
  avro_status_t status;
  uint32_t value = 0;
  int offset = 0;
  uint8_t b;
  const int MAX_VARINT_BUF_SIZE = 5;

  if (!io || !ip)
    {
      return AVRO_FAILURE;
    }

  do
    {
      if (offset == MAX_VARINT_BUF_SIZE)
	{
	  return AVRO_FAILURE;
	}
      status = io->read (io, (char *) &b, 1);
      if (status != AVRO_OK)
	{
	  return status;
	}
      value |= (int32_t) (b & 0x7F) << (7 * offset);
      ++offset;
    }
  while (b & 0x80);
  *ip = (value >> 1) ^ -(value & 1);
  return AVRO_OK;
}

avro_status_t
avro_read_long (struct avro_io_reader * io, avro_long_t * lp)
{
  avro_status_t status;
  uint64_t value = 0;
  int offset = 0;
  uint8_t b;
  const int MAX_VARINT_BUF_SIZE = 10;

  if (!io || !lp)
    {
      return AVRO_FAILURE;
    }

  do
    {
      if (offset == MAX_VARINT_BUF_SIZE)
	{
	  return AVRO_FAILURE;
	}
      status = io->read (io, (char *) &b, 1);
      if (status != AVRO_OK)
	{
	  return status;
	}
      value |= (int64_t) (b & 0x7F) << (7 * offset);
      ++offset;
    }
  while (b & 0x80);
  *lp = (value >> 1) ^ -(value & 1);
  return AVRO_OK;
}

avro_status_t
avro_write_float (struct avro_io_writer * io, float value)
{
  union
  {
    float f;
    int32_t i;
  } v;
  if (!io)
    {
      return AVRO_FAILURE;
    }
  v.f = value;
  return avro_write_int32_le (io, (const int32_t) v.i);
}

avro_status_t
avro_read_float (struct avro_io_reader *io, float *fp)
{
  avro_status_t status;
  union
  {
    float f;
    int32_t i;
  } v;
  if (!io || !fp)
    {
      return AVRO_FAILURE;
    }
  status = avro_read_int32_le (io, &v.i);
  if (status != AVRO_OK)
    {
      return status;
    }
  *fp = v.f;
  return AVRO_OK;
}

avro_status_t
avro_write_double (struct avro_io_writer * io, double value)
{
  union
  {
    double d;
    int64_t i;
  } v;
  if (!io)
    {
      return AVRO_FAILURE;
    }
  v.d = value;
  return avro_write_int64_le (io, v.i);
}

avro_status_t
avro_read_double (struct avro_io_reader * io, double *dp)
{
  avro_status_t status;
  union
  {
    double d;
    int64_t i;
  } v;
  if (!io || !dp)
    {
      return AVRO_FAILURE;
    }
  status = avro_read_int64_le (io, &v.i);
  if (status != AVRO_OK)
    {
      return AVRO_FAILURE;
    }
  *dp = v.d;
  return AVRO_OK;
}
