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
avro_putstring (struct avro_io *io, apr_pool_t * pool, avro_string_t string)
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
  status = avro_putlong (io, &len);
  if (status != AVRO_OK)
    {
      return status;
    }
  return io->write (io, s, len);
}

avro_status_t
avro_getstring (struct avro_io * io, apr_pool_t * pool,
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
  status = avro_getlong (io, &len);
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
  status = io->read (io, s, len);
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
avro_putbytes (struct avro_io * io, char *data, avro_long_t len)
{
  avro_status_t status;
  if (!io || !data || len < 0)
    {
      return AVRO_FAILURE;
    }
  status = avro_putlong (io, &len);
  if (status != AVRO_OK)
    {
      return status;
    }
  return io->write (io, data, len);
}

avro_status_t
avro_getbytes (struct avro_io * io, apr_pool_t * pool, char **data,
	       avro_long_t * len)
{
  avro_status_t status;
  if (!io || !pool || !data || !len)
    {
      return AVRO_FAILURE;
    }
  status = avro_getlong (io, len);
  if (status != AVRO_OK)
    {
      return status;
    }
  *data = (char *) apr_pcalloc (pool, *len);
  if (!*data)
    {
      return AVRO_FAILURE;
    }
  return io->read (io, *data, *len);
}

avro_status_t
avro_putbool (struct avro_io * io, int boolean)
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
avro_getbool (struct avro_io * io, int *boolean)
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
avro_putint (struct avro_io * io, avro_int_t * ip)
{
  avro_status_t status;
  int32_t n = *ip;
  uint8_t b;

  if (!io || !ip)
    {
      return AVRO_FAILURE;
    }

  /* move sign to low-order bit */
  n = (n << 1) ^ (n >> 31);
  while ((n & ~0x7F) != 0)
    {
      b = ((((uint8_t) n) & 0x7F) | 0x80);
      status = io->write (io, (char *) &b, 1);
      if (status != AVRO_OK)
	{
	  return status;
	}
      n >>= 7;
    }
  b = (uint8_t) n;
  return io->write (io, (char *) &b, 1);
}

avro_status_t
avro_putlong (struct avro_io *io, avro_long_t * lp)
{
  avro_status_t status;
  int64_t n = *lp;
  uint8_t b;

  if (!io || !lp)
    {
      return AVRO_FAILURE;
    }

  /* move sign to low-order bit */
  n = (n << 1) ^ (n >> 63);
  while ((n & ~0x7F) != 0)
    {
      b = ((((uint8_t) n) & 0x7F) | 0x80);
      status = io->write (io, (char *) &b, 1);
      if (status != AVRO_OK)
	{
	  return status;
	}
      n >>= 7;
    }
  b = (uint8_t) n;
  return io->write (io, (char *) &b, 1);
}

avro_status_t
avro_getint (struct avro_io *io, avro_int_t * ip)
{
  avro_status_t status;
  int64_t value = 0;
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
  /* back to two's-complement value; */
  *ip = (value >> 1) ^ -(value & 1);
  return AVRO_OK;
}

avro_status_t
avro_getlong (struct avro_io * io, avro_long_t * lp)
{
  avro_status_t status;
  int64_t value = 0;
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
  /* back to two's-complement value; */
  *lp = (value >> 1) ^ -(value & 1);
  return AVRO_OK;
}
