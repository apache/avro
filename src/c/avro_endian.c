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

avro_status_t
avro_read_int32_le (struct avro_io_reader *io, int32_t * value)
{
  avro_status_t status;
  uint8_t buf[4];
  if (!io || !value)
    {
      return AVRO_FAILURE;
    }
  status = io->read (io, (void *) buf, sizeof (buf));
  if (status != AVRO_OK)
    {
      return status;
    }
  *value = ((int32_t) buf[0] << 0)
    | ((int32_t) buf[1] << 8)
    | ((int32_t) buf[2] << 16) | ((int32_t) buf[3] << 24);
  return AVRO_OK;
}

avro_status_t
avro_write_int32_le (struct avro_io_writer * io, const int32_t value)
{
  uint8_t buf[4];
  buf[0] = (uint8_t) (value >> 0);
  buf[1] = (uint8_t) (value >> 8);
  buf[2] = (uint8_t) (value >> 16);
  buf[3] = (uint8_t) (value >> 24);
  if (!io)
    {
      return AVRO_FAILURE;
    }
  return io->write (io, (void *) buf, sizeof (buf));
}

avro_status_t
avro_read_int32_be (struct avro_io_reader *io, int32_t * value)
{
  avro_status_t status;
  uint8_t buf[4];
  if (!io || !value)
    {
      return AVRO_FAILURE;
    }
  status = io->read (io, (void *) buf, sizeof (buf));
  if (status != AVRO_OK)
    {
      return status;
    }
  *value = ((int32_t) buf[0] << 24)
    | ((int32_t) buf[1] << 16)
    | ((int32_t) buf[2] << 8) | ((int32_t) buf[3] << 0);
  return AVRO_OK;
}

avro_status_t
avro_write_int32_be (struct avro_io_writer * io, const int32_t value)
{
  uint8_t buf[4];
  buf[0] = (uint8_t) (value >> 24);
  buf[1] = (uint8_t) (value >> 16);
  buf[2] = (uint8_t) (value >> 8);
  buf[3] = (uint8_t) (value >> 0);
  if (!io)
    {
      return AVRO_FAILURE;
    }
  return io->write (io, (void *) buf, sizeof (buf));
}


avro_status_t
avro_read_int64_le (struct avro_io_reader *io, int64_t * value)
{
  avro_status_t status;
  uint8_t buf[8];
  if (!io || !value)
    {
      return AVRO_FAILURE;
    }
  status = io->read (io, (void *) buf, sizeof (buf));
  if (status != AVRO_OK)
    {
      return status;
    }
  *value = ((int64_t) buf[0] << 0)
    | ((int64_t) buf[1] << 8)
    | ((int64_t) buf[2] << 16)
    | ((int64_t) buf[3] << 24)
    | ((int64_t) buf[4] << 32)
    | ((int64_t) buf[5] << 40)
    | ((int64_t) buf[6] << 48) | ((int64_t) buf[7] << 56);
  return AVRO_OK;
}

avro_status_t
avro_write_int64_le (struct avro_io_writer * io, const int64_t value)
{
  uint8_t buf[8];
  buf[0] = (uint8_t) (value >> 0);
  buf[1] = (uint8_t) (value >> 8);
  buf[2] = (uint8_t) (value >> 16);
  buf[3] = (uint8_t) (value >> 24);
  buf[4] = (uint8_t) (value >> 32);
  buf[5] = (uint8_t) (value >> 40);
  buf[6] = (uint8_t) (value >> 48);
  buf[7] = (uint8_t) (value >> 56);
  if (!io)
    {
      return AVRO_FAILURE;
    }
  return io->write (io, (void *) buf, sizeof (buf));
}

avro_status_t
avro_read_int64_be (struct avro_io_reader *io, int64_t * value)
{
  avro_status_t status;
  uint8_t buf[8];
  if (!io || !value)
    {
      return AVRO_FAILURE;
    }
  status = io->read (io, (void *) buf, sizeof (buf));
  if (status != AVRO_OK)
    {
      return status;
    }
  *value = ((int64_t) buf[0] << 56)
    | ((int64_t) buf[1] << 48)
    | ((int64_t) buf[2] << 40)
    | ((int64_t) buf[3] << 32)
    | ((int64_t) buf[4] << 24)
    | ((int64_t) buf[5] << 16)
    | ((int64_t) buf[6] << 8) | ((int64_t) buf[7] << 0);
  return AVRO_OK;
}

avro_status_t
avro_write_int64_be (struct avro_io_writer * io, const int64_t value)
{
  uint8_t buf[8];
  buf[0] = (uint8_t) (value >> 56);
  buf[1] = (uint8_t) (value >> 48);
  buf[2] = (uint8_t) (value >> 40);
  buf[3] = (uint8_t) (value >> 32);
  buf[4] = (uint8_t) (value >> 24);
  buf[5] = (uint8_t) (value >> 16);
  buf[6] = (uint8_t) (value >> 8);
  buf[7] = (uint8_t) (value >> 0);
  if (!io)
    {
      return AVRO_FAILURE;
    }
  return io->write (io, (void *) buf, sizeof (buf));
}
