#include "avro.h"

static avro_status_t
avro_string_bytes_encode (AVRO * avro, char **str, int64_t * len,
			  int64_t maxlen)
{
  avro_status_t status = avro_int64 (avro, len);
  CHECK_ERROR (status);
  return AVRO_PUTBYTES (avro, *str, *len);
}

static avro_status_t
avro_string_bytes_decode (AVRO * avro, char **str, int64_t * len,
			  int null_terminated, int64_t maxlen)
{
  avro_status_t status = avro_int64 (avro, len);
  CHECK_ERROR (status);
  if (*len < 0)
    {
      return AVRO_FAILURE;
    }
  *str = (caddr_t) apr_palloc (avro->pool, *len + null_terminated);
  if (*str == NULL)
    {
      return AVRO_FAILURE;
    }
  status = AVRO_GETBYTES (avro, *str, *len);
  CHECK_ERROR (status);
  if (null_terminated)
    {
      (*str)[*len] = '\0';
    }
  return AVRO_OK;
}

avro_status_t
avro_string (AVRO * avro, char **str, int64_t maxlen)
{
  int64_t len;
  if (!avro || !str)
    {
      return AVRO_FAILURE;
    }
  switch (avro->a_op)
    {
    case AVRO_ENCODE:
      len = strlen (*str);
      return avro_string_bytes_encode (avro, str, &len, maxlen);
    case AVRO_DECODE:
      return avro_string_bytes_decode (avro, str, &len, 1, maxlen);
    default:
      return AVRO_FAILURE;
    }
  return AVRO_OK;
}

avro_status_t
avro_bytes (AVRO * avro, char **bytes, int64_t * len, int64_t maxlen)
{
  if (!avro || !bytes || !len || *len < 0)
    {
      return AVRO_FAILURE;
    }
  switch (avro->a_op)
    {
    case AVRO_ENCODE:
      return avro_string_bytes_encode (avro, bytes, len, maxlen);
    case AVRO_DECODE:
      return avro_string_bytes_decode (avro, bytes, len, 0, maxlen);
    default:
      return AVRO_FAILURE;
    }
  return AVRO_OK;
}
