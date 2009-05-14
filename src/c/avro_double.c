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
