#include "avro.h"

avro_status_t
avro_null (void)
{
  /* Do nothing */
  return AVRO_OK;
}

avro_status_t
avro_bool (AVRO * avro, bool_t * bp)
{
  avro_status_t status;
  char b;
  if (!avro || !bp)
    {
      return AVRO_FAILURE;
    }
  switch (avro->a_op)
    {
    case AVRO_ENCODE:
      {
	b = *bp ? 1 : 0;
	return AVRO_PUTBYTES (avro, &b, 1);
      }
    case AVRO_DECODE:
      {
	status = AVRO_GETBYTES (avro, &b, 1);
	CHECK_ERROR (status);
	*bp = b ? 1 : 0;
      }
    default:
      return AVRO_FAILURE;
    }
  return AVRO_OK;
}
