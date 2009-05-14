#include "avro.h"
#include <string.h>
#include "dump.h"

static avro_status_t
memory_get_bytes (struct AVRO *avro, caddr_t addr, const int64_t len)
{
  if ((avro->len - avro->used) < len || len < 0)
    {
      return AVRO_FAILURE;
    }
  if (len > 0)
    {
      memcpy (addr, avro->addr + avro->used, len);
      avro->used += len;
    }
  return AVRO_OK;
}

static avro_status_t
memory_put_bytes (struct AVRO *avro, const char *addr, const int64_t len)
{
  if ((avro->len - avro->used) < len || len < 0)
    {
      return AVRO_FAILURE;
    }
  if (len > 0)
    {
      memcpy (avro->addr + avro->used, addr, len);
      avro->used += len;
    }
  return AVRO_OK;
}

static const struct avro_ops avro_memory_ops = {
  memory_get_bytes,
  memory_put_bytes
};

avro_status_t
avro_create_memory (AVRO * avro, apr_pool_t * pool, caddr_t addr, int64_t len,
		    avro_op op)
{
  if (!avro || !pool || !addr || len <= 0)
    {
      return AVRO_FAILURE;
    }
  avro->pool = pool;
  avro->a_op = op;
  avro->a_ops = (struct avro_ops *) &avro_memory_ops;
  avro->addr = addr;
  avro->len = len;
  avro->used = 0;
  return AVRO_OK;
}

void
avro_dump_memory (AVRO * avro, FILE * fp)
{
  dump (fp, avro->addr, avro->used);
}
