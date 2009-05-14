#include "dump.h"
#include <ctype.h>
#include <string.h>
#include <stdint.h>

static void
dump_line (FILE * out, const caddr_t addr, const long len)
{
  int i;
  fprintf (out, "|");
  for (i = 0; i < 16; i++)
    {
      if (i < len)
	{
	  fprintf (out, " %02X", ((uint8_t *) addr)[i]);
	}
      else
	{
	  fprintf (out, " ..");
	}
      if (!((i + 1) % 8))
	{
	  fprintf (out, " |");
	}
    }
  fprintf (out, "\t");
  for (i = 0; i < 16; i++)
    {
      char c = 0x7f & ((uint8_t *) addr)[i];
      if (i < len && isprint (c))
	{
	  fprintf (out, "%c", c);
	}
      else
	{
	  fprintf (out, ".");
	}
    }
}

void
dump (FILE * out, const caddr_t addr, const long len)
{
  int i;
  for (i = 0; i < len; i += 16)
    {
      dump_line (out, addr + i, (len - i) < 16 ? (len - i) : 16);
      fprintf (out, "\n");
    }
  fflush (out);
}
