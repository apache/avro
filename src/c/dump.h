#ifndef DUMP_H
#define DUMP_H

#include <stdio.h>
#include <sys/types.h>

void dump (FILE * out, const caddr_t addr, const long len);

#endif
