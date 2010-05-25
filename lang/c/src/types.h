#ifndef AVRO_TYPES_H
#define AVRO_TYPES_H

#ifdef WIN32

typedef signed __int8  int8_t;
typedef signed __int16 int16_t;
typedef signed __int32 int32_t;
typedef signed __int64 int64_t;

typedef unsigned __int8  uint8_t;
typedef unsigned __int16 uint16_t;
typedef unsigned __int32 uint32_t;
typedef unsigned __int64 uint64_t;

typedef char* caddr_t;

#else // if not WIN32
#include <stdint.h>
#include <sys/types.h>
#endif // WIN32

#endif // AVRO_TYPES_H

