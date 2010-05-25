#ifndef AVRO_CONFIG_H
#define AVRO_CONFIG_H

#ifdef WIN32

// MSVC doesn't support C99, hence inline is not recognized as a keyword,
// so we need to use the MSVC specific __inline instead.
#define inline __inline

#define snprintf _snprintf

#endif // WIN32

#endif // AVRO_CONFIG_H
