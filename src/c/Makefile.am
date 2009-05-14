AM_CFLAGS=$(APR_CFLAGS) $(APR_INCLUDES) $(APU_INCLUDES) -Wall -pedantic

include_HEADERS = avro.h

lib_LTLIBRARIES = libavro.la
libavro_la_SOURCES = avro_memory.c avro_socket.c avro_file.c \
dump.c dump.h avro.c avro_string.c avro_zigzag.c error.c error.h avro_raw.c \
avro_double.c

check_PROGRAMS=test_avro_zigzag test_avro_string test_avro_bytes test_avro_raw \
test_avro_float_double

test_avro_zigzag_SOURCE=test_avro_zigzag.c
test_avro_zigzag_LDADD=$(APR_LIBS) $(APU_LIBS) $(top_builddir)/libavro.la

test_avro_string_SOURCE=test_avro_string.c
test_avro_string_LDADD=$(APR_LIBS) $(APU_LIBS) $(top_builddir)/libavro.la

test_avro_bytes_SOURCE=test_avro_bytes.c
test_avro_bytes_LDADD=$(APR_LIBS) $(APU_LIBS) $(top_builddir)/libavro.la

test_avro_raw_SOURCE=test_avro_raw.c
test_avro_raw_LDADD=$(APR_LIBS) $(APU_LIBS) $(top_builddir)/libavro.la

test_avro_float_double_SOURCE=test_avro_float_double.c
test_avro_float_double_LDADD=$(APR_LIBS) $(APU_LIBS) $(top_builddir)/libavro.la

TESTS=$(check_PROGRAMS)

pretty:
	indent *.c *.h

CLEANFILES=*~
