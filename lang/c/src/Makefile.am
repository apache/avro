# -pedantic
AM_CPPFLAGS=-I$(top_srcdir)/jansson/src
AM_CFLAGS=-Wall -Wextra -Wunused-parameter
ACLOCAL_AMFLAGS=-I m4

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = avro-c.pc

nobase_include_HEADERS = avro.h \
avro/allocation.h \
avro/basics.h \
avro/consumer.h \
avro/data.h \
avro/errors.h \
avro/generic.h \
avro/io.h \
avro/legacy.h \
avro/refcount.h \
avro/schema.h \
avro/value.h

lib_LTLIBRARIES = libavro.la
libavro_la_SOURCES = st.c st.h schema.c schema.h schema_equal.c \
value.c generic.c value-read.c value-sizeof.c value-write.c \
generic-array.c generic-enum.c generic-fixed.c generic-map.c \
generic-primitives.c generic-record.c generic-union.c \
datum.c datum_equal.c datum_validate.c datum_read.c datum_skip.c datum_write.c datum_size.c datum.h \
datum_json.c \
datum_value.c \
array.c map.c memoize.c string.c \
consumer.c resolver.c \
io.c dump.c dump.h encoding_binary.c \
allocation.c \
wrapped-buffer.c \
avro_private.h encoding.h datafile.c \
errors.c
libavro_la_LIBADD = $(top_builddir)/jansson/src/.libs/libjansson.a
libavro_la_LDFLAGS = \
        -version-info $(LIBAVRO_VERSION) \
        -release $(VERSION) \
        -export-dynamic
