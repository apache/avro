# -pedantic
AM_CPPFLAGS=-I$(top_srcdir)/jansson/src
AM_CFLAGS=-Wall -Wextra -Wunused-parameter
ACLOCAL_AMFLAGS=-I m4

bin_PROGRAMS =

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
avro/resolver.h \
avro/schema.h \
avro/value.h

lib_LTLIBRARIES = libavro.la
libavro_la_SOURCES = st.c st.h schema.c schema.h schema_equal.c \
value.c value-hash.c value-json.c value-read.c value-sizeof.c value-write.c \
avro_generic_internal.h generic.c \
datum.c datum_equal.c datum_validate.c datum_read.c datum_skip.c datum_write.c datum_size.c datum.h \
datum_value.c \
array.c map.c memoize.c string.c \
consumer.c consume-binary.c resolver.c \
resolved-reader.c resolved-writer.c \
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

bin_PROGRAMS += avrocat
avrocat_SOURCES = avrocat.c
avrocat_LDADD = libavro.la

bin_PROGRAMS += avropipe
avropipe_SOURCES = avropipe.c
avropipe_LDADD = libavro.la
