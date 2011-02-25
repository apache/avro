# -pedantic
AM_CPPFLAGS=-I$(top_srcdir)/jansson/src
AM_CFLAGS=-Wall -Wextra -Wunused-parameter
ACLOCAL_AMFLAGS=-I m4

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = avro-c.pc

include_HEADERS = avro.h

lib_LTLIBRARIES = libavro.la
libavro_la_SOURCES = st.c st.h schema.c schema.h schema_equal.c \
datum.c datum_equal.c datum_validate.c datum_read.c datum_skip.c datum_write.c datum_size.c datum.h \
datum_json.c \
io.c dump.c dump.h encoding_binary.c \
allocation.h allocation.c \
avro_private.h encoding.h datafile.c \
avro_errors.h errors.c
libavro_la_LIBADD = $(top_builddir)/jansson/src/.libs/libjansson.a
libavro_la_LDFLAGS = \
        -version-info $(LIBAVRO_VERSION) \
        -release $(VERSION) \
        -export-dynamic
