# -pedantic
AM_CPPFLAGS=-I$(top_srcdir)/jansson/src
AM_CFLAGS=-Wall
ACLOCAL_AMFLAGS=-I m4

include_HEADERS = avro.h

lib_LTLIBRARIES = libavro.la
libavro_la_SOURCES = st.c st.h schema.c schema.h schema_equal.c \
datum.c datum_equal.c datum_validate.c datum_read.c datum_skip.c datum_write.c datum.h \
io.c dump.c dump.h encoding_binary.c \
container_of.h encoding.h
libavro_la_LIBADD = $(top_builddir)/jansson/src/.libs/libjansson.a
libavro_la_LDFLAGS = \
        -version-info $(LIBAVRO_VERSION) \
        -release $(VERSION) \
        -export-dynamic
