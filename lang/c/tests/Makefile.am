# -pedantic
AM_CPPFLAGS=-I$(top_srcdir)/src
AM_CFLAGS=-Wall -Wextra -Wunused-parameter
ACLOCAL_AMFLAGS=-I m4

EXTRA_DIST=schema_tests test_valgrind

check_PROGRAMS=test_avro_schema test_avro_data

noinst_PROGRAMS=generate_interop_data test_interop_data

test_LDADD=$(top_builddir)/src/libavro.la

test_avro_schema_SOURCES=test_avro_schema.c
test_avro_schema_LDADD=$(test_LDADD)

test_avro_data_SOURCES=test_avro_data.c
test_avro_data_LDADD=$(test_LDADD)

generate_interop_data_SOURCES=generate_interop_data.c
generate_interop_data_LDADD=$(test_LDADD)

test_interop_data_SOURCES=test_interop_data.c
test_interop_data_LDADD=$(test_LDADD)

TESTS=$(check_PROGRAMS) test_valgrind
