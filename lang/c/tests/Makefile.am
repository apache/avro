# -pedantic
AM_CPPFLAGS=-I$(top_srcdir)/src
AM_CFLAGS=-Wall 
ACLOCAL_AMFLAGS=-I m4

EXTRA_DIST=schema_tests test_valgrind

check_PROGRAMS=test_avro_schema test_avro_data

test_LDADD=$(top_builddir)/src/libavro.la

test_avro_schema_SOURCES=test_avro_schema.c
test_avro_schema_LDADD=$(test_LDADD)

test_avro_data_SOURCES=test_avro_data.c
test_avro_data_LDADD=$(test_LDADD)

TESTS=$(check_PROGRAMS) test_valgrind
