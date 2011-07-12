# -pedantic
AM_CPPFLAGS=-I$(top_srcdir)/src
AM_CFLAGS=-Wall -Wextra -Wunused-parameter
ACLOCAL_AMFLAGS=-I m4

EXTRA_DIST=schema_tests test_valgrind

check_PROGRAMS=test_avro_schema test_avro_schema_names test_avro_data test_cpp

noinst_PROGRAMS=generate_interop_data test_interop_data performance

test_LDADD=$(top_builddir)/src/libavro.la

test_avro_schema_SOURCES=test_avro_schema.c
test_avro_schema_LDADD=$(test_LDADD)

test_avro_schema_names_SOURCES=test_avro_schema_names.c
test_avro_schema_names_LDADD=$(test_LDADD)

test_avro_data_SOURCES=test_avro_data.c
test_avro_data_LDADD=$(test_LDADD)

test_cpp_SOURCES=test_cpp.cpp
test_cpp_LDADD=$(test_LDADD)

generate_interop_data_SOURCES=generate_interop_data.c
generate_interop_data_LDADD=$(test_LDADD)

performance_SOURCES=performance.c
performance_LDADD=$(test_LDADD)

test_interop_data_SOURCES=test_interop_data.c
test_interop_data_LDADD=$(test_LDADD)

# Skip the valgrind test for now; we'll turn this back on once we commit
# a fix for AVRO-766.
#TESTS=$(check_PROGRAMS) test_valgrind
