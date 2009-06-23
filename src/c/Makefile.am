AM_CFLAGS=$(APR_CFLAGS) $(APR_INCLUDES) $(APU_INCLUDES) -Wall -pedantic
C_DOCS_OUTPUT ?= "docs/dox"

EXTRA_DIST=json_schema.y

include_HEADERS = avro.h

lib_LTLIBRARIES = libavro.la
libavro_la_SOURCES = avro_memory.c avro_socket.c avro_file.c \
dump.c dump.h avro.c avro_string.c avro_zigzag.c error.c error.h avro_raw.c \
avro_double.c json_schema.h json_schema.c json_tokenizer.c json.c json.h

check_PROGRAMS=test_avro_zigzag test_avro_string test_avro_bytes test_avro_raw \
test_avro_float_double test_json_parser

noinst_PROGRAMS=lemon
lemon_SOURCES=lemon.c

json_schema.h json_schema.c: lemon json_schema.y
	./lemon ./json_schema.y
	touch json_schema.c json_schema.h

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

test_json_parser=test_json_parser.c
test_json_parser_LDADD=$(APR_LIBS) $(APU_LIBS) $(top_builddir)/libavro.la

TESTS=$(check_PROGRAMS)

# Some files should not be run through indent 
pretty_files := $(shell echo *.[c,h]| sed s/lempar.c// | sed s/avro.h// | sed s/lemon.c//)
pretty:
	@indent $(pretty_files)

docs:
	@(cat docs/doxygen.conf; echo "OUTPUT_DIRECTORY=$(C_DOCS_OUTPUT)")| doxygen -

.PHONY: docs

CLEANFILES=*~
