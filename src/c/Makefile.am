# -pedantic
AM_CFLAGS=$(APR_CFLAGS) $(APR_INCLUDES) $(APU_INCLUDES) -Wall 
C_DOCS_OUTPUT ?= "docs/dox"

EXTRA_DIST=json_parser.y lemon.c lempar.c json_tests

include_HEADERS = avro.h

lib_LTLIBRARIES = libavro.la
libavro_la_SOURCES = avro.h avro_private.h avro.c \
json_parser.c json_parser.h json_tokenizer.c json.c json.h json_tokenizer.h  \
dump.c dump.h error.c error.h avro_value.c \
avro_array.c avro_boolean.c avro_bytes.c avro_double.c avro_enum.c avro_fixed.c  \
avro_float.c avro_int.c avro_long.c avro_map.c avro_null.c \
avro_record.c avro_string.c avro_union.c container_of.h \
avro_io_file.c avro_io_socket.c avro_io_memory.c \
avro_file_container.c avro_endian.c avro_primitives.c

check_PROGRAMS=test_json_parser test_avro_schema 

lemon: $(top_srcdir)/lemon.c $(top_srcdir)/lempar.c
	$(CC) -o $@ $(top_srcdir)/lemon.c
	-cp $(top_srcdir)/lempar.c .

json_parser.c json_shema.h: $(top_srcdir)/json_parser.y lemon
	-cp $(top_srcdir)/json_parser.y .
	./lemon json_parser.y

# Make sure to generate the json_parser in the source directory before distribution
dist-hook: json_parser.h json_parser.c

test_LDADD=$(APR_LIBS) $(APU_LIBS) $(top_builddir)/libavro.la

test_json_parser=test_json_parser.c
test_json_parser_LDADD=$(test_LDADD)

test_avro_schema=test_avro_schema.c
test_avro_schema_LDADD=$(test_LDADD)

#test_avro_interop=test_avro_interop.c
#test_avro_interop_LDADD=$(test_LDADD)

TESTS=$(check_PROGRAMS)

# Run indent on all files. 
# NOTE: Don't indent avro.h (messes up docs) or lemon.c,lempar.c (messes up parser)
pretty:
	pushd $(top_srcdir); \
	for file in *.[c,h]; \
	do \
	if [ $$file = "avro.h" -o $$file = "lemon.c" -o $$file = "lempar.c" ]; then \
		continue;\
	fi;\
	indent $$file; \
	done;\
	popd;

docs:
	pushd $(top_srcdir); \
	(cat docs/doxygen.conf; echo "OUTPUT_DIRECTORY=$(C_DOCS_OUTPUT)")| doxygen - ;\
	popd;


.PHONY: docs 

CLEANFILES=$(top_srcdir)/*~ trace.txt json_parser.out lemon
