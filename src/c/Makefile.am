AM_CFLAGS=$(APR_CFLAGS) $(APR_INCLUDES) $(APU_INCLUDES) -Wall -pedantic
C_DOCS_OUTPUT ?= "docs/dox"

EXTRA_DIST=json_schema.y lemon.c lempar.c

include_HEADERS = avro.h

lib_LTLIBRARIES = libavro.la
libavro_la_SOURCES = avro_memory.c avro_socket.c avro_file.c \
dump.c dump.h avro.c avro_string.c avro_zigzag.c error.c error.h avro_raw.c \
avro_double.c json_schema.c json_schema.h json_tokenizer.c json.c json.h \
json_tokenizer.h

check_PROGRAMS=test_avro_zigzag test_avro_string test_avro_bytes test_avro_raw \
test_avro_float_double test_json_parser

lemon: $(top_srcdir)/lemon.c $(top_srcdir)/lempar.c
	$(CC) -o $@ $(top_srcdir)/lemon.c
	-cp $(top_srcdir)/lempar.c .

json_schema.c json_shema.h: $(top_srcdir)/json_schema.y lemon
	-cp $(top_srcdir)/json_schema.y .
	./lemon json_schema.y

# Make sure to generate the json_schema in the source directory before distribution
dist-hook: json_schema.h json_schema.c
	cp -r $(srcdir)/json $(distdir)

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

CLEANFILES=$(top_srcdir)/*~ trace.txt json_schema.out lemon
