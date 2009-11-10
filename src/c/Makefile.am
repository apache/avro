# -pedantic
AM_CFLAGS=$(APR_CFLAGS) $(APR_INCLUDES) $(APU_INCLUDES) -Wall 
ACLOCAL_AMFLAGS=-I m4

C_DOCS_OUTPUT ?= "docs/dox"

EXTRA_DIST=json/json_parser.y json/lemon.c json/lempar.c \
tests/json_tests tests/schema_tests docs version.sh m4

include_HEADERS = avro.h

lib_LTLIBRARIES = libavro.la
libavro_la_SOURCES = avro.h avro_private.h avro.c avro_value.c avro_primitives.c \
json/json_parser.c json/json_parser.h json/json_tokenizer.c \
json/json.c json/json.h json/json_tokenizer.h \
util/dump.c util/dump.h util/error.c util/error.h \
util/container_of.h util/endian.c \
datatypes/array.c datatypes/boolean.c datatypes/bytes.c datatypes/double.c \
datatypes/enum.c datatypes/fixed.c datatypes/float.c datatypes/int.c datatypes/long.c \
datatypes/map.c datatypes/null.c datatypes/record.c datatypes/string.c datatypes/union.c \
io/file.c io/socket.c io/memory.c io/file_container.c 
libavro_la_LDFLAGS = \
        -version-info $(LIBAVRO_VERSION) \
        -release $(VERSION) \
        -export-dynamic

noinst_PROGRAMS=test_avro_interop
check_PROGRAMS=test_json_parser test_avro_schema test_avro_data

dist-hook: docs

test_LDADD=$(APR_LIBS) $(APU_LIBS) $(top_builddir)/libavro.la

test_json_parser_SOURCES=tests/test_json_parser.c
test_json_parser_LDADD=$(test_LDADD)

test_avro_schema_SOURCES=tests/test_avro_schema.c
test_avro_schema_LDADD=$(test_LDADD)

test_avro_data_SOURCES=tests/test_avro_data.c
test_avro_data_LDADD=$(test_LDADD)
# use -std=c99 to avoid warnings about universal character names
test_avro_data_CFLAGS=$(AM_CFLAGS) -std=c99

test_avro_interop_SOURCES=tests/test_avro_interop.c
test_avro_interop_LDADD=$(test_LDADD)

TESTS=$(check_PROGRAMS)

# Run indent on all files. 
# NOTE: Don't indent avro.h (messes up docs) or lemon.c,lempar.c (messes up parser)
pretty:
	pushd $(top_srcdir); \
	for path in $$(find . -name "*.[c,h]"); \
	do \
	file=$$(basename $$path); \
	if [ $$file = "avro.h" -o $$file = "lemon.c" -o $$file = "lempar.c" -o $$file = "json_parser.c" ]; then \
		continue;\
	fi;\
	indent $$path; \
	done;\
	popd;

docs:
	pushd $(top_srcdir); \
	(cat docs/doxygen.conf; echo "OUTPUT_DIRECTORY=$(C_DOCS_OUTPUT)")| doxygen - ;\
	popd;


.PHONY: docs 

CLEANFILES=$(top_srcdir)/*~ $(top_srcdir)/**/*~ trace.txt json_parser.out lemon


# We don't need to auto-generate the json parser code each time.  It makes
# the build overly complicated and isn't required since the json parser
# doesn't require frequent updates
# 
#lemon: $(top_srcdir)/lemon.c $(top_srcdir)/lempar.c
#	$(CC) -o $@ $(top_srcdir)/lemon.c
#	-cp $(top_srcdir)/lempar.c .
#
#json_parser.c json_parser.h: $(top_srcdir)/json_parser.y lemon
#	-cp $(top_srcdir)/json_parser.y .
#	./lemon json_parser.y
#
# Make sure to generate the json_parser in the source directory before distribution
#dist-hook: json_parser.h json_parser.c
#	@[ $(top_srcdir) = $(top_builddir) ] || rm -f json_parser.y lempar.c json_parser.c json_parser.h
