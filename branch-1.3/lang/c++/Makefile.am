
AM_CXXFLAGS= -I$(top_srcdir)/api $(BOOST_CPPFLAGS)
ACLOCAL_AMFLAGS=-I m4

BOOST_CPPFLAGS = @BOOST_CPPFLAGS@
BOOST_LDFLAGS = @BOOST_LDFLAGS@
BOOST_REGEX_LIB = @BOOST_REGEX_LIB@
PYTHON = @PYTHON@

library_includedir=$(includedir)/avrocpp
library_include_HEADERS = \
api/AvroParse.hh \
api/AvroSerialize.hh \
api/AvroTraits.hh \
api/Boost.hh \
api/Compiler.hh \
api/CompilerNode.hh \
api/Exception.hh \
api/InputStreamer.hh \
api/Layout.hh \
api/Node.hh \
api/NodeConcepts.hh \
api/NodeImpl.hh \
api/OutputStreamer.hh \
api/Parser.hh \
api/Reader.hh \
api/Resolver.hh \
api/ResolverSchema.hh \
api/ResolvingReader.hh \
api/Schema.hh \
api/SchemaResolution.hh \
api/Serializer.hh \
api/SymbolMap.hh \
api/Types.hh \
api/ValidSchema.hh \
api/ValidatingReader.hh \
api/ValidatingWriter.hh \
api/Validator.hh \
api/Writer.hh \
api/Zigzag.hh 

BUILT_SOURCES = AvroYacc.h testgen.hh testgen2.hh

bin_PROGRAMS = precompile testparser 
bin_SCRIPTS = scripts/gen-cppcode.py

precompile_SOURCES = test/precompile.cc

precompile_LDFLAGS = -static $(BOOST_LDFLAGS)
precompile_LDADD = $(top_builddir)/libavrocpp.la $(BOOST_REGEX_LIB)

testparser_SOURCES = test/testparser.cc

testparser_LDFLAGS = -static -no-install $(BOOST_LDFLAGS)
testparser_LDADD = $(top_builddir)/libavrocpp.la $(BOOST_REGEX_LIB)

lib_LTLIBRARIES = libavrocpp.la

libavrocpp_la_SOURCES = \
api/AvroParse.hh \
api/AvroSerialize.hh \
api/AvroTraits.hh \
api/Boost.hh \
api/Compiler.hh \
api/CompilerNode.hh \
api/Exception.hh \
api/InputStreamer.hh \
api/Layout.hh \
api/Node.hh \
api/NodeConcepts.hh \
api/NodeImpl.hh \
api/OutputStreamer.hh \
api/Parser.hh \
api/Reader.hh \
api/Resolver.hh \
api/ResolverSchema.hh \
api/ResolvingReader.hh \
api/Schema.hh \
api/SchemaResolution.hh \
api/Serializer.hh \
api/SymbolMap.hh \
api/Types.hh \
api/ValidSchema.hh \
api/ValidatingReader.hh \
api/ValidatingWriter.hh \
api/Validator.hh \
api/Writer.hh \
api/Zigzag.hh \
impl/Compiler.cc \
impl/CompilerNode.cc \
impl/Node.cc \
impl/NodeImpl.cc \
impl/Resolver.cc \
impl/ResolverSchema.cc \
impl/Schema.cc \
impl/Types.cc \
impl/ValidSchema.cc \
impl/ValidatingReader.cc \
impl/ValidatingWriter.cc \
impl/Validator.cc \
impl/Zigzag.cc \
parser/AvroYacc.yy \
parser/AvroLex.ll 

# libavrocpp_la_LDFLAGS = -export-dynamic

AM_LFLAGS= -o$(LEX_OUTPUT_ROOT).c
AM_YFLAGS = -d

check_PROGRAMS = unittest testgen 

TESTS=unittest testgen
TESTS_ENVIRONMENT = top_srcdir=$(top_srcdir)

unittest_SOURCES = test/unittest.cc
unittest_LDFLAGS = -static -no-install $(BOOST_LDFLAGS)
unittest_LDADD = $(top_builddir)/libavrocpp.la $(BOOST_REGEX_LIB)

testgen_SOURCES = test/testgen.cc testgen.hh testgen2.hh
testgen_CXXFLAGS = $(AM_CXXFLAGS) -Wno-invalid-offsetof  
testgen_LDFLAGS = -static -no-install $(BOOST_LDFLAGS)
testgen_LDADD = $(top_builddir)/libavrocpp.la $(BOOST_REGEX_LIB)

# Make sure we never package up '.svn' directories
dist-hook:
	find $(distdir) -name '.svn' | xargs rm -rf

testgen.hh : bigrecord.precompile 
	$(PYTHON) $(top_srcdir)/scripts/gen-cppcode.py -n testgen -i $< -o $@

testgen2.hh : bigrecord2.precompile 
	$(PYTHON) $(top_srcdir)/scripts/gen-cppcode.py -n testgen2 -i $< -o $@

bigrecord.precompile: $(top_srcdir)/jsonschemas/bigrecord precompile$(EXEEXT)
	$(top_builddir)/precompile$(EXEEXT) < $< > $@

bigrecord2.precompile: $(top_srcdir)/jsonschemas/bigrecord2 precompile$(EXEEXT) 
	$(top_builddir)/precompile$(EXEEXT) < $< > $@

DOXYGEN_INPUTS= $(top_srcdir)/MainPage.dox $(patsubst %,$(top_srcdir)/%, $(library_include_HEADERS))

CPP_DOC_DIR ?= "$(top_builddir)/doc"

doc: $(CPP_DOC_DIR)/html/index.html

$(CPP_DOC_DIR)/html/index.html: $(DOXYGEN_INPUTS) $(srcdir)/Doxyfile
	(cat $(srcdir)/Doxyfile; echo "OUTPUT_DIRECTORY=$(CPP_DOC_DIR)";  echo "INPUT=$(DOXYGEN_INPUTS)") | $(DOXYGEN) - ;

EXTRA_DIST=jsonschemas scripts

CLEANFILES=bigrecord.precompile bigrecord2.precompile testgen.hh testgen2.hh AvroLex.cc AvroYacc.cc AvroYacc.h test.avro

clean-local: clean-local-check
.PHONY: clean-local-check
clean-local-check:
	-rm -rf $(CPP_DOC_DIR)

