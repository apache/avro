
system=$(shell uname)
include System.$(system).mk

INCFLAGS = -I./api -I./parser -I.
INCFLAGS += -isystem$(BOOSTINC)
# include any flags defined in System.X.mk
INCFLAGS += $(SYSTEM_INCFLAGS)

LIBS = $(BOOSTREGEX)

OBJDIR = obj

SOURCES := $(wildcard impl/*.cc)
INCS    := $(wildcard api/*.hh)
files := $(SOURCES:impl/%.cc=%)
OBJS := $(files:%=$(OBJDIR)/%.o)
DEPENDS := $(files:%=$(OBJDIR)/%.d)

OJBS += $(OBJDIR)/parserlib.o

GENERATED = avro.tab.h avro.tab.c lex.yy.cc
GENERATEDSRCS = $(GENERATED:%=parser/%)
GENERATEDobjs = $(addsuffix .o, $(sort $(basename $(GENERATED))) )
GENERATEDOBJS = $(GENERATEDobjs:%=$(OBJDIR)/%)

EXECS = unittest testparser precompile testgen

all : $(OBJDIR)/avrolib.a $(EXECS)

CXXFLAGS = -Wall -Werror -g 

$(OBJDIR)/avrolib.a : $(OBJS) $(GENERATEDOBJS)
	ar ruc $@ $^ 
	ranlib $@

unittest : test/unittest.cc $(OBJDIR)/avrolib.a $(INCS)
	$(CXX) $(CXXFLAGS) $(INCFLAGS) -o $@ $< $(OBJDIR)/avrolib.a $(LIBS)
	./unittest

$(OBJDIR)/%.o : impl/%.cc
	$(CXX) $(CXXFLAGS) $(INCFLAGS) -c -o $@ $<

$(OBJDIR)/%.d : impl/%.cc 
	mkdir -p $(OBJDIR)
	$(CXX) $(CXXFLAGS) $(INCFLAGS) -MM -MP -MT $(OBJDIR)/$*.o -MT $@ -MF $@ $< 

parser/lex.yy.cc : parser/avro.l parser/avro.tab.h
	$(LEX) -oparser/lex.yy.cc parser/avro.l

parser/avro.tab.h parser/avro.tab.c : parser/avro.y
	$(YACC) -d --file-prefix=parser/avro parser/avro.y

$(OBJDIR)/avro.tab.o : parser/avro.tab.c parser/avro.tab.h
	$(CXX) $(CXXFLAGS) $(INCFLAGS) -c -o $@ $< 

$(OBJDIR)/lex.yy.o : parser/lex.yy.cc
	$(CXX) $(CXXFLAGS)  -Wno-unused $(INCFLAGS) -c -o $@ $< 

testparser: test/testparser.cc $(OBJDIR)/avrolib.a 
	$(CXX) $(CXXFLAGS) $(INCFLAGS) -o $@ $^ $(LIBS)

precompile: test/precompile.cc $(OBJDIR)/avrolib.a 
	$(CXX) $(CXXFLAGS) $(INCFLAGS) -o $@ $^ $(LIBS)

test/code.hh: scripts/gen.py precompile jsonschemas/bigrecord
	./precompile < jsonschemas/bigrecord > obj/bigrecord.flat
	python scripts/gen.py < obj/bigrecord.flat > test/code.hh

testgen: test/code.hh

testgen: test/testgen.cc $(OBJDIR)/avrolib.a 
	$(CXX) $(CXXFLAGS) $(INCFLAGS) -o $@ $^ $(LIBS)
	./testgen

-include $(DEPENDS)

clean :
	rm -rf $(EXECS) $(OBJS) $(OBJDIR)/avrolib.a $(GENERATEDSRCS) $(GENERATEDOBJS) core
