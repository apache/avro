# -pedantic
AM_CPPFLAGS=-I$(top_srcdir)/src 
AM_CFLAGS=-Wall 
ACLOCAL_AMFLAGS=-I m4

check_PROGRAMS=quickstop
TESTS=$(check_PROGRAMS)

examples_LDADD=$(top_builddir)/src/libavro.la

quickstop_SOURCES=quickstop.c
quickstop_LDADD=$(examples_LDADD)
