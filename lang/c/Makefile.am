EXTRA_DIST=version.sh m4
SUBDIRS = docs jansson src tests

pretty:
	@for dir in src tests; do \
	indent $(top_srcdir)/$$dir/*.[c,h]; \
	mv $(top_srcdir)/$$dir/*~ /tmp; \
	done
