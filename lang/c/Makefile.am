EXTRA_DIST=version.sh m4
SUBDIRS = docs jansson src tests examples

# Linux kernel source indent format options
INDENT_OPTS=-nbad -bap -nbc -bbo -hnl -br -brs -c33 -cd33 -ncdb -ce -ci4 \
-cli0 -d0 -di1 -nfc1 -i8 -ip0 -l80 -lp -npcs -nprs -npsl -sai \
-saf -saw -ncs -nsc -sob -nfca -cp33 -ss -ts8

cscope:
	cd $(top_srcdir) ; \
	find src tests examples jansson -iname '*.[chsSly]' > cscope.files ; \
	cscope -kqb

pretty:
	@for dir in src tests examples; do \
	indent $(INDENT_OPTS) $(top_srcdir)/$$dir/*.[c,h]; \
	mv $(top_srcdir)/$$dir/*~ /tmp; \
	done
