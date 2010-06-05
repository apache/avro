EXTRA_DIST = index.txt index.html

SUFFIXES = .html

.txt.html:
	asciidoc --unsafe --attribute=avro_version=@VERSION@ --attribute=libavro_version=@LIBAVRO_VERSION@ --attribute=toc -n -o $@ $<

docs: index.html
