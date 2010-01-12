#!/bin/bash

set -e						  # exit on error

cd `dirname "$0"`				  # connect to root

VERSION=`cat share/VERSION.txt`

case "$1" in

    test)
	# run lang-specific tests
	(cd lang/java; ant test)
	# (cd lang/py; ant test)
	# (cd lang/c; make test)
	# (cd lang/c++; make test)
	;;

    dist)
	# build source tarball
	mkdir build
	svn export --force . build/avro-src-$VERSION
	mkdir -p dist
        tar czf dist/avro-src-$VERSION.tar.gz build/avro-src-$VERSION
	md5sum dist/avro-src-$VERSION.tar.gz > dist/avro-src-$VERSION.tar.gz.md5
	sha1sum dist/avro-src-$VERSION.tar.gz > dist/avro-src-$VERSION.tar.gz.sha1

	# build lang-specific artifacts
	(cd lang/java; ant dist)

	# (cd lang/py; ant dist)

	# (cd lang/c; make dist)

	# (cd lang/c++; make dist)

	# build docs
	(cd doc; ant)
	(cd build; tar czf - avro-doc-$VERSION) > dist/avro-doc-$VERSION.tar.gz
	;;

    clean)
	rm -rf build dist
	(cd doc; ant clean)

	(cd lang/java; ant clean)

	# (cd lang/py; ant clean)

	# (cd lang/c; make clean)

	# (cd lang/c++; make clean)
	;;

    *)
        echo "Usage: $0 {test|dist|clean}"
        exit 1
esac

exit 0
