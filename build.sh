#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e						  # exit on error

cd `dirname "$0"`				  # connect to root

VERSION=`cat share/VERSION.txt`

function usage {
  echo "Usage: $0 {test|dist|sign|clean}"
  exit 1
}

if [ $# -eq 0 ]
then
  usage
fi

set -x						  # echo commands

for target in "$@"
do

case "$target" in

    test)
	# run lang-specific tests
        (cd lang/java; mvn test)
	(cd lang/py; ant test)
	(cd lang/c; ./build.sh test)
	(cd lang/c++; ./build.sh test)
	(cd lang/csharp; ./build.sh test)
	(cd lang/js; ./build.sh test)
	(cd lang/ruby; ./build.sh test)
	(cd lang/php; ./build.sh test)

	# create interop test data
        mkdir -p build/interop/data
	(cd lang/java/avro; mvn -P interop-data-generate generate-resources)
	(cd lang/py; ant interop-data-generate)
	(cd lang/c; ./build.sh interop-data-generate)
	#(cd lang/c++; make interop-data-generate)
	(cd lang/ruby; rake generate_interop)
	(cd lang/php; ./build.sh interop-data-generate)

	# run interop data tests
	(cd lang/java; mvn test -P interop-data-test)
	(cd lang/py; ant interop-data-test)
	(cd lang/c; ./build.sh interop-data-test)
	#(cd lang/c++; make interop-data-test)
	(cd lang/ruby; rake interop)
	(cd lang/php; ./build.sh test-interop)

	# java needs to package the jars for the interop rpc tests
        (cd lang/java; mvn package -DskipTests)
	# run interop rpc test
        /bin/bash share/test/interop/bin/test_rpc_interop.sh

	;;

    dist)
        # ensure version matches
        # FIXME: enforcer is broken:MENFORCER-42
        # mvn enforcer:enforce -Davro.version=$VERSION
        
	# build source tarball
        mkdir -p build

        SRC_DIR=avro-src-$VERSION

	rm -rf build/${SRC_DIR}
	svn export --force . build/${SRC_DIR}

	#runs RAT on artifacts
        mvn -N -P rat antrun:run

	mkdir -p dist
        (cd build; tar czf ../dist/${SRC_DIR}.tar.gz ${SRC_DIR})

	# build lang-specific artifacts
        
	(cd lang/java; mvn package -DskipTests -Dhadoop.version=2; rm -rf mapred/target/classes/;
	  mvn -P dist package -DskipTests -Davro.version=$VERSION javadoc:aggregate) 
        (cd lang/java/trevni/doc; mvn site)
        (mvn -N -P copy-artifacts antrun:run) 

	(cd lang/py; ant dist)

	(cd lang/c; ./build.sh dist)

	(cd lang/c++; ./build.sh dist)

	(cd lang/csharp; ./build.sh dist)

	(cd lang/js; ./build.sh dist)

	(cd lang/ruby; ./build.sh dist)

	(cd lang/php; ./build.sh dist)

	# build docs
	(cd doc; ant)
	(cd build; tar czf ../dist/avro-doc-$VERSION.tar.gz avro-doc-$VERSION)

	cp DIST_README.txt dist/README.txt
	;;

    sign)

	set +x

	echo -n "Enter password: "
	stty -echo
	read password
	stty echo

	for f in $(find dist -type f \
	    \! -name '*.md5' \! -name '*.sha1' \
	    \! -name '*.asc' \! -name '*.txt' );
	do
	    (cd `dirname $f`; md5sum `basename $f`) > $f.md5
	    (cd `dirname $f`; sha1sum `basename $f`) > $f.sha1
	    gpg --passphrase $password --armor --output $f.asc --detach-sig $f
	done

	set -x
	;;

    clean)
	rm -rf build dist
	(cd doc; ant clean)

        (mvn clean)         

	(cd lang/py; ant clean)

	(cd lang/c; ./build.sh clean)

	(cd lang/c++; ./build.sh clean)

	(cd lang/csharp; ./build.sh clean)

	(cd lang/js; ./build.sh clean)

	(cd lang/ruby; ./build.sh clean)

	(cd lang/php; ./build.sh clean)
	;;

    *)
        usage
        ;;
esac

done

exit 0
