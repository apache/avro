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

set -e                # exit on error

cd `dirname "$0"`     # connect to root

VERSION=`cat share/VERSION.txt`

function usage {
  echo "Usage: $0 {test|dist|sign|clean|docker|rat|githooks}"
  exit 1
}

if [ $# -eq 0 ]
then
  usage
fi

set -x                # echo commands

for target in "$@"
do
  case "$target" in

    test)
      # run lang-specific tests
      (cd lang/java; ./build.sh test)
      # install java artifacts required by other builds and interop tests
      mvn install -DskipTests
      (cd lang/py; ./build.sh test)
      (cd lang/py3; ./build.sh test)
      (cd lang/c; ./build.sh test)
      (cd lang/c++; ./build.sh test)
      (cd lang/csharp; ./build.sh test)
      (cd lang/js; ./build.sh test)
      (cd lang/ruby; ./build.sh test)
      (cd lang/php; ./build.sh test)
      (cd lang/perl; ./build.sh test)

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
      DOC_DIR=avro-doc-$VERSION

      rm -rf build/${SRC_DIR}
      if [ -d .svn ];
      then
        svn export --force . build/${SRC_DIR}
      elif [ -d .git ];
      then
        mkdir -p build/${SRC_DIR}
        git archive HEAD | tar -x -C build/${SRC_DIR}
      else
        echo "Not SVN and not GIT .. cannot continue"
        exit -1;
      fi

      #runs RAT on artifacts
      mvn -N -P rat antrun:run

      mkdir -p dist
      (cd build; tar czf ../dist/${SRC_DIR}.tar.gz ${SRC_DIR})

      # build lang-specific artifacts

      (cd lang/java;./build.sh dist; mvn install -pl tools -am -DskipTests)
      (cd lang/java/trevni/doc; mvn site)
      (mvn -N -P copy-artifacts antrun:run)

      (cd lang/py; ./build.sh dist)
      (cd lang/py3; ./build.sh dist)

      (cd lang/c; ./build.sh dist)

      (cd lang/c++; ./build.sh dist)

      (cd lang/csharp; ./build.sh dist)

      (cd lang/js; ./build.sh dist)

      (cd lang/ruby; ./build.sh dist)

      (cd lang/php; ./build.sh dist)

      mkdir -p dist/perl
      (cd lang/perl; ./build.sh dist)
      cp lang/perl/Avro-$VERSION.tar.gz dist/perl/

      # build docs
      (cd doc; ant)
      # add LICENSE and NOTICE for docs
      mkdir -p build/$DOC_DIR
      cp doc/LICENSE build/$DOC_DIR
      cp doc/NOTICE build/$DOC_DIR
      (cd build; tar czf ../dist/avro-doc-$VERSION.tar.gz $DOC_DIR)

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
      rm -rf lang/java/*/userlogs/
      rm -rf lang/java/*/dependency-reduced-pom.xml

      (cd lang/py; ant clean)
      rm -rf lang/py/userlogs/

      (cd lang/py3; python3 setup.py clean)
      rm -rf lang/py3/dist
      rm -rf lang/py3/avro_python3.egg-info
      rm -f  lang/py3/avro/*.avsc
      rm -f  lang/py3/avro/VERSION.txt
      rm -rf lang/py3/avro/__pycache__/
      rm -f  lang/py3/avro/tests/interop.avsc
      rm -rf lang/py3/avro/tests/__pycache__/

      (cd lang/c; ./build.sh clean)

      (cd lang/c++; ./build.sh clean)

      (cd lang/csharp; ./build.sh clean)

      (cd lang/js; ./build.sh clean)

      (cd lang/ruby; ./build.sh clean)

      (cd lang/php; ./build.sh clean)

      (cd lang/perl; ./build.sh clean)
      ;;

    docker)
      docker build -t avro-build share/docker
      if [ "$(uname -s)" == "Linux" ]; then
        USER_NAME=${SUDO_USER:=$USER}
        USER_ID=$(id -u $USER_NAME)
        GROUP_ID=$(id -g $USER_NAME)
      else # boot2docker uid and gid
        USER_NAME=$USER
        USER_ID=1000
        GROUP_ID=50
      fi
      docker build -t avro-build-${USER_NAME} - <<UserSpecificDocker
FROM docker.io/surajacharya/avro-build-test
RUN groupadd -g ${GROUP_ID} ${USER_NAME} || true
RUN useradd -g ${GROUP_ID} -u ${USER_ID} -k /root -m ${USER_NAME} || true
ENV HOME /home/${USER_NAME}
RUN apt-get update && apt-get install --no-install-recommends -y \
  git subversion curl ant make maven \
  gcc cmake libjansson-dev asciidoc source-highlight \
  g++ flex bison libboost-all-dev doxygen \
  mono-devel mono-gmcs nunit \
  nodejs \
  perl \
  php5 phpunit php5-gmp bzip2 \
  python python-setuptools python3-setuptools \
  ruby ruby-dev rake \
  libsnappy1 libsnappy-dev
UserSpecificDocker

docker build -t avro-build-test - <<UserSpecificDocker
FROM docker.io/surajacharya/avro-build-test
RUN apt-get install -y gem
RUN gem install ruby-lint rubocop
UserSpecificDocker
      # By mapping the .m2 directory you can do an mvn install from
      # within the container and use the result on your normal
      # system.  And this also is a significant speedup in subsequent
      # builds because the dependencies are downloaded only once.
      docker run --rm=true -t -i \
        -v ${PWD}:/home/${USER_NAME}/avro \
        -w /home/${USER_NAME}/avro \
        -v ${HOME}/.m2:/home/${USER_NAME}/.m2 \
        -v ${HOME}/.gnupg:/home/${USER_NAME}/.gnupg \
        -u ${USER_NAME} \
        avro-build-${USER_NAME}
      ;;

    rat)
      mvn test -Dmaven.main.skip=true -Dmaven.test.skip=true -DskipTests=true -P rat -pl :avro-toplevel
      ;;

    githooks)
      echo "Installing AVRO git hooks."
      cp share/githooks/* .git/hooks
      find .git/hooks/ -type f | fgrep -v sample | xargs chmod 755
      ;;

    *)
      usage
      ;;
  esac

done

exit 0
