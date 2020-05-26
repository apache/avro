#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ===========================================================================
# Bash functions that can be used in this script or exported by using
# source build.sh

change_java_version() {
  local jdk=$1
  if ((jdk)) && [[ -d /usr/local/openjdk-${jdk} ]]; then
    export JAVA_HOME=/usr/local/openjdk-${jdk}
    export PATH=$JAVA_HOME/bin:$PATH
    echo "----------------------"
    echo "Java version switched:"
  else
    echo "Using the current Java version:"
  fi
  echo "  JAVA_HOME=$JAVA_HOME"
  echo "  PATH=$PATH"
  java -version
}

# Stop here if sourcing for functions
[[ "$0" == *"bash" ]] && return 0

# ===========================================================================

set -xe
cd "${0%/*}"

VERSION=$(<share/VERSION.txt)
DOCKER_XTRA_ARGS=""

usage() {
  echo "Usage: $0 {lint|test|dist|sign|clean|veryclean|docker [--args \"docker-args\"]|rat|githooks|docker-test}"
  exit 1
}

(( $# == 0 )) && usage

while (( "$#" ))
do
  target="$1"
  shift

  # Change the JDK from the default for all targets that will eventually require Java (or maven).
  # This only occurs when the JAVA environment variable is set and a Java environment exists in
  # the "standard" location (defined by the openjdk docker images).  This will typically occur in CI
  # builds.  In all other cases, the Java version is taken from the current installation for the user.
  case "$target" in
    lint|test|dist|clean|veryclean|rat)
    change_java_version "$JAVA"
    ;;
  esac

  case "$target" in

    lint)
      for lang_dir in lang/*; do
        (cd "$lang_dir" && ./build.sh lint)
      done
      ;;

    test)
      # run lang-specific tests
      (cd lang/java; ./build.sh test)

      # create interop test data
      mkdir -p build/interop/data
      (cd lang/java/avro; mvn -B -P interop-data-generate generate-resources)

      # install java artifacts required by other builds and interop tests
      mvn -B install -DskipTests
      (cd lang/py && ./build.sh lint test)
      (cd lang/py3 && ./build.sh lint test)
      (cd lang/c; ./build.sh test)
      (cd lang/c++; ./build.sh lint test)
      (cd lang/csharp; ./build.sh test)
      (cd lang/js; ./build.sh lint test)
      (cd lang/ruby; ./build.sh lint test)
      (cd lang/php; ./build.sh test)
      (cd lang/perl; ./build.sh lint test)

      (cd lang/py; ./build.sh interop-data-generate)
      (cd lang/py3; python3 setup.py generate_interop_data \
        --schema-file=../../share/test/schemas/interop.avsc --output-path=../../build/interop/data)
      (cd lang/c; ./build.sh interop-data-generate)
      #(cd lang/c++; make interop-data-generate)
      (cd lang/csharp; ./build.sh interop-data-generate)
      (cd lang/js; ./build.sh interop-data-generate)
      (cd lang/ruby; rake generate_interop)
      (cd lang/php; ./build.sh interop-data-generate)
      (cd lang/perl; ./build.sh interop-data-generate)

      # run interop data tests
      (cd lang/java/ipc; mvn -B test -P interop-data-test)
      (cd lang/py; ./build.sh interop-data-test)
      (cd lang/py3; python3 setup.py test --test-suite avro.tests.test_datafile_interop.TestDataFileInterop)
      (cd lang/c; ./build.sh interop-data-test)
      #(cd lang/c++; make interop-data-test)
      (cd lang/csharp; ./build.sh interop-data-test)
      (cd lang/js; ./build.sh interop-data-test)
      (cd lang/ruby; rake interop)
      (cd lang/php; ./build.sh test-interop)
      (cd lang/perl; ./build.sh interop-data-test)

      # java needs to package the jars for the interop rpc tests
      (cd lang/java/tools; mvn -B package -DskipTests)

      # run interop rpc test
      ./share/test/interop/bin/test_rpc_interop.sh
    ;;

    dist)
      # build source tarball
      mkdir -p build

      SRC_DIR=avro-src-$VERSION
      DOC_DIR=avro-doc-$VERSION

      rm -rf "build/${SRC_DIR}"
      if [ -d .svn ]; then
        svn export --force . "build/${SRC_DIR}"
      elif [ -d .git ]; then
        mkdir -p "build/${SRC_DIR}"
        git archive HEAD | tar -x -C "build/${SRC_DIR}"
      else
        echo "Not SVN and not GIT .. cannot continue"
        exit 255
      fi

      # runs RAT on artifacts
      mvn -N -P rat antrun:run verify

      mkdir -p dist
      (cd build; tar czf "../dist/${SRC_DIR}.tar.gz" "${SRC_DIR}")

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
      cp "lang/perl/Avro-$VERSION.tar.gz" dist/perl/

      # build docs
      (cd doc; ant)
      # add LICENSE and NOTICE for docs
      mkdir -p "build/$DOC_DIR"
      cp doc/LICENSE "build/$DOC_DIR"
      cp doc/NOTICE "build/$DOC_DIR"
      (cd build; tar czf "../dist/avro-doc-$VERSION.tar.gz" "$DOC_DIR")

      cp DIST_README.txt dist/README.txt
      ;;

    sign)
      set +x

      echo -n "Enter password: "
      stty -echo
      read -r password
      stty echo

      for f in $(find dist -type f \
        \! -name '*.md5' \! -name '*.sha1' \
        \! -name '*.sha512' \! -name '*.sha256' \
        \! -name '*.asc' \! -name '*.txt' );
      do
        (cd "${f%/*}" && shasum -a 512 "${f##*/}") > "$f.sha512"
        gpg --passphrase "$password" --armor --output "$f.asc" --detach-sig "$f"
      done

      set -x
      ;;

    clean)
      rm -rf build dist
      (cd doc; ant clean)

      (mvn -B clean)
      rm -rf lang/java/*/userlogs/
      rm -rf lang/java/*/dependency-reduced-pom.xml

      (cd lang/py; ./build.sh clean)
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

    veryclean)
      rm -rf build dist
      (cd doc; ant clean)

      (mvn -B clean)
      rm -rf lang/java/*/userlogs/
      rm -rf lang/java/*/dependency-reduced-pom.xml

      (cd lang/py; ./build.sh clean)
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

      rm -rf lang/c++/build
      rm -rf lang/js/node_modules
      rm -rf lang/perl/inc/
      rm -rf lang/ruby/.gem/
      rm -rf lang/ruby/Gemfile.lock
      rm -rf lang/py/lib/ivy-2.2.0.jar
      rm -rf lang/csharp/src/apache/ipc.test/bin/
      rm -rf lang/csharp/src/apache/ipc.test/obj
      ;;

    docker)
      if [[ $1 =~ ^--args ]]; then
        DOCKER_XTRA_ARGS=$2
        shift 2
      fi
      if [[ "$(uname -s)" = Linux ]]; then
        USER_NAME=${SUDO_USER:=$USER}
        USER_ID=$(id -u "$USER_NAME")
        GROUP_ID=$(id -g "$USER_NAME")
      else # boot2docker uid and gid
        USER_NAME=$USER
        USER_ID=1000
        GROUP_ID=50
      fi
      {
        cat share/docker/Dockerfile
        grep -vF 'FROM avro-build-ci' share/docker/DockerfileLocal
        echo "ENV HOME /home/$USER_NAME"
        echo "RUN getent group $GROUP_ID || groupadd -g $GROUP_ID $USER_NAME"
        echo "RUN getent passwd $USER_ID || useradd -g $GROUP_ID -u $USER_ID -k /root -m $USER_NAME"
      } > Dockerfile
      tar -cf- lang/ruby/Gemfile Dockerfile | docker build -t "avro-build-$USER_NAME" -
      rm Dockerfile
      # By mapping the .m2 directory you can do an mvn install from
      # within the container and use the result on your normal
      # system.  And this also is a significant speedup in subsequent
      # builds because the dependencies are downloaded only once.
      #
      # On OSX, it's highly suggested to set an env variable of:
      # export DOCKER_MOUNT_FLAG=":delegated"
      # Using :delegated will drop the "mvn install" time from over 30 minutes
      # down to under 10.  However, editing files from OSX may take a few
      # extra second before the changes are available within the docker container.
      docker run --rm -t -i \
        --env "JAVA=${JAVA:-8}" \
        --user "${USER_NAME}" \
        --volume "${HOME}/.gnupg:/home/${USER_NAME}/.gnupg" \
        --volume "${HOME}/.m2:/home/${USER_NAME}/.m2${DOCKER_MOUNT_FLAG}" \
        --volume "${PWD}:/home/${USER_NAME}/avro${DOCKER_MOUNT_FLAG}" \
        --workdir "/home/${USER_NAME}/avro" \
        ${DOCKER_XTRA_ARGS} \
        "avro-build-${USER_NAME}" bash
      ;;

    rat)
      mvn test -Dmaven.main.skip=true -Dmaven.test.skip=true -DskipTests=true -P rat -pl :avro-toplevel
      ;;

    githooks)
      echo "Installing AVRO git hooks."
      cp share/githooks/* .git/hooks
      chmod +x .git/hooks/*
      chmod -x .git/hooks/*sample*
      ;;

    docker-test)
      tar -cf- share/docker/Dockerfile lang/ruby/Gemfile |
        docker build -t avro-test -f share/docker/Dockerfile -
      docker run --rm -v "${PWD}:/avro/" --env "JAVA=${JAVA:-8}" avro-test /avro/share/docker/run-tests.sh
      ;;

    *)
      usage
      ;;
  esac
done
