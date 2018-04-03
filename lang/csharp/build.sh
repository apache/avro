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
set -x

cd `dirname "$0"`                  # connect to root

ROOT=../..
VERSION=`cat $ROOT/share/VERSION.txt`

export CONFIGURATION=Release
export TARGETFRAMEWORKVERSION=v3.5

case "$1" in

  test)
    xbuild
    nunit-console Avro.nunit
    ;;

  perf)
    xbuild
    mono build/perf/Release/Avro.perf.exe
    ;;

  dist)
    # build binary tarball
    xbuild
    # add the binary LICENSE and NOTICE to the tarball
    cp LICENSE NOTICE build/
    mkdir -p $ROOT/dist/csharp
    (cd build; tar czf $ROOT/../dist/csharp/avro-csharp-$VERSION.tar.gz main codegen ipc LICENSE NOTICE)

    # build documentation
    doxygen Avro.dox
    mkdir -p $ROOT/build/avro-doc-$VERSION/api/csharp
    cp -pr build/doc/* $ROOT/build/avro-doc-$VERSION/api/csharp
    ;;

  clean)
    rm -rf src/apache/{main,test,codegen,ipc,msbuild,perf}/obj
    rm -rf build
    rm -f  TestResult.xml
    ;;

  *)
    echo "Usage: $0 {test|clean|dist|perf}"
    exit 1
esac

exit 0
