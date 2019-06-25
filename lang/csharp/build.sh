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

set -e                # exit on error
set -x

cd `dirname "$0"`                  # connect to root

ROOT=../..
VERSION=`cat $ROOT/share/VERSION.txt`

case "$1" in

  test)
    dotnet build --configuration Release --framework netcoreapp2.0 ./src/apache/codegen/Avro.codegen.csproj
    dotnet build --configuration Release --framework netstandard2.0 ./src/apache/msbuild/Avro.msbuild.csproj
    dotnet test --configuration Release --framework netcoreapp2.0 ./src/apache/test/Avro.test.csproj
    ;;

  perf)
    pushd ./src/apache/perf/
    dotnet run --configuration Release --framework netcoreapp2.0
    ;;

  dist)
    # build binary tarball
    dotnet build --configuration Release --framework netcoreapp2.0 ./src/apache/codegen/Avro.codegen.csproj
    dotnet build --configuration Release --framework netstandard2.0 ./src/apache/msbuild/Avro.msbuild.csproj
    dotnet build --configuration Release --framework netstandard2.0 ./src/apache/ipc/Avro.ipc.csproj

    # add the binary LICENSE and NOTICE to the tarball
    mkdir build/
    cp LICENSE NOTICE build/

    # add binaries to the tarball
    mkdir build/main/
    cp -R src/apache/main/bin/Release/* build/main/
    mkdir build/codegen/
    cp -R src/apache/codegen/bin/Release/* build/codegen/
    mkdir build/ipc/
    cp -R src/apache/ipc/bin/Release/* build/ipc/

    # build the tarball
    mkdir -p ${ROOT}/dist/csharp
    (cd build; tar czf ${ROOT}/../dist/csharp/avro-csharp-${VERSION}.tar.gz main codegen ipc LICENSE NOTICE)

    # build documentation
    doxygen Avro.dox
    mkdir -p ${ROOT}/build/avro-doc-${VERSION}/api/csharp
    cp -pr build/doc/* ${ROOT}/build/avro-doc-${VERSION}/api/csharp
    ;;

  clean)
    rm -rf src/apache/{main,test,codegen,ipc,msbuild,perf}/{obj,bin}
    rm -rf build
    rm -f  TestResult.xml
    ;;

  *)
    echo "Usage: $0 {test|clean|dist|perf}"
    exit 1
esac

exit 0
