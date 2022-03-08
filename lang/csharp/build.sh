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

set -e

ROOT=../..

BUILD_DESCRIPTION="Build script for Apache Avro C#"

CSHARP_CODEC_LIBS="Avro.File.Snappy Avro.File.BZip2 Avro.File.XZ Avro.File.Zstandard"
SUPPORTED_SDKS="3.1 5.0 6.0"
DEFAULT_FRAMEWORK="net6.0"
CONFIGURATION="Release"

function command_lint()
{
  echo "This is a stub where someone can provide linting."
}

function command_test()
{
  $OPTION_DRY_RUN dotnet build --configuration $CONFIGURATION Avro.sln

  # AVRO-2442: Explicitly set LANG to work around ICU bug in `dotnet test`
  $OPTION_DRY_RUN dotnet test --configuration "$CONFIGURATION" --no-build \
      --filter "TestCategory!=Interop" Avro.sln
}

function command_perf()
{
  $OPTION_DRY_RUN pushd ./src/apache/perf/
  $OPTION_DRY_RUN dotnet run --configuration "$CONFIGURATION" --framework "$FRAMEWORK"
}

function command_dist()
{
  # pack NuGet packages
  $OPTION_DRY_RUN dotnet pack --configuration "$CONFIGURATION" Avro.sln

  # add the binary LICENSE and NOTICE to the tarball
  $OPTION_DRY_RUN mkdir -p build/
  $OPTION_DRY_RUN cp LICENSE NOTICE build/

  # add binaries to the tarball
  $OPTION_DRY_RUN mkdir -p build/main/
  $OPTION_DRY_RUN cp -R src/apache/main/bin/$CONFIGURATION/* build/main/
  # add codec binaries to the tarball
  for codec in $CSHARP_CODEC_LIBS
  do
    $OPTION_DRY_RUN mkdir -p build/codec/$codec/
    $OPTION_DRY_RUN cp -R src/apache/codec/$codec/bin/$CONFIGURATION/* build/codec/$codec/
  done
  # add codegen binaries to the tarball
  $OPTION_DRY_RUN mkdir -p build/codegen/
  $OPTION_DRY_RUN cp -R src/apache/codegen/bin/$CONFIGURATION/* build/codegen/

  # build the tarball
  $OPTION_DRY_RUN mkdir -p ${ROOT}/dist/csharp
  $OPTION_DRY_RUN pushd cd build && tar czf ${ROOT}/../dist/csharp/avro-csharp-${VERSION}.tar.gz main codegen LICENSE NOTICE && popd

  # build documentation
  $OPTION_DRY_RUN doxygen Avro.dox
  $OPTION_DRY_RUN mkdir -p ${ROOT}/build/avro-doc-${VERSION}/api/csharp
  $OPTION_DRY_RUN cp -pr build/doc/* ${ROOT}/build/avro-doc-${VERSION}/api/csharp
}

function command_release()
{
  command_dist

  # Push packages to nuget.org
  # Note: use loop instead of -exec or xargs to stop at first failure
  for package in $(find ./build/ -name '*.nupkg' -type f)
  do
    ask "Push $package to nuget.org" && $OPTION_DRY_RUN dotnet nuget push "$package" -k "$OPTION_NUGET_KEY" -s "$OPTION_NUGET_SOURCE"
  done
}
    
function command_verify-release()
{
  for sdk_ver in $SUPPORTED_SDKS
  do
    $OPTION_DRY_RUN docker run -it --rm mcr.microsoft.com/dotnet/sdk:$sdk_ver /bin/bash -ce "\
      mkdir test-project && \
      cd test-project && \
      dotnet new console && \
      dotnet add package Apache.Avro --version $VERSION && \
      for codec in $CSHARP_CODEC_LIBS; do \
        dotnet add package Apache.\$codec --version $VERSION; \
      done && \
      dotnet build && \
      dotnet tool install --global Apache.Avro.Tools --version $VERSION && \
      export PATH=\$PATH:/root/.dotnet/tools && \
      avrogen --help"
  done
  echo "Verified"
}

function command_interop-data-generate()
{
  $OPTION_DRY_RUN dotnet run --project src/apache/test/Avro.test.csproj --framework $DEFAULT_FRAMEWORK ../../share/test/schemas/interop.avsc ../../build/interop/data
}

function command_interop-data-test()
{
  $OPTION_DRY_RUN dotnet test --filter "TestCategory=Interop" --logger "console;verbosity=normal;noprogress=true" src/apache/test/Avro.test.csproj
}

function command_clean()
{
  $OPTION_DRY_RUN rm -rf src/apache/{main,test,codegen,ipc,msbuild,perf}/{obj,bin}
  $OPTION_DRY_RUN rm -rf build
  $OPTION_DRY_RUN rm -f  TestResult.xml
}

source $ROOT/share/build-helper.sh
