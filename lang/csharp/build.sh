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

BUILD_DESCRIPTION="Build script for Apache Avro C#"
source ../../share/build-helper.sh

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
  execute dotnet build --configuration $CONFIGURATION Avro.sln

  # AVRO-2442: Explicitly set LANG to work around ICU bug in `dotnet test`
  execute LANG=en_US.UTF-8 dotnet test --configuration "$CONFIGURATION" --no-build --filter "TestCategory!=Interop" Avro.sln
}

function command_perf()
{
  execute pushd ./src/apache/perf/
  execute dotnet run --configuration "$CONFIGURATION" --framework "$FRAMEWORK"
}

function command_dist()
{
  # pack NuGet packages
  execute dotnet pack --configuration "$CONFIGURATION" Avro.sln

  # add the binary LICENSE and NOTICE to the tarball
  execute mkdir -p build/
  execute cp LICENSE NOTICE build/

  # add binaries to the tarball
  execute mkdir -p build/main/
  execute cp -R src/apache/main/bin/$CONFIGURATION/* build/main/
  # add codec binaries to the tarball
  for codec in $CSHARP_CODEC_LIBS
  do
    execute mkdir -p build/codec/$codec/
    execute cp -R src/apache/codec/$codec/bin/$CONFIGURATION/* build/codec/$codec/
  done
  # add codegen binaries to the tarball
  execute mkdir -p build/codegen/
  execute cp -R src/apache/codegen/bin/$CONFIGURATION/* build/codegen/

  # build the tarball
  execute mkdir -p ${BUILD_ROOT}/dist/csharp
  execute pushd build
  execute tar czf ${BUILD_ROOT}/../dist/csharp/avro-csharp-${BUILD_VERSION}.tar.gz main codegen LICENSE NOTICE
  execute popd

  # build documentation
  execute doxygen Avro.dox
  execute mkdir -p ${BUILD_ROOT}/build/avro-doc-${BUILD_VERSION}/api/csharp
  execute cp -pr build/doc/* ${BUILD_ROOT}/build/avro-doc-${BUILD_VERSION}/api/csharp
}

function command_release()
{
  command_dist

  # Push packages to nuget.org
  # Note: use loop instead of -exec or xargs to stop at first failure
  for package in $(find ./build/ -name '*.nupkg' -type f)
  do
    ask "Push $package to nuget.org" && execute dotnet nuget push "$package" -k "$OPTION_NUGET_KEY" -s "$OPTION_NUGET_SOURCE"
  done
}
    
function command_verify-release()
{
  for sdk_ver in $SUPPORTED_SDKS
  do
    execute docker run -it --rm mcr.microsoft.com/dotnet/sdk:$sdk_ver /bin/bash -ce "\
      mkdir test-project && \
      cd test-project && \
      dotnet new console && \
      dotnet add package Apache.Avro --version $BUILD_VERSION && \
      for codec in $CSHARP_CODEC_LIBS; do \
        dotnet add package Apache.\$codec --version $BUILD_VERSION; \
      done && \
      dotnet build && \
      dotnet tool install --global Apache.Avro.Tools --version $BUILD_VERSION && \
      export PATH=\$PATH:/root/.dotnet/tools && \
      avrogen --help"
  done
  echo "Verified"
}

function command_interop-data-generate()
{
  execute dotnet run --project src/apache/test/Avro.test.csproj --framework $DEFAULT_FRAMEWORK ../../share/test/schemas/interop.avsc ../../build/interop/data
}

function command_interop-data-test()
{
  execute LANG=en_US.UTF-8 dotnet test --filter 'TestCategory=Interop' --logger 'console\;verbosity=normal\;noprogress=true' src/apache/test/Avro.test.csproj
}

function command_clean()
{
  execute rm -rf src/apache/{main,test,codegen,ipc,msbuild,perf}/{obj,bin}
  execute rm -rf build
  execute rm -f  TestResult.xml
}

build-run "$@"