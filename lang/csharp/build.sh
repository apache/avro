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

cd "$(dirname "$0")"                  # connect to root

ROOT=../..
VERSION="$(cat $ROOT/share/VERSION.txt)"

CSHARP_CODEC_LIBS="Avro.File.Snappy Avro.File.BZip2 Avro.File.XZ Avro.File.Zstandard"
SUPPORTED_SDKS="3.1 5.0 6.0"

OPTION_YES="0"
OPTION_DRY_RUN=""
OPTION_NUGET_KEY=""
OPTION_NUGET_SOURCE="https://api.nuget.org/v3/index.json"

function usage()
{
    echo "Usage: $(basename "$0") [OPTION]... [COMMAND]..."
    echo "Build script for Apache Avro C#"
    echo ""
    echo "Options:"
    echo "  -y, --yes                             Answer yes to all question"
    echo "      --dry-run                         Dont execute commands, just echo them"
    echo "      --nuget-source NUGET_SOURCE       Nuget source (default: $OPTION_DRY_RUN)"
    echo "      --nuget-key NUGET_KEY             Nuget key"
    echo "  -v, --verbode                         Verbose output"
    echo "  -V, --version                         Version"
    echo "  -h, --help                            Shows help"
    echo ""
    echo "Commands:"
    echo "  lint"
    echo "  test"
    echo "  clean"
    echo "  dist"
    echo "  release"
    echo "  verify-release"
    echo "  perf"
    echo "  interop-data-generate"
    echo "  interop-data-test"
}

function ask
{
  [ "$OPTION_YES" == "1" ] && return 0
  [ "$OPTION_DRY_RUN" == "1" ] && return 0
  while true; do
    read -r -n 1 -p "$1 ([y]es/[n]o/([a]bort)? " answer
    echo
    case $answer in
        [Yy]) return 0; break;;
        [Nn]) echo "Skip..."; return 1; break;;
        [Aa]) echo "ABORT..."; exit 1;;
    esac
  done
}

# Iterate through arguments
while [ $# -gt 0 ]
do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;

    -v|--verbose)
      set -x
      ;;

    -y|--yes)
      OPTION_YES="1"
      ;;

    --dry-run)
      OPTION_DRY_RUN="echo"
      ;;

    --nuget-key)
      OPTION_NUGET_KEY="$2"
      shift
      ;;

    lint)
      echo 'This is a stub where someone can provide linting.'
      ;;

    test)
      $OPTION_DRY_RUN dotnet build --configuration Release Avro.sln

      # AVRO-2442: Explicitly set LANG to work around ICU bug in `dotnet test`
      $OPTION_DRY_RUN LANG=en_US.UTF-8 dotnet test --configuration Release --no-build \
          --filter "TestCategory!=Interop" Avro.sln
      ;;

    perf)
      $OPTION_DRY_RUN pushd ./src/apache/perf/
      $OPTION_DRY_RUN dotnet run --configuration Release --framework net6.0
      ;;

    dist|release)
      # pack NuGet packages
      $OPTION_DRY_RUN dotnet pack --configuration Release Avro.sln

      # add the binary LICENSE and NOTICE to the tarball
      $OPTION_DRY_RUN mkdir -p build/
      $OPTION_DRY_RUN cp LICENSE NOTICE build/

      # add binaries to the tarball
      $OPTION_DRY_RUN mkdir -p build/main/
      $OPTION_DRY_RUN cp -R src/apache/main/bin/Release/* build/main/
      # add codec binaries to the tarball
      for codec in $CSHARP_CODEC_LIBS
      do
        $OPTION_DRY_RUN mkdir -p build/codec/$codec/
        $OPTION_DRY_RUN cp -R src/apache/codec/$codec/bin/Release/* build/codec/$codec/
      done
      # add codegen binaries to the tarball
      $OPTION_DRY_RUN mkdir -p build/codegen/
      $OPTION_DRY_RUN cp -R src/apache/codegen/bin/Release/* build/codegen/

      # build the tarball
      $OPTION_DRY_RUN mkdir -p ${ROOT}/dist/csharp
      $OPTION_DRY_RUN pushd cd build && tar czf ${ROOT}/../dist/csharp/avro-csharp-${VERSION}.tar.gz main codegen LICENSE NOTICE && popd

      # build documentation
      $OPTION_DRY_RUN doxygen Avro.dox
      $OPTION_DRY_RUN mkdir -p ${ROOT}/build/avro-doc-${VERSION}/api/csharp
      $OPTION_DRY_RUN cp -pr build/doc/* ${ROOT}/build/avro-doc-${VERSION}/api/csharp

      # Release (pushing packages to nuget.org)
      if [ "$1" == "release" ]
      then
        # Push packages to nuget.org
        # Note: use loop instead of -exec or xargs to stop at first failure
        for package in $(find ./build/ -name '*.nupkg' -type f)
        do
          ask "Push $package to nuget.org" && $OPTION_DRY_RUN dotnet nuget push "$package" -k "$OPTION_NUGET_KEY" -s "$OPTION_NUGET_SOURCE"
        done
      fi
      ;;

    verify-release)
      for sdk_ver in $SUPPORTED_SDKS
      do
        ask "Verify .NET SDK $sdk_ver" && \
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
      ;;

    interop-data-generate)
      $OPTION_DRY_RUN dotnet run --project src/apache/test/Avro.test.csproj --framework net6.0 ../../share/test/schemas/interop.avsc ../../build/interop/data
      ;;

    interop-data-test)
      $OPTION_DRY_RUN LANG=en_US.UTF-8 dotnet test --filter "TestCategory=Interop" --logger "console;verbosity=normal;noprogress=true" src/apache/test/Avro.test.csproj
      ;;

    clean)
      $OPTION_DRY_RUN rm -rf src/apache/{main,test,codegen,ipc,msbuild,perf}/{obj,bin}
      $OPTION_DRY_RUN rm -rf build
      $OPTION_DRY_RUN rm -f  TestResult.xml
      ;;

    *)
      echo "Unknown option or command: $1"
      usage
      exit 1
      ;;
  esac

  shift
done
