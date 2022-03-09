#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e        # exit on error

cd "$(dirname "$0")" # If being called from another folder, cd into the directory containing this script.

# shellcheck disable=SC1091
source ../../share/build-helper.sh "C"

root_dir=$(pwd)
build_dir="$BUILD_ROOT/build/c"
dist_dir="$BUILD_ROOT/dist/c"
version=$(./version.sh project)
tarball="avro-c-$version.tar.gz"
doc_dir="$BUILD_ROOT/build/avro-doc-$version/api/c"

function prepare_build()
{
  command_clean
  execute mkdir -p "$build_dir"
  execute pushd "$build_dir"
  execute cmake "$root_dir" -DCMAKE_BUILD_TYPE=RelWithDebInfo
  execute popd
}

function command_clean()
{
  if [ -d "$build_dir" ]; then
    execute find "$build_dir" -exec chmod 755 {} +
    execute rm -rf "$build_dir"
  fi
  execute rm -f VERSION.txt
  execute rm -f examples/quickstop.db
}

function command_interop-data-generate()
{
  prepare_build
  execute make -C "$build_dir"
  execute "$build_dir/tests/generate_interop_data" "$BUILD_ROOT/share/test/schemas/interop.avsc"  "$BUILD_ROOT/build/interop/data"
}

function command_interop-data-test()
{
    prepare_build
    execute make -C "$build_dir"
    execute "$build_dir/tests/test_interop_data" "$BUILD_ROOT/build/interop/data"
}

function command_lint()
{
  echo 'This is a stub where someone can provide linting.'
}

function command_test()
{
  prepare_build
  execute make -C "$build_dir"
  execute make -C "$build_dir" test
}

function command_dist()
{
  prepare_build
  execute cp "$BUILD_ROOT/share/VERSION.txt" "$root_dir"
  execute make -C "$build_dir" docs
  # This is a hack to force the built documentation to be included
  # in the source package.
  execute cp "$build_dir"/docs/*.html "$root_dir/docs"
  execute make -C "$build_dir" package_source
  execute rm "$root_dir"/docs/*.html
  if [ ! -d "$dist_dir" ]; then
    execute mkdir -p "$dist_dir"
  fi
  if [ ! -d "$doc_dir" ]; then
    execute mkdir -p "$doc_dir"
  fi
  execute mv "$build_dir/$tarball" "$dist_dir"
  execute cp "$build_dir"/docs/*.html "$doc_dir"
  command_clean
}

build-run "$@"