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

cd "$(dirname "$0")" # If being called from another folder, cd into the directory containing this script.

source ../../share/build-helper.sh "PHP"

dist_dir="$BUILD_ROOT/dist/php"
build_dir="pkg"
libname="avro-php-$BUILD_VERSION"
lib_dir="$build_dir/$libname"
tarball="$libname.tar.bz2"

test_tmp_dir="test/tmp"

function command_clean()
{
  execute rm -rf "$test_tmp_dir"
  execute rm -rf "$build_dir"
}

function command_dist()
{
    mkdir -p "$build_dir/$libname" "$lib_dir/examples"
    cp -pr lib "$lib_dir"
    cp -pr examples/*.php "$lib_dir/examples"
    cp README.md LICENSE NOTICE "$lib_dir"
    pushd "$build_dir"
    tar -cjf "$tarball" "$libname"
    mkdir -p "../$dist_dir"
    cp "$tarball" "../$dist_dir"
    popd
}


function command_interop-data-generate()
{
  execute composer install -d "$BUILD_ROOT"
  execute php test/generate_interop_data.php
}

function command_interop-data-test()
{
  execute composer install -d "$BUILD_ROOT"
  execute vendor/bin/phpunit test/InterOpTest.php
}

function command_lint()
{
  execute composer install -d "$BUILD_ROOT"
  find . -name "*.php" -print0 | xargs -0 -n1 -P8 php -l
  execute vendor/bin/phpcs --standard=PSR12 lib
}

function command_test()
{
  execute composer install -d "$BUILD_ROOT"
  execute vendor/bin/phpunit -v
}

build-run "$@"
