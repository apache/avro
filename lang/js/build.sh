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

source ../../share/build-helper.sh "Javascript"

function command_lint()
{
  execute npm install
  execute npm run lint
}

function command_test()
{
  execute npm install
  execute npm run cover
}

function command_dist()
{
  execute npm pack
  execute mkdir -p $BUILD_ROOT/dist/js
  execute mv avro-js-*.tgz $BUILD_ROOT/dist/js
}

function command_clean()
{
  execute rm -rf coverage
}

function command_interop-data-generate()
{
  execute npm run interop-data-generate
}

function command_interop-data-test()
{
  execute npm run interop-data-test
}

build-run "$@"