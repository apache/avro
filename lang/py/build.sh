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

source ../../share/build-helper.sh "Python"

function command_clean()
{
  execute git clean -xdf '*.avpr' \
                 '*.avsc' \
                 '*.egg-info' \
                 '*.py[co]' \
                 'VERSION.txt' \
                 '__pycache__' \
                 '.tox' \
                 'avro/test/interop' \
                 'dist' \
                 'userlogs'
}

function command_dist()
{
  ##
  # Use https://pypa-build.readthedocs.io to create the build artifacts.
  local destination virtualenv
  destination=$(
    d=../../dist/py
    mkdir -p "$d"
    cd -P "$d"
    pwd
  )
  virtualenv="$(mktemp -d)"
  execute python3 -m venv "$virtualenv"
  execute "$virtualenv/bin/python3" -m pip install build
  execute "$virtualenv/bin/python3" -m build --outdir "$destination"
}

function command_interop-data-generate()
{
  execute ./setup.py generate_interop_data
  execute cp -r avro/test/interop/data $BUILD_ROOT/build/interop
}

function command_interop-data-test()
{
  execute mkdir -p avro/test/interop $BUILD_ROOT/build/interop/data
  execute cp -r $BUILD_ROOT/build/interop/data avro/test/interop
  execute python3 -m unittest avro.test.test_datafile_interop
}

function command_lint()
{
  execute python3 -m tox -e lint
}

function command_test()
{
  execute TOX_SKIP_ENV=lint python3 -m tox --skip-missing-interpreters
}

build-run "$@"
