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

set -e # exit on error

cd "$(dirname "$0")" # If being called from another folder, cd into the directory containing this script.

# shellcheck disable=SC1091
source ../../share/build-helper.sh "Perl"

function command_clean()
{
  [ ! -f Makefile ] || execute make clean
  execute rm -f  Avro-*.tar.gz META.yml Makefile.old
  execute rm -rf lang/perl/inc/
}

function command_lint()
{
  local failures=0
  # shellcheck disable=SC2044
  for i in $(find lib t xt -name '*.p[lm]' -or -name '*.t'); do
    if ! execute perlcritic --verbose 1 "${i}"; then
      ((failures=failures+1))
    fi
  done
  if [ ${failures} -gt 0 ]; then
    return 1
  fi
}

function command_test()
{
  execute perl ./Makefile.PL
  execute make test
}

function command_dist()
{
  execute cp "$BUILD_ROOT/share/VERSION.txt" .
  execute perl ./Makefile.PL
  execute make dist
}

function command_interop-data-generate()
{
  execute perl -Ilib share/interop-data-generate
}

function command_interop-data-test()
{
  execute prove -Ilib xt/interop.t
}

build-run "$@"