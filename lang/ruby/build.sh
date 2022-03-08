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

BUILD_DESCRIPTION="Build script for Apache Avro Ruby"
source ../../share/build-helper.sh

# connect to avro ruby root directory
cd "$(dirname "$0")"

# maintain our gems here
export GEM_HOME="$PWD/.gem/"
export PATH="/usr/local/rbenv/shims:$GEM_HOME/bin:$PATH"

# bootstrap bundler
gem install --no-document -v 1.17.3 bundler

# rbenv is used by the Dockerfile but not the Github action in CI
rbenv rehash 2>/dev/null || echo "Not using rbenv"
bundle install

function command_lint()
{
  execute bundle exec rubocop
}

function command_interop-data-generate()
{
  execute bundle exec rake generate_interop
}

function command_interop-data-test()
{
  execute bundle exec rake interop
}

function command_test()
{
  execute bundle exec rake test
}

function command_dist()
{
  execute bundle exec rake build
  DIST="$BUILD_ROOT/dist/ruby"
  mkdir -p "${DIST}"
  cp "pkg/avro-${BUILD_VERSION}.gem" "${DIST}"
}

function command_clean()
{
  execute bundle exec rake clean
  execute rm -rf tmp data.avr lib/avro/VERSION.txt
}

build-run "$@"