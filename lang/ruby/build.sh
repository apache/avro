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

# connect to avro ruby root directory
cd `dirname "$0"`

# maintain our gems here
export GEM_HOME=.gem/
export PATH="$PATH:.gem/bin"

# bootstrap bundler
gem install --no-document -v 1.17.3 bundler
bundle install

for target in "$@"
do
  case "$target" in
    lint)
      rubocop --lint
      ;;

    test)
      bundle exec rake test
      ;;

    dist)
      bundle exec rake dist
      ;;

    clean)
      bundle exec rake clean
      rm -rf tmp avro.gemspec data.avr
      ;;

    *)
      echo "Usage: $0 {lint|test|dist|clean}"
      exit 1
  esac
done
