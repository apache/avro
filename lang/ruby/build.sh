#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# connect to Avro Ruby root directory
cd `dirname "$0"`

# maintain our gems here
export GEM_HOME=.gem/
export PATH="$PATH:.gem/bin"

# boostrap bundler
gem install --conservative --no-rdoc --no-ri bundler

case "$1" in
     test)
        bundle install
        bundle exec rake test
       ;;

     dist)
        bundle exec rake dist
       ;;

     clean)
        bundle exec rake clean
       ;;

     *)
       echo "Usage: $0 {test|dist|clean}"
       exit 1

esac

exit 0
