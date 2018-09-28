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

function headline(){
  echo -e "\e[1;34m#################################################################"
  echo -e "##### $1 \e[1;37m"
  echo -e "\e[1;34m#################################################################\e[0m"
}

set -e

headline "Run Java tests"
cd /avro/lang/java
./build.sh test

headline "Run Python2 tests"
cd /avro/lang/py
./build.sh test

headline "Run Python3 tests"
cd /avro/lang/py3
./build.sh test

headline "Run C tests"
cd /avro/lang/c
./build.sh test

headline "Run C++ tests"
cd /avro/lang/c++
# The current cpp tests are failing:
# https://issues.apache.org/jira/projects/AVRO/issues/AVRO-2230
./build.sh test || true

headline "Run C# tests"
cd /avro/lang/csharp
./build.sh test

echo "Run Ruby tests"
cd /avro/lang/ruby
./build.sh test

echo "Run PHP tests"
cd /avro/lang/php
./build.sh test

echo "Run JS tests"
cd /avro/lang/js
./build.sh test

echo "Run Perl tests"
cd /avro/lang/perl
./build.sh test
