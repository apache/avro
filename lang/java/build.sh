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

source ../../share/build-helper.sh "Java"

function command_lint()
{
  execute mvn -B spotless:apply
}


function command_test()
{
  execute mvn -B test
  # Test the modules that depend on hadoop using Hadoop 2
  execute mvn -B test -Phadoop2
}

function command_dist()
{
  execute mvn -P dist package -DskipTests javadoc:aggregate
}

function command_clean()
{
  execute mvn clean
}

build-run "$@"
