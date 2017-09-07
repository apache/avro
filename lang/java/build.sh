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

set -e # exit on error

function usage {
  echo "Usage: $0 {test|dist|clean}"
  exit 1
}

if [ $# -eq 0 ]
then
  usage
fi

if [ -f VERSION.txt ]
then
  VERSION=`cat VERSION.txt`
else
  VERSION=`cat ../../share/VERSION.txt`
fi

for target in "$@"
do

function do_dist() {
  mvn -P dist package -DskipTests -Davro.version=$VERSION javadoc:aggregate
}

case "$target" in
  test)
    mvn test
    ;;

  dist)
    do_dist
    ;;

  clean)
    mvn clean
    ;;

  *)
    usage
esac

done

exit 0
