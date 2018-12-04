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

set -e                          # exit on error

cd `dirname "$0"`/../../../..   # connect to root

VERSION=`cat share/VERSION.txt`

set -x                          # echo commands

java_client="java -jar lang/java/tools/target/avro-tools-$VERSION.jar rpcsend"
java_server="java -jar lang/java/tools/target/avro-tools-$VERSION.jar rpcreceive"

py_client="python lang/py/build/src/avro/tool.py rpcsend"
py_server="python lang/py/build/src/avro/tool.py rpcreceive"

ruby_client="ruby -rubygems -Ilang/ruby/lib lang/ruby/test/tool.rb rpcsend"
ruby_server="ruby -rubygems -Ilang/ruby/lib lang/ruby/test/tool.rb rpcreceive"

export PYTHONPATH=lang/py/build/src      # path to avro Python module

clients=("$java_client" "$py_client" "$ruby_client")
servers=("$java_server" "$py_server" "$ruby_server")

proto=share/test/schemas/simple.avpr

portfile=/tmp/interop_$$

function cleanup() {
  rm -rf "$portfile"
  for job in `jobs -p` ; do
    kill $(jobs -p) 2>/dev/null || true;
  done
}

trap 'cleanup' EXIT

for server in "${servers[@]}"
do
  for msgDir in share/test/interop/rpc/*
  do
    msg=`basename "$msgDir"`
    for c in ${msgDir}/*
    do
      echo TEST: $c
      for client in "${clients[@]}"
      do
        rm -rf "$portfile"
        $server http://127.0.0.1:0/ $proto $msg -file $c/response.avro > $portfile &
        count=0
        while [ ! -s $portfile ]
        do
          sleep 1
          if [ $count -ge 10 ]
          then
            echo $server did not start.
            exit 1
          fi
          count=`expr $count + 1`
        done
        read ignore port < $portfile
        $client http://127.0.0.1:$port $proto $msg -file $c/request.avro
        wait
        done
    done
    done
done
