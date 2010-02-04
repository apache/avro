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

set -e						  # exit on error

cd `dirname "$0"`/../../..			  # connect to lang/java 

VERSION=`cat ../../share/VERSION.txt`

set -x						  # echo commands

function write_request() {
    messageName="$1"
    callName="$2"
    params="$3"
    data="$4"
    
    outdir=../../share/test/interop/rpc/$messageName/$callName
    mkdir -p $outdir

    outfile=$outdir/request.avro

    schema='{"type":"record","name":"'$messageName'","fields":'$params'}'

    echo -n "$data" | \
	java -jar build/avro-tools-$VERSION.jar fromjson "$schema" - > $outfile
}

function write_response() {
    messageName="$1"
    callName="$2"
    schema="$3"
    data="$4"
    
    outdir=../../share/test/interop/rpc/$messageName/$callName
    mkdir -p $outdir

    outfile=$outdir/response.avro

    echo -n "$data" | \
	java -jar build/avro-tools-$VERSION.jar fromjson "$schema" - > $outfile
}

write_request hello world \
    '[{"name": "greeting", "type": "string"}]' \
    '{"greeting": "Hello World!"}'

write_response hello world '"string"' '"Hello World"'

write_request echo foo \
    '[{"name": "record", "type": {"name": "org.apache.avro.test.TestRecord", "type": "record", "fields": [ {"name": "name", "type": "string", "order": "ignore"}, {"name": "kind", "type": {"name": "Kind", "type": "enum", "symbols": ["FOO","BAR","BAZ"]}, "order": "descending"}, {"name": "hash", "type": {"name": "MD5", "type": "fixed", "size": 16}} ] }}]' \
    '{"record": {"name": "Foo", "kind": "FOO", "hash": "0123456789012345"}}'

write_response echo foo \
    '{"name": "org.apache.avro.test.TestRecord", "type": "record", "fields": [ {"name": "name", "type": "string", "order": "ignore"}, {"name": "kind", "type": {"name": "Kind", "type": "enum", "symbols": ["FOO","BAR","BAZ"]}, "order": "descending"}, {"name": "hash", "type": {"name": "MD5", "type": "fixed", "size": 16}} ]}' \
    '{"name": "Foo", "kind": "FOO", "hash": "0123456789012345"}'

write_request add onePlusOne \
    '[{"name": "arg1", "type": "int"}, {"name": "arg2", "type": "int"}]' \
    '{"arg1": 1, "arg2": 1}'

write_response add onePlusOne '"int"' 2
