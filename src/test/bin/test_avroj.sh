#!/usr/bin/env bash

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

# Tests "avroj" script and commands.

# Echo all commands, so that test failure location is clear.
set -o xtrace
# This script will exist at any false return value.
set -o errexit

AVROJ="java -jar build/avroj-*.jar"

# Create a temp directory.
TMPDIR=$(mktemp -d -t test_avroj)

######################################################################
echo "Testing code generation..."

$AVROJ compile protocol src/test/schemata/namespace.avpr $TMPDIR/namespace
# Check that the expected names were generated
[ "MD5.java TestError.java TestNamespace.java TestRecord.java " = \
  "$(find $TMPDIR/namespace -name "*.java" \
    | awk -F "/" '{ print $NF }' | sort | tr '\n' ' ')" ]
$AVROJ compile schema src/test/schemata/interop.avsc $TMPDIR/schema
[ "Foo.java Interop.java Kind.java MD5.java Node.java " = \
  "$(find $TMPDIR/schema -name "*.java" \
    | awk -F "/" '{ print $NF }' | sort | tr '\n' ' ')" ]

######################################################################
echo "Testing avroj command..."

$AVROJ 2>&1 | grep -q "Expected one of the following"
$AVROJ doesnotexist 2>&1 | grep -q "Expected one of the following"

######################################################################
# Clean up temp directory.
rm -rf $TMPDIR

echo "Tests passed!"
