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

# Tests avro-tools commands.

# Echo all commands, so that test failure location is clear.
set -o xtrace
# This script will exist at any false return value.
set -o errexit

if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

if [ "$TOOLS" = "" ]; then
  echo "Error: TOOLS is not set."
  exit 1
fi

if [ "$TMPDIR" = "" ]; then
  echo "Error: TMPDIR is not set."
  exit 1
fi

CMD="$JAVA_HOME/bin/java -jar $TOOLS"

######################################################################
# Clean up temp directory.
rm -rf $TMPDIR

######################################################################
$CMD compile protocol ../../share/test/schemas/namespace.avpr $TMPDIR/namespace
# Check that the expected names were generated
[ "MD5.java TestError.java TestNamespace.java TestRecord.java " = \
  "$(find $TMPDIR/namespace -name "*.java" \
    | awk -F "/" '{ print $NF }' | sort | tr '\n' ' ')" ]
$CMD compile schema ../../share/test/schemas/interop.avsc $TMPDIR/schema
[ "Foo.java Interop.java Kind.java MD5.java Node.java " = \
  "$(find $TMPDIR/schema -name "*.java" \
    | awk -F "/" '{ print $NF }' | sort | tr '\n' ' ')" ]
######################################################################
# Testing induce schema
$CMD induce build/test/classes org.apache.avro.BarRecord \
 | tr -d '\n ' | grep -q -F  '{"type":"record","name":"BarRecord"'
######################################################################
# Test induce protocol
$CMD induce build/test/classes 'org.apache.avro.TestReflect$C' \
 | tr -d '\n ' | grep -q -F  '{"protocol":"C"'
######################################################################
# Test to/from avro (both fragments and data files)
$CMD jsontofrag '"string"' <(echo '"Long string implies readable length encoding."') \
 | cmp -s - <(echo -n 'ZLong string implies readable length encoding.')
$CMD fragtojson '"string"' <(printf \\006foo) \
 | cmp -s - <(echo '"foo"')
# And test that stdin support (via "-") works too
echo '"The identity function"' \
  | $CMD jsontofrag '"string"' - \
  | $CMD fragtojson '"string"' - \
  | cmp -s - <(echo '"The identity function"')

$CMD fromjson '"string"' <(echo '"foo"'; echo '"bar"') \
  > $TMPDIR/data_file_write.avro
$CMD tojson $TMPDIR/data_file_write.avro \
  | cmp -s - <(echo '"foo"'; echo '"bar"')
$CMD getschema $TMPDIR/data_file_write.avro \
  | cmp -s - <(echo '"string"')
######################################################################

$CMD 2>&1 | grep -q "Available tools:"
$CMD doesnotexist 2>&1 | grep -q "Available tools:"
! $CMD 2>&1 > /dev/null
! $CMD 2>&1 doesnotexist 2>&1 > /dev/null
