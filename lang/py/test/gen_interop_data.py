#!/usr/bin/env python

##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import, division, print_function

import os
import sys

from avro import datafile, io, schema

CODECS_TO_VALIDATE = ('null', 'deflate')

try:
  import snappy
  CODECS_TO_VALIDATE += ('snappy',)
except ImportError:
  print('Snappy not present, will skip generating it.')
try:
  import zstandard
  CODECS_TO_VALIDATE += ('zstandard',)
except ImportError:
  print('Zstandard not present, will skip generating it.')

DATUM = {
  'intField': 12,
  'longField': 15234324,
  'stringField': unicode('hey'),
  'boolField': True,
  'floatField': 1234.0,
  'doubleField': -1234.0,
  'bytesField': '12312adf',
  'nullField': None,
  'arrayField': [5.0, 0.0, 12.0],
  'mapField': {'a': {'label': 'a'}, 'bee': {'label': 'cee'}},
  'unionField': 12.0,
  'enumField': 'C',
  'fixedField': '1019181716151413',
  'recordField': {'label': 'blah', 'children': [{'label': 'inner', 'children': []}]},
}

if __name__ == "__main__":
  for codec in CODECS_TO_VALIDATE:
    interop_schema = schema.parse(open(sys.argv[1], 'r').read())
    filename = sys.argv[2]
    if codec != 'null':
      base, ext = os.path.splitext(filename)
      filename = base + "_" + codec + ext
    writer = open(filename, 'wb')
    datum_writer = io.DatumWriter()
    # NB: not using compression
    dfw = datafile.DataFileWriter(writer, datum_writer, interop_schema, codec=codec)
    dfw.append(DATUM)
    dfw.close()
