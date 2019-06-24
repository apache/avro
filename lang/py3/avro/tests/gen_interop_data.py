#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

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
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

from avro import datafile
from avro import io
from avro import schema


DATUM = {
    'intField': 12,
    'longField': 15234324,
    'stringField': 'hey',
    'boolField': True,
    'floatField': 1234.0,
    'doubleField': -1234.0,
    'bytesField': b'12312adf',
    'nullField': None,
    'arrayField': [5.0, 0.0, 12.0],
    'mapField': {'a': {'label': 'a'}, 'bee': {'label': 'cee'}},
    'unionField': 12.0,
    'enumField': 'C',
    'fixedField': b'1019181716151413',
    'recordField': {
        'label': 'blah',
        'children': [{'label': 'inner', 'children': []}],
    },
}


if __name__ == "__main__":
  interop_schema = schema.Parse(open(sys.argv[1], 'r').read())
  writer = open(sys.argv[2], 'wb')
  datum_writer = io.DatumWriter()
  # NB: not using compression
  dfw = datafile.DataFileWriter(writer, datum_writer, interop_schema)
  dfw.append(DATUM)
  dfw.close()
