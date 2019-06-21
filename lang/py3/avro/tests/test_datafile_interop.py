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

import logging
import os
import tempfile
import unittest

from avro import datafile
from avro import io
from avro import schema


def GetInteropSchema():
  test_dir = os.path.dirname(os.path.abspath(__file__))
  schema_json_path = os.path.join(test_dir, 'interop.avsc')
  with open(schema_json_path, 'r') as f:
    schema_json = f.read()
  return schema.Parse(schema_json)


INTEROP_SCHEMA = GetInteropSchema()
INTEROP_DATUM = {
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


def WriteDataFile(path, datum, schema):
  datum_writer = io.DatumWriter()
  with open(path, 'wb') as writer:
    # NB: not using compression
    with datafile.DataFileWriter(writer, datum_writer, schema) as dfw:
      dfw.append(datum)


class TestDataFileInterop(unittest.TestCase):
  def testInterop(self):
    with tempfile.NamedTemporaryFile() as temp_path:
      WriteDataFile(temp_path.name, INTEROP_DATUM, INTEROP_SCHEMA)

      # read data in binary from file
      datum_reader = io.DatumReader()
      with open(temp_path.name, 'rb') as reader:
        dfr = datafile.DataFileReader(reader, datum_reader)
        for datum in dfr:
          self.assertEqual(INTEROP_DATUM, datum)


if __name__ == '__main__':
  raise Exception('Use run_tests.py')
