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


# ------------------------------------------------------------------------------


SCHEMAS_TO_VALIDATE = (
  ('"null"', None),
  ('"boolean"', True),
  ('"string"', 'adsfasdf09809dsf-=adsf'),
  ('"bytes"', b'12345abcd'),
  ('"int"', 1234),
  ('"long"', 1234),
  ('"float"', 1234.0),
  ('"double"', 1234.0),
  ('{"type": "fixed", "name": "Test", "size": 1}', b'B'),
  ('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', 'B'),
  ('{"type": "array", "items": "long"}', [1, 3, 2]),
  ('{"type": "map", "values": "long"}', {'a': 1, 'b': 3, 'c': 2}),
  ('["string", "null", "long"]', None),

  ("""
   {
     "type": "record",
     "name": "Test",
     "fields": [{"name": "f", "type": "long"}]
   }
   """,
   {'f': 5}),

  ("""
   {
     "type": "record",
     "name": "Lisp",
     "fields": [{
        "name": "value",
        "type": [
          "null",
          "string",
          {
            "type": "record",
            "name": "Cons",
            "fields": [{"name": "car", "type": "Lisp"},
                       {"name": "cdr", "type": "Lisp"}]
          }
        ]
     }]
   }
   """,
   {'value': {'car': {'value': 'head'}, 'cdr': {'value': None}}}),
)

CODECS_TO_VALIDATE = ('null', 'deflate')

try:
  import snappy
  CODECS_TO_VALIDATE += ('snappy',)
except ImportError:
  logging.warning('Snappy not present, will skip testing it.')

try:
  import zstandard
  CODECS_TO_VALIDATE += ('zstandard',)
except ImportError:
  logging.warning('Zstandard not present, will skip testing it.')

# ------------------------------------------------------------------------------


class TestDataFile(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    cls._temp_dir = (
        tempfile.TemporaryDirectory(prefix=cls.__name__, suffix='.tmp'))
    logging.debug('Created temporary directory: %s', cls._temp_dir.name)

  @classmethod
  def tearDownClass(cls):
    logging.debug('Cleaning up temporary directory: %s', cls._temp_dir.name)
    cls._temp_dir.cleanup()

  def NewTempFile(self):
    """Creates a new temporary file.

    File is automatically cleaned up after test.

    Returns:
      The path of the new temporary file.
    """
    temp_file = tempfile.NamedTemporaryFile(
        dir=self._temp_dir.name,
        prefix='test',
        suffix='.avro',
        delete=False,
    )
    return temp_file.name

  def testRoundTrip(self):
    correct = 0
    for iexample, (writer_schema, datum) in enumerate(SCHEMAS_TO_VALIDATE):
      for codec in CODECS_TO_VALIDATE:
        file_path = self.NewTempFile()

        # Write the datum this many times in the data file:
        nitems = 10

        logging.debug(
            'Performing round-trip with codec %r in file %s for example #%d\n'
            'Writing datum: %r using writer schema:\n%s',
            codec, file_path, iexample,
            datum, writer_schema)

        logging.debug('Creating data file %r', file_path)
        with open(file_path, 'wb') as writer:
          datum_writer = io.DatumWriter()
          schema_object = schema.Parse(writer_schema)
          with datafile.DataFileWriter(
              writer=writer,
              datum_writer=datum_writer,
              writer_schema=schema_object,
              codec=codec,
          ) as dfw:
            for _ in range(nitems):
              dfw.append(datum)

        logging.debug('Reading data from %r', file_path)
        with open(file_path, 'rb') as reader:
          datum_reader = io.DatumReader()
          with datafile.DataFileReader(reader, datum_reader) as dfr:
            round_trip_data = list(dfr)

        logging.debug(
            'Round-trip data has %d items: %r',
            len(round_trip_data), round_trip_data)

        if ([datum] * nitems) == round_trip_data:
          correct += 1
        else:
          logging.error(
              'Round-trip data does not match:\n'
              'Expect: %r\n'
              'Actual: %r',
              [datum] * nitems,
              round_trip_data)

    self.assertEqual(
        correct,
        len(CODECS_TO_VALIDATE) * len(SCHEMAS_TO_VALIDATE))

  def testAppend(self):
    correct = 0
    for iexample, (writer_schema, datum) in enumerate(SCHEMAS_TO_VALIDATE):
      for codec in CODECS_TO_VALIDATE:
        file_path = self.NewTempFile()

        logging.debug(
            'Performing append with codec %r in file %s for example #%d\n'
            'Writing datum: %r using writer schema:\n%s',
            codec, file_path, iexample,
            datum, writer_schema)

        logging.debug('Creating data file %r', file_path)
        with open(file_path, 'wb') as writer:
          datum_writer = io.DatumWriter()
          schema_object = schema.Parse(writer_schema)
          with datafile.DataFileWriter(
              writer=writer,
              datum_writer=datum_writer,
              writer_schema=schema_object,
              codec=codec,
          ) as dfw:
            dfw.append(datum)

        logging.debug('Appending data to %r', file_path)
        for i in range(9):
          with open(file_path, 'ab+') as writer:
            with datafile.DataFileWriter(writer, io.DatumWriter()) as dfw:
              dfw.append(datum)

        logging.debug('Reading appended data from %r', file_path)
        with open(file_path, 'rb') as reader:
          datum_reader = io.DatumReader()
          with datafile.DataFileReader(reader, datum_reader) as dfr:
            appended_data = list(dfr)

        logging.debug(
            'Appended data has %d items: %r',
            len(appended_data), appended_data)

        if ([datum] * 10) == appended_data:
          correct += 1
        else:
          logging.error(
              'Appended data does not match:\n'
              'Expect: %r\n'
              'Actual: %r',
              [datum] * 10,
              appended_data)

    self.assertEqual(
        correct,
        len(CODECS_TO_VALIDATE) * len(SCHEMAS_TO_VALIDATE))

  def testContextManager(self):
    file_path = self.NewTempFile()

    # Test the writer with a 'with' statement.
    with open(file_path, 'wb') as writer:
      datum_writer = io.DatumWriter()
      sample_schema, sample_datum = SCHEMAS_TO_VALIDATE[1]
      schema_object = schema.Parse(sample_schema)
      with datafile.DataFileWriter(writer, datum_writer, schema_object) as dfw:
        dfw.append(sample_datum)
      self.assertTrue(writer.closed)

    # Test the reader with a 'with' statement.
    datums = []
    with open(file_path, 'rb') as reader:
      datum_reader = io.DatumReader()
      with datafile.DataFileReader(reader, datum_reader) as dfr:
        for datum in dfr:
          datums.append(datum)
      self.assertTrue(reader.closed)

  def testMetadata(self):
    file_path = self.NewTempFile()

    # Test the writer with a 'with' statement.
    with open(file_path, 'wb') as writer:
      datum_writer = io.DatumWriter()
      sample_schema, sample_datum = SCHEMAS_TO_VALIDATE[1]
      schema_object = schema.Parse(sample_schema)
      with datafile.DataFileWriter(writer, datum_writer, schema_object) as dfw:
        dfw.SetMeta('test.string', 'foo')
        dfw.SetMeta('test.number', '1')
        dfw.append(sample_datum)
      self.assertTrue(writer.closed)

    # Test the reader with a 'with' statement.
    datums = []
    with open(file_path, 'rb') as reader:
      datum_reader = io.DatumReader()
      with datafile.DataFileReader(reader, datum_reader) as dfr:
        self.assertEqual(b'foo', dfr.GetMeta('test.string'))
        self.assertEqual(b'1', dfr.GetMeta('test.number'))
        for datum in dfr:
          datums.append(datum)
      self.assertTrue(reader.closed)


# ------------------------------------------------------------------------------


if __name__ == '__main__':
  raise Exception('Use run_tests.py')
