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
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import, division, print_function

import os
import unittest

from avro import datafile, io, schema
from avro.codecs import Codecs

try:
    unicode
except NameError:
    unicode = str


SCHEMAS_TO_VALIDATE = (
    ('"null"', None),
    ('"boolean"', True),
    ('"string"', unicode('adsfasdf09809dsf-=adsf')),
    ('"bytes"', b'12345abcd'),
    ('"int"', 1234),
    ('"long"', 1234),
    ('"float"', 1234.0),
    ('"double"', 1234.0),
    ('{"type": "fixed", "name": "Test", "size": 1}', b'B'),
    ('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', 'B'),
    ('{"type": "array", "items": "long"}', [1, 3, 2]),
    ('{"type": "map", "values": "long"}', {unicode('a'): 1,
                                           unicode('b'): 3,
                                           unicode('c'): 2}),
    ('["string", "null", "long"]', None),
    ("""\
   {"type": "record",
    "name": "Test",
    "fields": [{"name": "f", "type": "long"}]}
   """, {'f': 5}),
    ("""\
   {"type": "record",
    "name": "Lisp",
    "fields": [{"name": "value",
                "type": ["null", "string",
                         {"type": "record",
                          "name": "Cons",
                          "fields": [{"name": "car", "type": "Lisp"},
                                     {"name": "cdr", "type": "Lisp"}]}]}]}
   """, {'value': {'car': {'value': unicode('head')}, 'cdr': {'value': None}}}),
)

FILENAME = 'test_datafile.out'
CODECS_TO_VALIDATE = Codecs.supported_codec_names()


class TestDataFile(unittest.TestCase):
    def test_round_trip(self):
        print('')
        print('TEST ROUND TRIP')
        print('===============')
        print('')
        correct = 0
        for i, (example_schema, datum) in enumerate(SCHEMAS_TO_VALIDATE):
            for codec in CODECS_TO_VALIDATE:
                print('')
                print('SCHEMA NUMBER %d' % (i + 1))
                print('================')
                print('')
                print('Schema: %s' % example_schema)
                print('Datum: %s' % datum)
                print('Codec: %s' % codec)

                # write data in binary to file 10 times
                writer = open(FILENAME, 'wb')
                datum_writer = io.DatumWriter()
                schema_object = schema.parse(example_schema)
                dfw = datafile.DataFileWriter(writer, datum_writer, schema_object, codec=codec)
                for i in range(10):
                    dfw.append(datum)
                dfw.close()

                # read data in binary from file
                reader = open(FILENAME, 'rb')
                datum_reader = io.DatumReader()
                dfr = datafile.DataFileReader(reader, datum_reader)
                round_trip_data = []
                for datum in dfr:
                    round_trip_data.append(datum)

                print('Round Trip Data: %s' % round_trip_data)
                print('Round Trip Data Length: %d' % len(round_trip_data))
                is_correct = [datum] * 10 == round_trip_data
                if is_correct:
                    correct += 1
                print('Correct Round Trip: %s' % is_correct)
                print('')
        os.remove(FILENAME)
        self.assertEquals(correct, len(CODECS_TO_VALIDATE) * len(SCHEMAS_TO_VALIDATE))

    def test_append(self):
        print('')
        print('TEST APPEND')
        print('===========')
        print('')
        correct = 0
        for i, (example_schema, datum) in enumerate(SCHEMAS_TO_VALIDATE):
            for codec in CODECS_TO_VALIDATE:
                print('')
                print('SCHEMA NUMBER %d' % (i + 1))
                print('================')
                print('')
                print('Schema: %s' % example_schema)
                print('Datum: %s' % datum)
                print('Codec: %s' % codec)

                # write data in binary to file once
                writer = open(FILENAME, 'wb')
                datum_writer = io.DatumWriter()
                schema_object = schema.parse(example_schema)
                dfw = datafile.DataFileWriter(writer, datum_writer, schema_object, codec=codec)
                dfw.append(datum)
                dfw.close()

                # open file, write, and close nine times
                for i in range(9):
                    writer = open(FILENAME, 'ab+')
                    dfw = datafile.DataFileWriter(writer, io.DatumWriter())
                    dfw.append(datum)
                    dfw.close()

                # read data in binary from file
                reader = open(FILENAME, 'rb')
                datum_reader = io.DatumReader()
                dfr = datafile.DataFileReader(reader, datum_reader)
                appended_data = []
                for datum in dfr:
                    appended_data.append(datum)

                print('Appended Data: %s' % appended_data)
                print('Appended Data Length: %d' % len(appended_data))
                is_correct = [datum] * 10 == appended_data
                if is_correct:
                    correct += 1
                print('Correct Appended: %s' % is_correct)
                print('')
        os.remove(FILENAME)
        self.assertEquals(correct, len(CODECS_TO_VALIDATE) * len(SCHEMAS_TO_VALIDATE))

    def test_context_manager(self):
        """Test the writer with a 'with' statement."""
        writer = open(FILENAME, 'wb')
        datum_writer = io.DatumWriter()
        sample_schema, sample_datum = SCHEMAS_TO_VALIDATE[1]
        schema_object = schema.parse(sample_schema)
        with datafile.DataFileWriter(writer, datum_writer, schema_object) as dfw:
            dfw.append(sample_datum)
        self.assertTrue(writer.closed)

        # Test the reader with a 'with' statement.
        datums = []
        reader = open(FILENAME, 'rb')
        datum_reader = io.DatumReader()
        with datafile.DataFileReader(reader, datum_reader) as dfr:
            for datum in dfr:
                datums.append(datum)
        self.assertTrue(reader.closed)

    def test_metadata(self):
        # Test the writer with a 'with' statement.
        writer = open(FILENAME, 'wb')
        datum_writer = io.DatumWriter()
        sample_schema, sample_datum = SCHEMAS_TO_VALIDATE[1]
        schema_object = schema.parse(sample_schema)
        with datafile.DataFileWriter(writer, datum_writer, schema_object) as dfw:
            dfw.set_meta('test.string', b'foo')
            dfw.set_meta('test.number', b'1')
            dfw.append(sample_datum)
        self.assertTrue(writer.closed)

        # Test the reader with a 'with' statement.
        datums = []
        reader = open(FILENAME, 'rb')
        datum_reader = io.DatumReader()
        with datafile.DataFileReader(reader, datum_reader) as dfr:
            self.assertEquals(b'foo', dfr.get_meta('test.string'))
            self.assertEquals(b'1', dfr.get_meta('test.number'))
            for datum in dfr:
                datums.append(datum)
        self.assertTrue(reader.closed)

    def test_empty_datafile(self):
        """A reader should not fail to read a file consisting of a single empty block."""
        sample_schema = schema.parse(SCHEMAS_TO_VALIDATE[1][0])
        with datafile.DataFileWriter(open(FILENAME, 'wb'), io.DatumWriter(),
                                     sample_schema) as dfw:
            dfw.flush()
            # Write an empty block
            dfw.encoder.write_long(0)
            dfw.encoder.write_long(0)
            dfw.writer.write(dfw.sync_marker)

        with datafile.DataFileReader(open(FILENAME, 'rb'), io.DatumReader()) as dfr:
            self.assertEqual([], list(dfr))


if __name__ == '__main__':
    unittest.main()
