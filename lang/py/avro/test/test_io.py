#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

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

import binascii
import datetime
import decimal
import io
import unittest

import avro.io
import avro.schema
import avro.timezones

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
    ('{"type": "fixed", "logicalType": "decimal", "name": "Test", "size": 8, "precision": 5, "scale": 4}',
     decimal.Decimal('3.1415')),
    ('{"type": "fixed", "logicalType": "decimal", "name": "Test", "size": 8, "precision": 5, "scale": 4}',
     decimal.Decimal('-3.1415')),
    ('{"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 4}', decimal.Decimal('3.1415')),
    ('{"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 4}', decimal.Decimal('-3.1415')),
    ('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', 'B'),
    ('{"type": "array", "items": "long"}', [1, 3, 2]),
    ('{"type": "map", "values": "long"}', {'a': 1,
                                           'b': 3,
                                           'c': 2}),
    ('["string", "null", "long"]', None),
    ('{"type": "int", "logicalType": "date"}', datetime.date(2000, 1, 1)),
    ('{"type": "int", "logicalType": "time-millis"}', datetime.time(23, 59, 59, 999000)),
    ('{"type": "int", "logicalType": "time-millis"}', datetime.time(0, 0, 0, 000000)),
    ('{"type": "long", "logicalType": "time-micros"}', datetime.time(23, 59, 59, 999999)),
    ('{"type": "long", "logicalType": "time-micros"}', datetime.time(0, 0, 0, 000000)),
    (
        '{"type": "long", "logicalType": "timestamp-millis"}',
        datetime.datetime(1000, 1, 1, 0, 0, 0, 000000, tzinfo=avro.timezones.utc)
    ),
    (
        '{"type": "long", "logicalType": "timestamp-millis"}',
        datetime.datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=avro.timezones.utc)
    ),
    (
        '{"type": "long", "logicalType": "timestamp-millis"}',
        datetime.datetime(2000, 1, 18, 2, 2, 1, 100000, tzinfo=avro.timezones.tst)
    ),
    (
        '{"type": "long", "logicalType": "timestamp-micros"}',
        datetime.datetime(1000, 1, 1, 0, 0, 0, 000000, tzinfo=avro.timezones.utc)
    ),
    (
        '{"type": "long", "logicalType": "timestamp-micros"}',
        datetime.datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=avro.timezones.utc)
    ),
    (
        '{"type": "long", "logicalType": "timestamp-micros"}',
        datetime.datetime(2000, 1, 18, 2, 2, 1, 123499, tzinfo=avro.timezones.tst)
    ),
    ('{"type": "string", "logicalType": "uuid"}', u'12345abcd'),
    ('{"type": "string", "logicalType": "unknown-logical-type"}', u'12345abcd'),
    ('{"type": "string", "logicalType": "timestamp-millis"}', u'12345abcd'),
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
   """, {'value': {'car': {'value': 'head'}, 'cdr': {'value': None}}}),
)

BINARY_ENCODINGS = (
    (0, b'00'),
    (-1, b'01'),
    (1, b'02'),
    (-2, b'03'),
    (2, b'04'),
    (-64, b'7f'),
    (64, b'80 01'),
    (8192, b'80 80 01'),
    (-8193, b'81 80 01'),
)

DEFAULT_VALUE_EXAMPLES = (
    ('"null"', 'null', None),
    ('"boolean"', 'true', True),
    ('"string"', '"foo"', u'foo'),
    ('"bytes"', '"\u00FF\u00FF"', u'\xff\xff'),
    ('"int"', '5', 5),
    ('"long"', '5', 5),
    ('"float"', '1.1', 1.1),
    ('"double"', '1.1', 1.1),
    ('{"type": "fixed", "name": "F", "size": 2}', '"\u00FF\u00FF"', u'\xff\xff'),
    ('{"type": "enum", "name": "F", "symbols": ["FOO", "BAR"]}', '"FOO"', 'FOO'),
    ('{"type": "array", "items": "int"}', '[1, 2, 3]', [1, 2, 3]),
    ('{"type": "map", "values": "int"}', '{"a": 1, "b": 2}', {'a': 1,
                                                              'b': 2}),
    ('["int", "null"]', '5', 5),
    ('{"type": "record", "name": "F", "fields": [{"name": "A", "type": "int"}]}',
     '{"A": 5}', {'A': 5}),
)

LONG_RECORD_SCHEMA = avro.schema.parse("""\
  {"type": "record",
   "name": "Test",
   "fields": [{"name": "A", "type": "int"},
              {"name": "B", "type": "int"},
              {"name": "C", "type": "int"},
              {"name": "D", "type": "int"},
              {"name": "E", "type": "int"},
              {"name": "F", "type": "int"},
              {"name": "G", "type": "int"}]}""")

LONG_RECORD_DATUM = {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6, 'G': 7}


def avro_hexlify(reader):
    """Return the hex value, as a string, of a binary-encoded int or long."""
    b = []
    current_byte = reader.read(1)
    b.append(binascii.hexlify(current_byte))
    while (ord(current_byte) & 0x80) != 0:
        current_byte = reader.read(1)
        b.append(binascii.hexlify(current_byte))
    return b' '.join(b)


def print_test_name(test_name):
    print('')
    print(test_name)
    print('=' * len(test_name))
    print('')


def write_datum(datum, writers_schema):
    writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(writer)
    datum_writer = avro.io.DatumWriter(writers_schema)
    datum_writer.write(datum, encoder)
    return writer, encoder, datum_writer


def read_datum(buffer, writers_schema, readers_schema=None):
    reader = io.BytesIO(buffer.getvalue())
    decoder = avro.io.BinaryDecoder(reader)
    datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
    return datum_reader.read(decoder)


def check_binary_encoding(number_type):
    print_test_name('TEST BINARY %s ENCODING' % number_type.upper())
    correct = 0
    for datum, hex_encoding in BINARY_ENCODINGS:
        print('Datum: %d' % datum)
        print('Correct Encoding: %s' % hex_encoding)

        writers_schema = avro.schema.parse('"%s"' % number_type.lower())
        writer, encoder, datum_writer = write_datum(datum, writers_schema)
        writer.seek(0)
        hex_val = avro_hexlify(writer)

        print('Read Encoding: %s' % hex_val)
        if hex_encoding == hex_val:
            correct += 1
        print('')
    return correct


def check_skip_number(number_type):
    print_test_name('TEST SKIP %s' % number_type.upper())
    correct = 0
    for value_to_skip, hex_encoding in BINARY_ENCODINGS:
        VALUE_TO_READ = 6253
        print('Value to Skip: %d' % value_to_skip)

        # write the value to skip and a known value
        writers_schema = avro.schema.parse('"%s"' % number_type.lower())
        writer, encoder, datum_writer = write_datum(value_to_skip, writers_schema)
        datum_writer.write(VALUE_TO_READ, encoder)

        # skip the value
        reader = io.BytesIO(writer.getvalue())
        decoder = avro.io.BinaryDecoder(reader)
        decoder.skip_long()

        # read data from string buffer
        datum_reader = avro.io.DatumReader(writers_schema)
        read_value = datum_reader.read(decoder)

        print('Read Value: %d' % read_value)
        if read_value == VALUE_TO_READ:
            correct += 1
        print('')
    return correct


class TestIO(unittest.TestCase):
    #
    # BASIC FUNCTIONALITY
    #

    def test_validate(self):
        print_test_name('TEST VALIDATE')
        passed = 0
        for example_schema, datum in SCHEMAS_TO_VALIDATE:
            print('Schema: %s' % example_schema)
            print('Datum: %s' % datum)
            validated = avro.io.validate(avro.schema.parse(example_schema), datum)
            print('Valid: %s' % validated)
            if validated:
                passed += 1
        self.assertEqual(passed, len(SCHEMAS_TO_VALIDATE))

    def test_round_trip(self):
        print_test_name('TEST ROUND TRIP')
        correct = 0
        for example_schema, datum in SCHEMAS_TO_VALIDATE:
            print('Schema: %s' % example_schema)
            print('Datum: %s' % datum)

            writers_schema = avro.schema.parse(example_schema)
            writer, encoder, datum_writer = write_datum(datum, writers_schema)
            round_trip_datum = read_datum(writer, writers_schema)

            print('Round Trip Datum: %s' % round_trip_datum)
            if isinstance(round_trip_datum, decimal.Decimal):
                round_trip_datum = round_trip_datum.to_eng_string()
                datum = str(datum)
            elif isinstance(round_trip_datum, datetime.datetime):
                datum = datum.astimezone(tz=avro.timezones.utc)
            if datum == round_trip_datum:
                correct += 1
        self.assertEqual(correct, len(SCHEMAS_TO_VALIDATE))

    #
    # BINARY ENCODING OF INT AND LONG
    #

    def test_binary_int_encoding(self):
        correct = check_binary_encoding('int')
        self.assertEqual(correct, len(BINARY_ENCODINGS))

    def test_binary_long_encoding(self):
        correct = check_binary_encoding('long')
        self.assertEqual(correct, len(BINARY_ENCODINGS))

    def test_skip_int(self):
        correct = check_skip_number('int')
        self.assertEqual(correct, len(BINARY_ENCODINGS))

    def test_skip_long(self):
        correct = check_skip_number('long')
        self.assertEqual(correct, len(BINARY_ENCODINGS))

    #
    # SCHEMA RESOLUTION
    #

    def test_schema_promotion(self):
        print_test_name('TEST SCHEMA PROMOTION')
        # note that checking writers_schema.type in read_data
        # allows us to handle promotion correctly
        promotable_schemas = ['"int"', '"long"', '"float"', '"double"']
        incorrect = 0
        for i, ws in enumerate(promotable_schemas):
            writers_schema = avro.schema.parse(ws)
            datum_to_write = 219
            for rs in promotable_schemas[i + 1:]:
                readers_schema = avro.schema.parse(rs)
                writer, enc, dw = write_datum(datum_to_write, writers_schema)
                datum_read = read_datum(writer, writers_schema, readers_schema)
                print('Writer: %s Reader: %s' % (writers_schema, readers_schema))
                print('Datum Read: %s' % datum_read)
                if datum_read != datum_to_write:
                    incorrect += 1
        self.assertEqual(incorrect, 0)

    def test_unknown_symbol(self):
        print_test_name('TEST UNKNOWN SYMBOL')
        writers_schema = avro.schema.parse("""\
      {"type": "enum", "name": "Test",
       "symbols": ["FOO", "BAR"]}""")
        datum_to_write = 'FOO'

        readers_schema = avro.schema.parse("""\
      {"type": "enum", "name": "Test",
       "symbols": ["BAR", "BAZ"]}""")

        writer, encoder, datum_writer = write_datum(datum_to_write, writers_schema)
        reader = io.BytesIO(writer.getvalue())
        decoder = avro.io.BinaryDecoder(reader)
        datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
        self.assertRaises(avro.errors.SchemaResolutionException, datum_reader.read, decoder)

    def test_default_value(self):
        print_test_name('TEST DEFAULT VALUE')
        writers_schema = LONG_RECORD_SCHEMA
        datum_to_write = LONG_RECORD_DATUM

        correct = 0
        for field_type, default_json, default_datum in DEFAULT_VALUE_EXAMPLES:
            readers_schema = avro.schema.parse("""\
        {"type": "record", "name": "Test",
         "fields": [{"name": "H", "type": %s, "default": %s}]}
        """ % (field_type, default_json))
            datum_to_read = {'H': default_datum}

            writer, encoder, datum_writer = write_datum(datum_to_write, writers_schema)
            datum_read = read_datum(writer, writers_schema, readers_schema)
            print('Datum Read: %s' % datum_read)
            if datum_to_read == datum_read:
                correct += 1
        self.assertEqual(correct, len(DEFAULT_VALUE_EXAMPLES))

    def test_no_default_value(self):
        print_test_name('TEST NO DEFAULT VALUE')
        writers_schema = LONG_RECORD_SCHEMA
        datum_to_write = LONG_RECORD_DATUM

        readers_schema = avro.schema.parse("""\
      {"type": "record", "name": "Test",
       "fields": [{"name": "H", "type": "int"}]}""")

        writer, encoder, datum_writer = write_datum(datum_to_write, writers_schema)
        reader = io.BytesIO(writer.getvalue())
        decoder = avro.io.BinaryDecoder(reader)
        datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
        self.assertRaises(avro.errors.SchemaResolutionException, datum_reader.read, decoder)

    def test_projection(self):
        print_test_name('TEST PROJECTION')
        writers_schema = LONG_RECORD_SCHEMA
        datum_to_write = LONG_RECORD_DATUM

        readers_schema = avro.schema.parse("""\
      {"type": "record", "name": "Test",
       "fields": [{"name": "E", "type": "int"},
                  {"name": "F", "type": "int"}]}""")
        datum_to_read = {'E': 5, 'F': 6}

        writer, encoder, datum_writer = write_datum(datum_to_write, writers_schema)
        datum_read = read_datum(writer, writers_schema, readers_schema)
        print('Datum Read: %s' % datum_read)
        self.assertEqual(datum_to_read, datum_read)

    def test_field_order(self):
        print_test_name('TEST FIELD ORDER')
        writers_schema = LONG_RECORD_SCHEMA
        datum_to_write = LONG_RECORD_DATUM

        readers_schema = avro.schema.parse("""\
      {"type": "record", "name": "Test",
       "fields": [{"name": "F", "type": "int"},
                  {"name": "E", "type": "int"}]}""")
        datum_to_read = {'E': 5, 'F': 6}

        writer, encoder, datum_writer = write_datum(datum_to_write, writers_schema)
        datum_read = read_datum(writer, writers_schema, readers_schema)
        print('Datum Read: %s' % datum_read)
        self.assertEqual(datum_to_read, datum_read)

    def test_type_exception(self):
        print_test_name('TEST TYPE EXCEPTION')
        writers_schema = avro.schema.parse("""\
      {"type": "record", "name": "Test",
       "fields": [{"name": "F", "type": "int"},
                  {"name": "E", "type": "int"}]}""")
        datum_to_write = {'E': 5, 'F': 'Bad'}
        self.assertRaises(avro.errors.AvroTypeException, write_datum, datum_to_write, writers_schema)


if __name__ == '__main__':
    unittest.main()
