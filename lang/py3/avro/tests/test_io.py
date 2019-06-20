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
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import binascii
import io
import logging
import sys
import unittest

from avro import io as avro_io
from avro import schema


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
  ("""\
   {"type": "record",
    "name": "Test",
    "fields": [{"name": "f", "type": "long"}]}
   """, {'f': 5}),
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
   """, {'value': {'car': {'value': 'head'}, 'cdr': {'value': None}}}),
)

BINARY_ENCODINGS = (
  (0, '00'),
  (-1, '01'),
  (1, '02'),
  (-2, '03'),
  (2, '04'),
  (-64, '7f'),
  (64, '80 01'),
  (8192, '80 80 01'),
  (-8193, '81 80 01'),
)

DEFAULT_VALUE_EXAMPLES = (
  ('"null"', 'null', None),
  ('"boolean"', 'true', True),
  ('"string"', '"foo"', 'foo'),
  ('"bytes"', '"\u00FF\u00FF"', '\xff\xff'),
  ('"int"', '5', 5),
  ('"long"', '5', 5),
  ('"float"', '1.1', 1.1),
  ('"double"', '1.1', 1.1),
  ('{"type": "fixed", "name": "F", "size": 2}', '"\u00FF\u00FF"', '\xff\xff'),
  ('{"type": "enum", "name": "F", "symbols": ["FOO", "BAR"]}', '"FOO"', 'FOO'),
  ('{"type": "array", "items": "int"}', '[1, 2, 3]', [1, 2, 3]),
  ('{"type": "map", "values": "int"}', '{"a": 1, "b": 2}', {'a': 1, 'b': 2}),
  ('["int", "null"]', '5', 5),
  ('{"type": "record", "name": "F", "fields": [{"name": "A", "type": "int"}]}',
   '{"A": 5}', {'A': 5}),
)

LONG_RECORD_SCHEMA = schema.Parse("""
{
  "type": "record",
  "name": "Test",
  "fields": [
    {"name": "A", "type": "int"},
    {"name": "B", "type": "int"},
    {"name": "C", "type": "int"},
    {"name": "D", "type": "int"},
    {"name": "E", "type": "int"},
    {"name": "F", "type": "int"},
    {"name": "G", "type": "int"}
  ]
}
""")

LONG_RECORD_DATUM = {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6, 'G': 7}


def avro_hexlify(reader):
  """Return the hex value, as a string, of a binary-encoded int or long."""
  bytes = []
  current_byte = reader.read(1)
  bytes.append(binascii.hexlify(current_byte).decode())
  while (ord(current_byte) & 0x80) != 0:
    current_byte = reader.read(1)
    bytes.append(binascii.hexlify(current_byte).decode())
  return ' '.join(bytes)


def write_datum(datum, writer_schema):
  writer = io.BytesIO()
  encoder = avro_io.BinaryEncoder(writer)
  datum_writer = avro_io.DatumWriter(writer_schema)
  datum_writer.write(datum, encoder)
  return writer, encoder, datum_writer


def read_datum(buffer, writer_schema, reader_schema=None):
  reader = io.BytesIO(buffer.getvalue())
  decoder = avro_io.BinaryDecoder(reader)
  datum_reader = avro_io.DatumReader(writer_schema, reader_schema)
  return datum_reader.read(decoder)


def check_binary_encoding(number_type):
  logging.debug('Testing binary encoding for type %s', number_type)
  correct = 0
  for datum, hex_encoding in BINARY_ENCODINGS:
    logging.debug('Datum: %d', datum)
    logging.debug('Correct Encoding: %s', hex_encoding)

    writer_schema = schema.Parse('"%s"' % number_type.lower())
    writer, encoder, datum_writer = write_datum(datum, writer_schema)
    writer.seek(0)
    hex_val = avro_hexlify(writer)

    logging.debug('Read Encoding: %s', hex_val)
    if hex_encoding == hex_val: correct += 1
  return correct


def check_skip_number(number_type):
  logging.debug('Testing skip number for %s', number_type)
  correct = 0
  for value_to_skip, hex_encoding in BINARY_ENCODINGS:
    VALUE_TO_READ = 6253
    logging.debug('Value to Skip: %d', value_to_skip)

    # write the value to skip and a known value
    writer_schema = schema.Parse('"%s"' % number_type.lower())
    writer, encoder, datum_writer = write_datum(value_to_skip, writer_schema)
    datum_writer.write(VALUE_TO_READ, encoder)

    # skip the value
    reader = io.BytesIO(writer.getvalue())
    decoder = avro_io.BinaryDecoder(reader)
    decoder.skip_long()

    # read data from string buffer
    datum_reader = avro_io.DatumReader(writer_schema)
    read_value = datum_reader.read(decoder)

    logging.debug('Read Value: %d', read_value)
    if read_value == VALUE_TO_READ: correct += 1
  return correct


# ------------------------------------------------------------------------------


class TestIO(unittest.TestCase):
  #
  # BASIC FUNCTIONALITY
  #

  def testValidate(self):
    passed = 0
    for example_schema, datum in SCHEMAS_TO_VALIDATE:
      logging.debug('Schema: %r', example_schema)
      logging.debug('Datum: %r', datum)
      validated = avro_io.Validate(schema.Parse(example_schema), datum)
      logging.debug('Valid: %s', validated)
      if validated: passed += 1
    self.assertEqual(passed, len(SCHEMAS_TO_VALIDATE))

  def testRoundTrip(self):
    correct = 0
    for example_schema, datum in SCHEMAS_TO_VALIDATE:
      logging.debug('Schema: %s', example_schema)
      logging.debug('Datum: %s', datum)

      writer_schema = schema.Parse(example_schema)
      writer, encoder, datum_writer = write_datum(datum, writer_schema)
      round_trip_datum = read_datum(writer, writer_schema)

      logging.debug('Round Trip Datum: %s', round_trip_datum)
      if datum == round_trip_datum: correct += 1
    self.assertEqual(correct, len(SCHEMAS_TO_VALIDATE))

  #
  # BINARY ENCODING OF INT AND LONG
  #

  def testBinaryIntEncoding(self):
    correct = check_binary_encoding('int')
    self.assertEqual(correct, len(BINARY_ENCODINGS))

  def testBinaryLongEncoding(self):
    correct = check_binary_encoding('long')
    self.assertEqual(correct, len(BINARY_ENCODINGS))

  def testSkipInt(self):
    correct = check_skip_number('int')
    self.assertEqual(correct, len(BINARY_ENCODINGS))

  def testSkipLong(self):
    correct = check_skip_number('long')
    self.assertEqual(correct, len(BINARY_ENCODINGS))

  #
  # SCHEMA RESOLUTION
  #

  def testSchemaPromotion(self):
    # note that checking writer_schema.type in read_data
    # allows us to handle promotion correctly
    promotable_schemas = ['"int"', '"long"', '"float"', '"double"']
    incorrect = 0
    for i, ws in enumerate(promotable_schemas):
      writer_schema = schema.Parse(ws)
      datum_to_write = 219
      for rs in promotable_schemas[i + 1:]:
        reader_schema = schema.Parse(rs)
        writer, enc, dw = write_datum(datum_to_write, writer_schema)
        datum_read = read_datum(writer, writer_schema, reader_schema)
        logging.debug('Writer: %s Reader: %s', writer_schema, reader_schema)
        logging.debug('Datum Read: %s', datum_read)
        if datum_read != datum_to_write: incorrect += 1
    self.assertEqual(incorrect, 0)

  def testUnknownSymbol(self):
    writer_schema = schema.Parse("""\
      {"type": "enum", "name": "Test",
       "symbols": ["FOO", "BAR"]}""")
    datum_to_write = 'FOO'

    reader_schema = schema.Parse("""\
      {"type": "enum", "name": "Test",
       "symbols": ["BAR", "BAZ"]}""")

    writer, encoder, datum_writer = write_datum(datum_to_write, writer_schema)
    reader = io.BytesIO(writer.getvalue())
    decoder = avro_io.BinaryDecoder(reader)
    datum_reader = avro_io.DatumReader(writer_schema, reader_schema)
    self.assertRaises(avro_io.SchemaResolutionException, datum_reader.read, decoder)

  def testDefaultValue(self):
    writer_schema = LONG_RECORD_SCHEMA
    datum_to_write = LONG_RECORD_DATUM

    correct = 0
    for field_type, default_json, default_datum in DEFAULT_VALUE_EXAMPLES:
      reader_schema = schema.Parse("""\
        {"type": "record", "name": "Test",
         "fields": [{"name": "H", "type": %s, "default": %s}]}
        """ % (field_type, default_json))
      datum_to_read = {'H': default_datum}

      writer, encoder, datum_writer = write_datum(datum_to_write, writer_schema)
      datum_read = read_datum(writer, writer_schema, reader_schema)
      logging.debug('Datum Read: %s', datum_read)
      if datum_to_read == datum_read: correct += 1
    self.assertEqual(correct, len(DEFAULT_VALUE_EXAMPLES))

  def testNoDefaultValue(self):
    writer_schema = LONG_RECORD_SCHEMA
    datum_to_write = LONG_RECORD_DATUM

    reader_schema = schema.Parse("""\
      {"type": "record", "name": "Test",
       "fields": [{"name": "H", "type": "int"}]}""")

    writer, encoder, datum_writer = write_datum(datum_to_write, writer_schema)
    reader = io.BytesIO(writer.getvalue())
    decoder = avro_io.BinaryDecoder(reader)
    datum_reader = avro_io.DatumReader(writer_schema, reader_schema)
    self.assertRaises(avro_io.SchemaResolutionException, datum_reader.read, decoder)

  def testProjection(self):
    writer_schema = LONG_RECORD_SCHEMA
    datum_to_write = LONG_RECORD_DATUM

    reader_schema = schema.Parse("""\
      {"type": "record", "name": "Test",
       "fields": [{"name": "E", "type": "int"},
                  {"name": "F", "type": "int"}]}""")
    datum_to_read = {'E': 5, 'F': 6}

    writer, encoder, datum_writer = write_datum(datum_to_write, writer_schema)
    datum_read = read_datum(writer, writer_schema, reader_schema)
    logging.debug('Datum Read: %s', datum_read)
    self.assertEqual(datum_to_read, datum_read)

  def testFieldOrder(self):
    writer_schema = LONG_RECORD_SCHEMA
    datum_to_write = LONG_RECORD_DATUM

    reader_schema = schema.Parse("""\
      {"type": "record", "name": "Test",
       "fields": [{"name": "F", "type": "int"},
                  {"name": "E", "type": "int"}]}""")
    datum_to_read = {'E': 5, 'F': 6}

    writer, encoder, datum_writer = write_datum(datum_to_write, writer_schema)
    datum_read = read_datum(writer, writer_schema, reader_schema)
    logging.debug('Datum Read: %s', datum_read)
    self.assertEqual(datum_to_read, datum_read)

  def testTypeException(self):
    writer_schema = schema.Parse("""\
      {"type": "record", "name": "Test",
       "fields": [{"name": "F", "type": "int"},
                  {"name": "E", "type": "int"}]}""")
    datum_to_write = {'E': 5, 'F': 'Bad'}
    self.assertRaises(
        avro_io.AvroTypeException, write_datum, datum_to_write, writer_schema)

  def testUnionSchemaSpecificity(self):
    union_schema = schema.Parse("""
        [{
         "type" : "record",
         "name" : "A",
         "fields" : [{"name" : "foo", "type" : ["string", "null"]}]
        },
        {
         "type" : "record",
         "name" : "B",
         "fields" : [{"name" : "bar", "type" : ["string", "null"]}]
        },
        {
         "type" : "record",
         "name" : "AOrB",
         "fields" : [{"name" : "entity", "type" : ["A", "B"]}]
        }]
    """)
    sch = {s.name: s for s in union_schema.schemas}.get('AOrB')
    datum_to_read = {'entity': {'foo': 'this is an instance of schema A'}}
    writer, encoder, datum_writer = write_datum(datum_to_read, sch)
    datum_read = read_datum(writer, sch, sch)
    self.assertEqual(datum_to_read, datum_read)


if __name__ == '__main__':
  raise Exception('Use run_tests.py')
