# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest
import cStringIO
from binascii import hexlify
from avro import schema
from avro import io

SCHEMAS_TO_VALIDATE = (
  ('"null"', None),
  ('"boolean"', True),
  ('"string"', unicode('adsfasdf09809dsf-=adsf')),
  ('"bytes"', '12345abcd'),
  ('"int"', 1234),
  ('"long"', 1234),
  ('"float"', 1234.0),
  ('"double"', 1234.0),
  ('{"type": "fixed", "name": "Test", "size": 1}', 'B'),
  ('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', 'B'),
  ('{"type": "array", "items": "long"}', [1, 3, 2]),
  ('{"type": "map", "values": "long"}', {'a': 1, 'b': 3, 'c': 2}),
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
   """, {'value': {'car': {'value': 'head'}, 'cdr': {'value': None}}}),
)

BINARY_INT_ENCODINGS = (
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

def avro_hexlify(reader):
  """Return the hex value, as a string, of a binary-encoded int or long."""
  bytes = []
  current_byte = reader.read(1)
  bytes.append(hexlify(current_byte))
  while (ord(current_byte) & 0x80) != 0:
    current_byte = reader.read(1)
    bytes.append(hexlify(current_byte))
  return ' '.join(bytes)

class TestIO(unittest.TestCase):
  def test_validate(self):
    print ''
    print 'Test Validate'
    print '============='
    print ''
    passed = 0
    for expected_schema, datum in SCHEMAS_TO_VALIDATE:
      print expected_schema, datum
      validated = io.validate(schema.parse(expected_schema), datum)
      print validated
      if validated: passed += 1
    self.assertEquals(passed, len(SCHEMAS_TO_VALIDATE))

  # TODO(hammer): print bytes in python
  def test_encode(self):
    print ''
    print 'Test Encode'
    print '============='
    print ''

    # boolean
    writer = cStringIO.StringIO()
    string_encoder = io.BinaryEncoder(writer)
    string_encoder.write_boolean(True)
    print 'Boolean: ' + repr(writer.getvalue())

    # string
    writer = cStringIO.StringIO()
    string_encoder = io.BinaryEncoder(writer)
    string_encoder.write_utf8(unicode('adsfasdf09809dsf-=adsf'))
    print 'String: ' + repr(writer.getvalue())

    # int
    writer = cStringIO.StringIO()
    string_encoder = io.BinaryEncoder(writer)
    string_encoder.write_int(1)
    print 'Int: ' + repr(writer.getvalue())

    # long
    writer = cStringIO.StringIO()
    string_encoder = io.BinaryEncoder(writer)
    string_encoder.write_long(1)
    print 'Long: ' + repr(writer.getvalue())

    # float
    writer = cStringIO.StringIO()
    string_encoder = io.BinaryEncoder(writer)
    string_encoder.write_float(1.0)
    print 'Float: ' + repr(writer.getvalue())

    # double
    writer = cStringIO.StringIO()
    string_encoder = io.BinaryEncoder(writer)
    string_encoder.write_double(1.0)
    print 'Double: ' + repr(writer.getvalue())

    # bytes
    writer = cStringIO.StringIO()
    string_encoder = io.BinaryEncoder(writer)
    string_encoder.write_bytes('12345abcd')
    print 'Bytes: ' + repr(writer.getvalue())
    
  def test_decode(self):
    pass

  def test_datum_reader(self):
    pass

  def test_datum_writer(self):
    pass

  def test_round_trip(self):
    print ''
    print 'TEST ROUND TRIP'
    print '==============='
    print ''
    correct = 0
    for example_schema, datum in SCHEMAS_TO_VALIDATE:
      print 'Schema: %s' % example_schema
      print 'Datum: %s' % datum
      print 'Valid: %s' % io.validate(schema.parse(example_schema), datum)

      # write datum in binary to string buffer
      writer = cStringIO.StringIO()
      encoder = io.BinaryEncoder(writer)
      datum_writer = io.DatumWriter(schema.parse(example_schema))
      datum_writer.write(datum, encoder)

      # read data from string buffer
      reader = cStringIO.StringIO(writer.getvalue())
      decoder = io.BinaryDecoder(reader)
      datum_reader = io.DatumReader(schema.parse(example_schema))
      round_trip_datum = datum_reader.read(decoder)

      print 'Round Trip Datum: %s' % round_trip_datum
      if datum == round_trip_datum: correct += 1
      print 'Correct Round Trip: %s' % (datum == round_trip_datum)
      print ''
    self.assertEquals(correct, len(SCHEMAS_TO_VALIDATE))

  def test_binary_int_encoding(self):
    print ''
    print 'TEST BINARY INT ENCODING'
    print '========================'
    print ''
    correct = 0
    for value, hex_encoding in BINARY_INT_ENCODINGS:
      print 'Value: %d' % value
      print 'Correct Encoding: %s' % hex_encoding

      # write datum in binary to string buffer
      buffer = cStringIO.StringIO()
      encoder = io.BinaryEncoder(buffer)
      datum_writer = io.DatumWriter(schema.parse('"int"'))
      datum_writer.write(value, encoder)

      # read it out of the buffer and hexlify it
      buffer.seek(0)
      hex_val = avro_hexlify(buffer)

      # check it
      print 'Read Encoding: %s' % hex_val
      if hex_encoding == hex_val: correct += 1
      print ''
    self.assertEquals(correct, len(BINARY_INT_ENCODINGS))

  def test_binary_long_encoding(self):
    print ''
    print 'TEST BINARY LONG ENCODING'
    print '========================='
    print ''
    correct = 0
    for value, hex_encoding in BINARY_INT_ENCODINGS:
      print 'Value: %d' % value
      print 'Correct Encoding: %s' % hex_encoding

      # write datum in binary to string buffer
      buffer = cStringIO.StringIO()
      encoder = io.BinaryEncoder(buffer)
      datum_writer = io.DatumWriter(schema.parse('"long"'))
      datum_writer.write(value, encoder)

      # read it out of the buffer and hexlify it
      buffer.seek(0)
      hex_val = avro_hexlify(buffer)

      # check it
      print 'Read Encoding: %s' % hex_val
      if hex_encoding == hex_val: correct += 1
      print ''
    self.assertEquals(correct, len(BINARY_INT_ENCODINGS))

  def test_skip_long(self):
    print ''
    print 'TEST SKIP LONG'
    print '=============='
    print ''
    correct = 0
    for value_to_skip, hex_encoding in BINARY_INT_ENCODINGS:
      VALUE_TO_READ = 6253
      print 'Value to Skip: %d' % value_to_skip

      # write some data in binary to string buffer
      writer = cStringIO.StringIO()
      encoder = io.BinaryEncoder(writer)
      datum_writer = io.DatumWriter(schema.parse('"long"'))
      datum_writer.write(value_to_skip, encoder)
      datum_writer.write(VALUE_TO_READ, encoder)

      # skip the value
      reader = cStringIO.StringIO(writer.getvalue())
      decoder = io.BinaryDecoder(reader)
      decoder.skip_long()

      # read data from string buffer
      datum_reader = io.DatumReader(schema.parse('"long"'))
      read_value = datum_reader.read(decoder)

      # check it
      print 'Read Value: %d' % read_value
      if read_value == VALUE_TO_READ: correct += 1
      print ''
    self.assertEquals(correct, len(BINARY_INT_ENCODINGS))

  def test_skip_int(self):
    print ''
    print 'TEST SKIP INT'
    print '============='
    print ''
    correct = 0
    for value_to_skip, hex_encoding in BINARY_INT_ENCODINGS:
      VALUE_TO_READ = 6253
      print 'Value to Skip: %d' % value_to_skip

      # write some data in binary to string buffer
      writer = cStringIO.StringIO()
      encoder = io.BinaryEncoder(writer)
      datum_writer = io.DatumWriter(schema.parse('"int"'))
      datum_writer.write(value_to_skip, encoder)
      datum_writer.write(VALUE_TO_READ, encoder)

      # skip the value
      reader = cStringIO.StringIO(writer.getvalue())
      decoder = io.BinaryDecoder(reader)
      decoder.skip_int()

      # read data from string buffer
      datum_reader = io.DatumReader(schema.parse('"int"'))
      read_value = datum_reader.read(decoder)

      # check it
      print 'Read Value: %d' % read_value
      if read_value == VALUE_TO_READ: correct += 1
      print ''
    self.assertEquals(correct, len(BINARY_INT_ENCODINGS))

if __name__ == '__main__':
  unittest.main()
