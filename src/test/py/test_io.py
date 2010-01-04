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

if __name__ == '__main__':
  unittest.main()
