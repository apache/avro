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
"""
Test the schema parsing logic.
"""
import unittest
from avro import schema

class ExampleSchema(object):
  def __init__(self, schema_string, valid, name='', comment=''):
    self._schema_string = schema_string
    self._valid = valid
    self._name = name or schema_string # default to schema_string for name
    self.comment = comment

  @property
  def schema_string(self):
    return self._schema_string

  @property
  def valid(self):
    return self._valid

  @property
  def name(self):
    return self._name

#
# Example Schemas
#

def make_primitive_examples():
  examples = []
  for type in schema.PRIMITIVE_TYPES:
    examples.append(ExampleSchema('"%s"' % type, True))
    examples.append(ExampleSchema('{"type": "%s"}' % type, True))
  return examples

PRIMITIVE_EXAMPLES = [
  ExampleSchema('"True"', False),
  ExampleSchema('True', False),
  ExampleSchema('{"no_type": "test"}', False),
  ExampleSchema('{"type": "panther"}', False),
] + make_primitive_examples()

FIXED_EXAMPLES = [
  ExampleSchema('{"type": "fixed", "name": "Test", "size": 1}', True),
  ExampleSchema("""\
    {"type": "fixed",
     "name": "MyFixed",
     "namespace": "org.apache.hadoop.avro",
     "size": 1}
    """, True),
  ExampleSchema("""\
    {"type": "fixed",
     "name": "Missing size"}
    """, False),
  ExampleSchema("""\
    {"type": "fixed",
     "size": 314}
    """, False),
]

ENUM_EXAMPLES = [
  ExampleSchema('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', True),
  ExampleSchema("""\
    {"type": "enum",
     "name": "Status",
     "symbols": "Normal Caution Critical"}
    """, False),
  ExampleSchema("""\
    {"type": "enum",
     "name": [ 0, 1, 1, 2, 3, 5, 8 ],
     "symbols": ["Golden", "Mean"]}
    """, False),
  ExampleSchema("""\
    {"type": "enum",
     "symbols" : ["I", "will", "fail", "no", "name"]}
    """, False),
]

ARRAY_EXAMPLES = [
  ExampleSchema('{"type": "array", "items": "long"}', True),
  ExampleSchema("""\
    {"type": "array",
     "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}
    """, True),
]

MAP_EXAMPLES = [
  ExampleSchema('{"type": "map", "values": "long"}', True),
  ExampleSchema("""\
    {"type": "map",
     "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}
    """, True),
]

UNION_EXAMPLES = [
  ExampleSchema('["string", "null", "long"]', True),
  ExampleSchema('["null", "null"]', False),
  ExampleSchema("""\
    [{"type": "array", "items": "long"}
     {"type": "array", "items": "string"}]
    """, False),
]

RECORD_EXAMPLES = [
  ExampleSchema("""\
    {"type": "record",
     "name": "Test",
     "fields": [{"name": "f",
                 "type": "long"}]}
    """, True),
  ExampleSchema("""\
    {"type": "error",
     "name": "Test",
     "fields": [{"name": "f",
                 "type": "long"}]}
    """, True),
  ExampleSchema("""\
    {"type": "record",
     "name": "Node",
     "fields": [{"name": "label", "type": "string"},
                {"name": "children",
                 "type": {"type": "array", "items": "Node"}}]}
    """, True),
  ExampleSchema("""\
    {"type": "record",
     "name": "Lisp",
     "fields": [{"name": "value",
                 "type": ["null", "string",
                          {"type": "record",
                           "name": "Cons",
                           "fields": [{"name": "car", "type": "Lisp"},
                                      {"name": "cdr", "type": "Lisp"}]}]}]}
    """, True),
  ExampleSchema("""\
    {"type": "record",
     "name": "HandshakeRequest",
     "namespace": "org.apache.avro.ipc",
     "fields": [{"name": "clientHash",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "clientProtocol", "type": ["null", "string"]},
                {"name": "serverHash", "type": "MD5"},
                {"name": "meta", 
                 "type": ["null", {"type": "map", "values": "bytes"}]}]}
    """, True),
  ExampleSchema("""\
    {"type": "record",
     "name": "HandshakeResponse",
     "namespace": "org.apache.avro.ipc",
     "fields": [{"name": "match",
                 "type": {"type": "enum",
                          "name": "HandshakeMatch",
                          "symbols": ["BOTH", "CLIENT", "NONE"]}},
                {"name": "serverProtocol", "type": ["null", "string"]},
                {"name": "serverHash",
                 "type": ["null",
                          {"name": "MD5", "size": 16, "type": "fixed"}]},
                {"name": "meta",
                 "type": ["null", {"type": "map", "values": "bytes"}]}]}
    """, True),
  ExampleSchema("""\
    {"type": "record",
     "name": "Interop",
     "namespace": "org.apache.avro",
     "fields": [{"name": "intField", "type": "int"},
                {"name": "longField", "type": "long"},
                {"name": "stringField", "type": "string"},
                {"name": "boolField", "type": "boolean"},
                {"name": "floatField", "type": "float"},
                {"name": "doubleField", "type": "double"},
                {"name": "bytesField", "type": "bytes"},
                {"name": "nullField", "type": "null"},
                {"name": "arrayField",
                 "type": {"type": "array", "items": "double"}},
                {"name": "mapField",
                 "type": {"type": "map",
                          "values": {"name": "Foo",
                                     "type": "record",
                                     "fields": [{"name": "label",
                                                 "type": "string"}]}}},
                {"name": "unionField",
                 "type": ["boolean",
                          "double",
                          {"type": "array", "items": "bytes"}]},
                {"name": "enumField",
                 "type": {"type": "enum",
                          "name": "Kind",
                          "symbols": ["A", "B", "C"]}},
                {"name": "fixedField",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "recordField",
                 "type": {"type": "record",
                          "name": "Node",
                          "fields": [{"name": "label", "type": "string"},
                                     {"name": "children",
                                      "type": {"type": "array",
                                               "items": "Node"}}]}}]}
    """, True),
  ExampleSchema("""\
    {"type": "record",
     "name": "Address",
     "fields": [{"type": "string"},
                {"type": "string", "name": "City"}]}
    """, False),
  ExampleSchema("""\
    {"type": "record",
     "name": "Event",
     "fields": [{"name": "Sponsor"},
                {"name": "City", "type": "string"}]}
    """, False),
  ExampleSchema("""\
    {"type": "record",
     "fields": "His vision, from the constantly passing bars,"
     "name", "Rainer"}
    """, False),
  ExampleSchema("""\
    {"name": ["Tom", "Jerry"],
     "type": "record",
     "fields": [{"name": "name", "type": "string"}]}
    """, False),
]

EXAMPLES = PRIMITIVE_EXAMPLES
EXAMPLES += FIXED_EXAMPLES
EXAMPLES += ENUM_EXAMPLES
EXAMPLES += ARRAY_EXAMPLES
EXAMPLES += MAP_EXAMPLES
EXAMPLES += UNION_EXAMPLES
EXAMPLES += RECORD_EXAMPLES

VALID_EXAMPLES = [e for e in EXAMPLES if e.valid]

# TODO(hammer): refactor into harness for examples
# TODO(hammer): pretty-print detailed output
# TODO(hammer): make verbose flag
# TODO(hammer): show strack trace to user
# TODO(hammer): use logging module?
class TestSchema(unittest.TestCase):
  def test_parse(self):
    debug_msg = "\nTEST PARSE\n"
    print debug_msg

    num_correct = 0
    for example in EXAMPLES:
      try:
        schema.parse(example.schema_string)
        if example.valid: num_correct += 1
        debug_msg = "%s: PARSE SUCCESS" % example.name
      except:
        if not example.valid: num_correct += 1
        debug_msg = "%s: PARSE FAILURE" % example.name
      finally:
        print debug_msg

    fail_msg = "Parse behavior correct on %d out of %d schemas." % \
      (num_correct, len(EXAMPLES))
    self.assertEqual(num_correct, len(EXAMPLES), fail_msg)

  def test_valid_cast_to_string_after_parse(self):
    """
    Test that the string generated by an Avro Schema object
    is, in fact, a valid Avro schema.
    """
    debug_msg = "\nTEST CAST TO STRING\n"
    print debug_msg

    num_correct = 0
    for example in VALID_EXAMPLES:
      schema_data = schema.parse(example.schema_string)
      try:
        schema.parse(str(schema_data))
        debug_msg = "%s: STRING CAST SUCCESS" % example.name
        num_correct += 1
      except:
        debug_msg = "%s: STRING CAST FAILURE" % example.name
      finally:
        print debug_msg

    fail_msg = "Cast to string success on %d out of %d schemas" % \
      (num_correct, len(VALID_EXAMPLES))
    self.assertEqual(num_correct, len(VALID_EXAMPLES), fail_msg)

  def test_equivalence_after_round_trip(self):
    """
    1. Given a string, parse it to get Avro schema "original".
    2. Serialize "original" to a string and parse that string
         to generate Avro schema "round trip".
    3. Ensure "original" and "round trip" schemas are equivalent.
    """
    debug_msg = "\nTEST ROUND TRIP\n"
    print debug_msg

    num_correct = 0
    for example in VALID_EXAMPLES:
      try:
        original_schema = schema.parse(example.schema_string)
        round_trip_schema = schema.parse(str(original_schema))

        if original_schema == round_trip_schema:
          num_correct += 1
          debug_msg = "%s: ROUND TRIP SUCCESS" % example.name
        else:       
          debug_msg = "%s: ROUND TRIP FAILURE" % example.name
      except:
        debug_msg = "%s: ROUND TRIP FAILURE" % example.name
      finally:
        print debug_msg

    fail_msg = "Round trip success on %d out of %d schemas" % \
      (num_correct, len(VALID_EXAMPLES))
    self.assertEqual(num_correct, len(VALID_EXAMPLES), fail_msg)

  # TODO(hammer): more tests
  def test_fullname(self):
    """Test process for making full names from name, namespace pairs."""
    debug_msg = '\nTEST FULL NAME\n'
    print debug_msg

    fullname = schema.Name.make_fullname('a', 'o.a.h')
    self.assertEqual(fullname, 'o.a.h.a')

if __name__ == '__main__':
  unittest.main()
