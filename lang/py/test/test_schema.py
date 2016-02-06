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
import set_avro_test_path

from avro import schema

def print_test_name(test_name):
  print ''
  print test_name
  print '=' * len(test_name)
  print ''

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
  ExampleSchema("""\
    {"type": "enum",
     "name": "Test"
     "symbols" : ["AA", "AA"]}
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
  ExampleSchema('["long", "long"]', False),
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
     "name": "ipAddr",
     "fields": [{"name": "addr", 
                 "type": [{"name": "IPv6", "type": "fixed", "size": 16},
                          {"name": "IPv4", "type": "fixed", "size": 4}]}]}
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

DOC_EXAMPLES = [
  ExampleSchema("""\
    {"type": "record",
     "name": "TestDoc",
     "doc":  "Doc string",
     "fields": [{"name": "name", "type": "string", 
                 "doc" : "Doc String"}]}
    """, True),
  ExampleSchema("""\
    {"type": "enum", "name": "Test", "symbols": ["A", "B"],
     "doc": "Doc String"}
    """, True),
]

OTHER_PROP_EXAMPLES = [
  ExampleSchema("""\
    {"type": "record",
     "name": "TestRecord",
     "cp_string": "string",
     "cp_int": 1,
     "cp_array": [ 1, 2, 3, 4],
     "fields": [ {"name": "f1", "type": "string", "cp_object": {"a":1,"b":2} },
                 {"name": "f2", "type": "long", "cp_null": null} ]}
    """, True),
  ExampleSchema("""\
     {"type": "map", "values": "long", "cp_boolean": true}
    """, True),
  ExampleSchema("""\
    {"type": "enum",
     "name": "TestEnum",
     "symbols": [ "one", "two", "three" ],
     "cp_float" : 1.0 }
    """,True),
  ExampleSchema("""\
    {"type": "long",
     "date": "true"}
    """, True)
]

EXAMPLES = PRIMITIVE_EXAMPLES
EXAMPLES += FIXED_EXAMPLES
EXAMPLES += ENUM_EXAMPLES
EXAMPLES += ARRAY_EXAMPLES
EXAMPLES += MAP_EXAMPLES
EXAMPLES += UNION_EXAMPLES
EXAMPLES += RECORD_EXAMPLES
EXAMPLES += DOC_EXAMPLES

VALID_EXAMPLES = [e for e in EXAMPLES if e.valid]

# TODO(hammer): refactor into harness for examples
# TODO(hammer): pretty-print detailed output
# TODO(hammer): make verbose flag
# TODO(hammer): show strack trace to user
# TODO(hammer): use logging module?
class TestSchema(unittest.TestCase):

  def test_correct_recursive_extraction(self):
    s = schema.parse('{"type": "record", "name": "X", "fields": [{"name": "y", "type": {"type": "record", "name": "Y", "fields": [{"name": "Z", "type": "X"}]}}]}')
    t = schema.parse(str(s.fields[0].type))
    # If we've made it this far, the subschema was reasonably stringified; it ccould be reparsed.
    self.assertEqual("X", t.fields[0].type.name)

  def test_parse(self):
    correct = 0
    for example in EXAMPLES:
      try:
        schema.parse(example.schema_string)
        if example.valid:
          correct += 1
        else:
          self.fail("Invalid schema was parsed: " + example.schema_string)
      except:
        if not example.valid: 
          correct += 1
        else:
          self.fail("Valid schema failed to parse: " + example.schema_string)

    fail_msg = "Parse behavior correct on %d out of %d schemas." % \
      (correct, len(EXAMPLES))
    self.assertEqual(correct, len(EXAMPLES), fail_msg)

  def test_valid_cast_to_string_after_parse(self):
    """
    Test that the string generated by an Avro Schema object
    is, in fact, a valid Avro schema.
    """
    print_test_name('TEST CAST TO STRING AFTER PARSE')
    correct = 0
    for example in VALID_EXAMPLES:
      schema_data = schema.parse(example.schema_string)
      schema.parse(str(schema_data))
      correct += 1

    fail_msg = "Cast to string success on %d out of %d schemas" % \
      (correct, len(VALID_EXAMPLES))
    self.assertEqual(correct, len(VALID_EXAMPLES), fail_msg)

  def test_equivalence_after_round_trip(self):
    """
    1. Given a string, parse it to get Avro schema "original".
    2. Serialize "original" to a string and parse that string
         to generate Avro schema "round trip".
    3. Ensure "original" and "round trip" schemas are equivalent.
    """
    print_test_name('TEST ROUND TRIP')
    correct = 0
    for example in VALID_EXAMPLES:
      original_schema = schema.parse(example.schema_string)
      round_trip_schema = schema.parse(str(original_schema))
      if original_schema == round_trip_schema:
        correct += 1
        debug_msg = "%s: ROUND TRIP SUCCESS" % example.name
      else:       
        debug_msg = "%s: ROUND TRIP FAILURE" % example.name
        self.fail("Round trip failure: %s, %s, %s" % (example.name, original_schema, str(original_schema)))

    fail_msg = "Round trip success on %d out of %d schemas" % \
      (correct, len(VALID_EXAMPLES))
    self.assertEqual(correct, len(VALID_EXAMPLES), fail_msg)

  # TODO(hammer): more tests
  def test_fullname(self):
    """
    The fullname is determined in one of the following ways:
     * A name and namespace are both specified.  For example,
       one might use "name": "X", "namespace": "org.foo"
       to indicate the fullname "org.foo.X".
     * A fullname is specified.  If the name specified contains
       a dot, then it is assumed to be a fullname, and any
       namespace also specified is ignored.  For example,
       use "name": "org.foo.X" to indicate the
       fullname "org.foo.X".
     * A name only is specified, i.e., a name that contains no
       dots.  In this case the namespace is taken from the most
       tightly encosing schema or protocol.  For example,
       if "name": "X" is specified, and this occurs
       within a field of the record definition
       of "org.foo.Y", then the fullname is "org.foo.X".

    References to previously defined names are as in the latter
    two cases above: if they contain a dot they are a fullname, if
    they do not contain a dot, the namespace is the namespace of
    the enclosing definition.

    Primitive type names have no namespace and their names may
    not be defined in any namespace.  A schema may only contain
    multiple definitions of a fullname if the definitions are
    equivalent.
    """
    print_test_name('TEST FULLNAME')

    # name and namespace specified    
    fullname = schema.Name('a', 'o.a.h', None).fullname
    self.assertEqual(fullname, 'o.a.h.a')

    # fullname and namespace specified
    fullname = schema.Name('a.b.c.d', 'o.a.h', None).fullname
    self.assertEqual(fullname, 'a.b.c.d')
    
    # name and default namespace specified
    fullname = schema.Name('a', None, 'b.c.d').fullname
    self.assertEqual(fullname, 'b.c.d.a')

    # fullname and default namespace specified
    fullname = schema.Name('a.b.c.d', None, 'o.a.h').fullname
    self.assertEqual(fullname, 'a.b.c.d')

    # fullname, namespace, default namespace specified
    fullname = schema.Name('a.b.c.d', 'o.a.a', 'o.a.h').fullname
    self.assertEqual(fullname, 'a.b.c.d')

    # name, namespace, default namespace specified
    fullname = schema.Name('a', 'o.a.a', 'o.a.h').fullname
    self.assertEqual(fullname, 'o.a.a.a')

  def test_doc_attributes(self):
    print_test_name('TEST DOC ATTRIBUTES')
    correct = 0
    for example in DOC_EXAMPLES:
      original_schema = schema.parse(example.schema_string)
      if original_schema.doc is not None:
        correct += 1
      if original_schema.type == 'record':
        for f in original_schema.fields:
          if f.doc is None:
            self.fail("Failed to preserve 'doc' in fields: " + example.schema_string)
    self.assertEqual(correct,len(DOC_EXAMPLES))

  def test_other_attributes(self):
    print_test_name('TEST OTHER ATTRIBUTES')
    correct = 0
    props = {}
    for example in OTHER_PROP_EXAMPLES:
      original_schema = schema.parse(example.schema_string)
      round_trip_schema = schema.parse(str(original_schema))
      self.assertEqual(original_schema.other_props,round_trip_schema.other_props)
      if original_schema.type == "record":
        field_props = 0
        for f in original_schema.fields:
          if f.other_props:
            props.update(f.other_props)
            field_props += 1
        self.assertEqual(field_props,len(original_schema.fields))
      if original_schema.other_props:
        props.update(original_schema.other_props)
        correct += 1
    for k in props:
      v = props[k]
      if k == "cp_boolean":
        self.assertEqual(type(v), bool)
      elif k == "cp_int":
        self.assertEqual(type(v), int)
      elif k == "cp_object":
        self.assertEqual(type(v), dict)
      elif k == "cp_float":
        self.assertEqual(type(v), float)
      elif k == "cp_array":
        self.assertEqual(type(v), list)
    self.assertEqual(correct,len(OTHER_PROP_EXAMPLES))

  def test_exception_is_not_swallowed_on_parse_error(self):
    print_test_name('TEST EXCEPTION NOT SWALLOWED ON PARSE ERROR')

    try:
        schema.parse('/not/a/real/file')
        caught_exception = False
    except schema.SchemaParseException, e:
        expected_message = 'Error parsing JSON: /not/a/real/file, error = ' \
                           'No JSON object could be decoded'
        self.assertEqual(expected_message, e.args[0])
        caught_exception = True

    self.assertTrue(caught_exception, 'Exception was not caught')

if __name__ == '__main__':
  unittest.main()
