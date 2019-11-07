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

"""Test the schema parsing logic."""

from __future__ import absolute_import, division, print_function

import json
import unittest

import set_avro_test_path
from avro import schema
from avro.schema import AvroException, SchemaParseException


class TestSchema(object):
  """A proxy for a schema string that provides useful test metadata."""

  def __init__(self, data, name='', comment=''):
    if not isinstance(data, basestring):
      data = json.dumps(data)
    self.data = data
    self.name = name or data  # default to data for name
    self.comment = comment

  def parse(self):
    return schema.parse(str(self))

  def __str__(self):
    return str(self.data)


class ValidTestSchema(TestSchema):
  """A proxy for a valid schema string that provides useful test metadata."""
  valid = True


class InvalidTestSchema(ValidTestSchema):
  """A proxy for an invalid schema string that provides useful test metadata."""
  valid = False


PRIMITIVE_EXAMPLES = ([
  InvalidTestSchema('"True"'),
  InvalidTestSchema('True'),
  InvalidTestSchema('{"no_type": "test"}'),
  InvalidTestSchema('{"type": "panther"}'),
] + [ValidTestSchema('"{}"'.format(t)) for t in schema.PRIMITIVE_TYPES]
  + [ValidTestSchema({"type": t}) for t in schema.PRIMITIVE_TYPES])

FIXED_EXAMPLES = [
  ValidTestSchema({"type": "fixed", "name": "Test", "size": 1}),
  ValidTestSchema({"type": "fixed", "name": "MyFixed", "size": 1,
                   "namespace": "org.apache.hadoop.avro"}),
  InvalidTestSchema({"type": "fixed", "name": "Missing size"}),
  InvalidTestSchema({"type": "fixed", "size": 314}),
]

ENUM_EXAMPLES = [
  ValidTestSchema({"type": "enum", "name": "Test", "symbols": ["A", "B"]}),
  InvalidTestSchema({"type": "enum", "name": "Status", "symbols": "Normal Caution Critical"}),
  InvalidTestSchema({"type": "enum", "name": [0, 1, 1, 2, 3, 5, 8],
                     "symbols": ["Golden", "Mean"]}),
  InvalidTestSchema({"type": "enum", "symbols" : ["I", "will", "fail", "no", "name"]}),
  InvalidTestSchema({"type": "enum", "name": "Test", "symbols": ["AA", "AA"]}),
]

ARRAY_EXAMPLES = [
  ValidTestSchema({"type": "array", "items": "long"}),
  ValidTestSchema({"type": "array",
                   "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}),
]

MAP_EXAMPLES = [
  ValidTestSchema({"type": "map", "values": "long"}),
  ValidTestSchema({"type": "map",
                   "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}),
]

UNION_EXAMPLES = [
  ValidTestSchema(["string", "null", "long"]),
  InvalidTestSchema(["null", "null"]),
  InvalidTestSchema(["long", "long"]),
  InvalidTestSchema([{"type": "array", "items": "long"},
                     {"type": "array", "items": "string"}]),
]

RECORD_EXAMPLES = [
  ValidTestSchema({"type": "record", "name": "Test", "fields": [{"name": "f", "type": "long"}]}),
  ValidTestSchema({"type": "error", "name": "Test", "fields": [{"name": "f", "type": "long"}]}),
  ValidTestSchema({"type": "record", "name": "Node",
                   "fields": [
                     {"name": "label", "type": "string"},
                     {"name": "children", "type": {"type": "array", "items": "Node"}}]}),
  ValidTestSchema({"type": "record", "name": "Lisp",
                   "fields": [{"name": "value",
                               "type": ["null", "string",
                                        {"type": "record", "name": "Cons",
                                         "fields": [{"name": "car", "type": "Lisp"},
                                                    {"name": "cdr", "type": "Lisp"}]}]}]}),
  ValidTestSchema({"type": "record", "name": "HandshakeRequest",
                   "namespace": "org.apache.avro.ipc",
                   "fields": [{"name": "clientHash",
                               "type": {"type": "fixed", "name": "MD5", "size": 16}},
                              {"name": "clientProtocol", "type": ["null", "string"]},
                              {"name": "serverHash", "type": "MD5"},
                              {"name": "meta",
                               "type": ["null", {"type": "map", "values": "bytes"}]}]}),
  ValidTestSchema({"type": "record", "name": "HandshakeResponse",
                   "namespace": "org.apache.avro.ipc",
                   "fields": [{"name": "match",
                               "type": {"type": "enum", "name": "HandshakeMatch",
                                        "symbols": ["BOTH", "CLIENT", "NONE"]}},
                              {"name": "serverProtocol", "type": ["null", "string"]},
                              {"name": "serverHash",
                               "type": ["null", {"name": "MD5", "size": 16, "type": "fixed"}]},
                              {"name": "meta",
                               "type": ["null", {"type": "map", "values": "bytes"}]}]}),
  ValidTestSchema({"type": "record",
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
                              {"name": "arrayField", "type": {"type": "array", "items": "double"}},
                              {"name": "mapField",
                               "type": {"type": "map",
                                        "values": {"name": "Foo",
                                                   "type": "record",
                                                   "fields": [{"name": "label", "type": "string"}]}}},
                              {"name": "unionField",
                               "type": ["boolean", "double", {"type": "array", "items": "bytes"}]},
                              {"name": "enumField",
                               "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B", "C"]}},
                              {"name": "fixedField",
                               "type": {"type": "fixed", "name": "MD5", "size": 16}},
                              {"name": "recordField",
                               "type": {"type": "record", "name": "Node",
                                        "fields": [{"name": "label", "type": "string"},
                                                   {"name": "children",
                                                    "type": {"type": "array",
                                                             "items": "Node"}}]}}]}),
  ValidTestSchema({"type": "record", "name": "ipAddr",
                   "fields": [{"name": "addr", "type": [{"name": "IPv6", "type": "fixed", "size": 16},
                                                        {"name": "IPv4", "type": "fixed", "size": 4}]}]}),
  InvalidTestSchema({"type": "record", "name": "Address",
                     "fields": [{"type": "string"}, {"type": "string", "name": "City"}]}),
  InvalidTestSchema({"type": "record", "name": "Event",
                     "fields": [{"name": "Sponsor"}, {"name": "City", "type": "string"}]}),
  InvalidTestSchema({"type": "record", "name": "Rainer",
                     "fields": "His vision, from the constantly passing bars"}),
  InvalidTestSchema({"name": ["Tom", "Jerry"], "type": "record",
                     "fields": [{"name": "name", "type": "string"}]}),
]

DOC_EXAMPLES = [
  ValidTestSchema({"type": "record", "name": "TestDoc", "doc": "Doc string",
                   "fields": [{"name": "name", "type": "string", "doc" : "Doc String"}]}),
  ValidTestSchema({"type": "enum", "name": "Test", "symbols": ["A", "B"], "doc": "Doc String"}),
]

OTHER_PROP_EXAMPLES = [
  ValidTestSchema({"type": "record", "name": "TestRecord", "cp_string": "string",
                   "cp_int": 1, "cp_array": [1, 2, 3, 4],
                   "fields": [{"name": "f1", "type": "string", "cp_object": {"a": 1,"b": 2}},
                              {"name": "f2", "type": "long", "cp_null": None}]}),
  ValidTestSchema({"type": "map", "values": "long", "cp_boolean": True}),
  ValidTestSchema({"type": "enum", "name": "TestEnum",
                   "symbols": ["one", "two", "three"], "cp_float": 1.0}),
]

DECIMAL_LOGICAL_TYPE = [
  ValidTestSchema({"type": "fixed", "logicalType": "decimal", "name": "TestDecimal", "precision": 4, "size": 10, "scale": 2}),
  ValidTestSchema({"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}),
  InvalidTestSchema({"type": "fixed", "logicalType": "decimal", "name": "TestDecimal2", "precision": 2, "scale": 2, "size": -2}),
]

DATE_LOGICAL_TYPE = [
  ValidTestSchema({"type": "int", "logicalType": "date"})
]

TIMEMILLIS_LOGICAL_TYPE = [
  ValidTestSchema({"type": "int", "logicalType": "time-millis"})
]

TIMEMICROS_LOGICAL_TYPE = [
  ValidTestSchema({"type": "long", "logicalType": "time-micros"})
]

TIMESTAMPMILLIS_LOGICAL_TYPE = [
  ValidTestSchema({"type": "long", "logicalType": "timestamp-millis"})
]

TIMESTAMPMICROS_LOGICAL_TYPE = [
  ValidTestSchema({"type": "long", "logicalType": "timestamp-micros"})
]

IGNORED_LOGICAL_TYPE = [
  ValidTestSchema({"type": "string", "logicalType": "uuid"}),
  ValidTestSchema({"type": "string", "logicalType": "unknown-logical-type"}),
  ValidTestSchema({"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": -2}),
  ValidTestSchema({"type": "bytes", "logicalType": "decimal", "precision": -2, "scale": 2}),
  ValidTestSchema({"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": 3}),
  ValidTestSchema({"type": "fixed", "logicalType": "decimal", "name": "TestIgnored", "precision": -10, "scale": 2, "size": 5}),
  ValidTestSchema({"type": "fixed", "logicalType": "decimal", "name": "TestIgnored2", "precision": 2, "scale": 3, "size": 2}),
  ValidTestSchema({"type": "int", "logicalType": "date1"}),
  ValidTestSchema({"type": "long", "logicalType": "date"}),
  ValidTestSchema({"type": "int", "logicalType": "time-milis"}),
  ValidTestSchema({"type": "long", "logicalType": "time-millis"}),
  ValidTestSchema({"type": "long", "logicalType": "time-micro"}),
  ValidTestSchema({"type": "int", "logicalType": "time-micros"}),
  ValidTestSchema({"type": "long", "logicalType": "timestamp-milis"}),
  ValidTestSchema({"type": "int", "logicalType": "timestamp-millis"}),
  ValidTestSchema({"type": "long", "logicalType": "timestamp-micro"}),
  ValidTestSchema({"type": "int", "logicalType": "timestamp-micros"})
]

EXAMPLES = PRIMITIVE_EXAMPLES
EXAMPLES += FIXED_EXAMPLES
EXAMPLES += ENUM_EXAMPLES
EXAMPLES += ARRAY_EXAMPLES
EXAMPLES += MAP_EXAMPLES
EXAMPLES += UNION_EXAMPLES
EXAMPLES += RECORD_EXAMPLES
EXAMPLES += DOC_EXAMPLES
EXAMPLES += DECIMAL_LOGICAL_TYPE
EXAMPLES += DATE_LOGICAL_TYPE
EXAMPLES += TIMEMILLIS_LOGICAL_TYPE
EXAMPLES += TIMEMICROS_LOGICAL_TYPE
EXAMPLES += TIMESTAMPMILLIS_LOGICAL_TYPE
EXAMPLES += TIMESTAMPMICROS_LOGICAL_TYPE
EXAMPLES += IGNORED_LOGICAL_TYPE

VALID_EXAMPLES = [e for e in EXAMPLES if e.valid]
INVALID_EXAMPLES = [e for e in EXAMPLES if not e.valid]

class TestSchema(unittest.TestCase):
  """Miscellaneous tests for schema"""

  def test_correct_recursive_extraction(self):
    """A recursive reference within a schema should be the same type every time."""
    s = schema.parse('{"type": "record", "name": "X", "fields": [{"name": "y", "type": {"type": "record", "name": "Y", "fields": [{"name": "Z", "type": "X"}]}}]}')
    t = schema.parse(str(s.fields[0].type))
    # If we've made it this far, the subschema was reasonably stringified; it ccould be reparsed.
    self.assertEqual("X", t.fields[0].type.name)

  # TODO(hammer): more tests
  def test_fullname(self):
    """Test schema full names

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

  def test_exception_is_not_swallowed_on_parse_error(self):
    """A specific exception message should appear on a json parse error."""
    try:
        schema.parse('/not/a/real/file')
        caught_exception = False
    except schema.SchemaParseException as e:
        expected_message = 'Error parsing JSON: /not/a/real/file, error = ' \
                           'No JSON object could be decoded'
        self.assertEqual(expected_message, e.args[0])
        caught_exception = True

    self.assertTrue(caught_exception, 'Exception was not caught')

  def test_decimal_valid_type(self):
    fixed_decimal_schema = ValidTestSchema({
      "type": "fixed",
      "logicalType": "decimal",
      "name": "TestDecimal",
      "precision": 4,
      "scale": 2,
      "size": 2})

    bytes_decimal_schema = ValidTestSchema({
      "type": "bytes",
      "logicalType": "decimal",
      "precision": 4})

    fixed_decimal = fixed_decimal_schema.parse()
    self.assertEqual(4, fixed_decimal.get_prop('precision'))
    self.assertEqual(2, fixed_decimal.get_prop('scale'))
    self.assertEqual(2, fixed_decimal.get_prop('size'))

    bytes_decimal = bytes_decimal_schema.parse()
    self.assertEqual(4, bytes_decimal.get_prop('precision'))
    self.assertEqual(0, bytes_decimal.get_prop('scale'))

  def test_fixed_decimal_valid_max_precision(self):
    # An 8 byte number can represent any 18 digit number.
    fixed_decimal_schema = ValidTestSchema({
      "type": "fixed",
      "logicalType": "decimal",
      "name": "TestDecimal",
      "precision": 18,
      "scale": 0,
      "size": 8})

    fixed_decimal = fixed_decimal_schema.parse()
    self.assertIsInstance(fixed_decimal, schema.FixedSchema)
    self.assertIsInstance(fixed_decimal, schema.DecimalLogicalSchema)

  def test_fixed_decimal_invalid_max_precision(self):
    # An 8 byte number can't represent every 19 digit number, so the logical
    # type is not applied.
    fixed_decimal_schema = ValidTestSchema({
      "type": "fixed",
      "logicalType": "decimal",
      "name": "TestDecimal",
      "precision": 19,
      "scale": 0,
      "size": 8})

    fixed_decimal = fixed_decimal_schema.parse()
    self.assertIsInstance(fixed_decimal, schema.FixedSchema)
    self.assertNotIsInstance(fixed_decimal, schema.DecimalLogicalSchema)

class SchemaParseTestCase(unittest.TestCase):
  """Enable generating parse test cases over all the valid and invalid example schema."""

  def __init__(self, test_schema):
    """Ignore the normal signature for unittest.TestCase because we are generating
    many test cases from this one class. This is safe as long as the autoloader
    ignores this class. The autoloader will ignore this class as long as it has
    no methods starting with `test_`.
    """
    super(SchemaParseTestCase, self).__init__(
        'parse_valid' if test_schema.valid else 'parse_invalid')
    self.test_schema = test_schema

  def parse_valid(self):
    """Parsing a valid schema should not error."""
    try:
      self.test_schema.parse()
    except (schema.AvroException, schema.SchemaParseException):
      self.fail("Valid schema failed to parse: {!s}".format(self.test_schema))

  def parse_invalid(self):
    """Parsing an invalid schema should error."""
    try:
      self.test_schema.parse()
    except (schema.AvroException, schema.SchemaParseException):
      pass
    else:
      self.fail("Invalid schema should not have parsed: {!s}".format(self.test_schema))

class RoundTripParseTestCase(unittest.TestCase):
  """Enable generating round-trip parse test cases over all the valid test schema."""

  def __init__(self, test_schema):
    """Ignore the normal signature for unittest.TestCase because we are generating
    many test cases from this one class. This is safe as long as the autoloader
    ignores this class. The autoloader will ignore this class as long as it has
    no methods starting with `test_`.
    """
    super(RoundTripParseTestCase, self).__init__('parse_round_trip')
    self.test_schema = test_schema

  def parse_round_trip(self):
    """The string of a Schema should be parseable to the same Schema."""
    parsed = self.test_schema.parse()
    round_trip = schema.parse(str(parsed))
    self.assertEqual(parsed, round_trip)

class DocAttributesTestCase(unittest.TestCase):
  """Enable generating document attribute test cases over all the document test schema."""

  def __init__(self, test_schema):
    """Ignore the normal signature for unittest.TestCase because we are generating
    many test cases from this one class. This is safe as long as the autoloader
    ignores this class. The autoloader will ignore this class as long as it has
    no methods starting with `test_`.
    """
    super(DocAttributesTestCase, self).__init__('check_doc_attributes')
    self.test_schema = test_schema

  def check_doc_attributes(self):
    """Documentation attributes should be preserved."""
    sch = self.test_schema.parse()
    self.assertIsNotNone(sch.doc, "Failed to preserve 'doc' in schema: {!s}".format(self.test_schema))
    if sch.type == 'record':
      for f in sch.fields:
        self.assertIsNotNone(f.doc, "Failed to preserve 'doc' in fields: {!s}".format(self.test_schema))


class OtherAttributesTestCase(unittest.TestCase):
  """Enable generating attribute test cases over all the other-prop test schema."""
  _type_map = {
    "cp_array": list,
    "cp_boolean": bool,
    "cp_float": float,
    "cp_int": int,
    "cp_null": type(None),
    "cp_object": dict,
    "cp_string": basestring,
  }

  def __init__(self, test_schema):
    """Ignore the normal signature for unittest.TestCase because we are generating
    many test cases from this one class. This is safe as long as the autoloader
    ignores this class. The autoloader will ignore this class as long as it has
    no methods starting with `test_`.
    """
    super(OtherAttributesTestCase, self).__init__('check_attributes')
    self.test_schema = test_schema

  def _check_props(self, props):
    for k, v in props.items():
      self.assertIsInstance(v, self._type_map[k])

  def check_attributes(self):
    """Other attributes and their types on a schema should be preserved."""
    sch = self.test_schema.parse()
    round_trip = schema.parse(str(sch))
    self.assertEqual(sch.other_props, round_trip.other_props,
                     "Properties were not preserved in a round-trip parse.")
    self._check_props(sch.other_props)
    if sch.type == "record":
      field_props = [f.other_props for f in sch.fields if f.other_props]
      self.assertEqual(len(field_props), len(sch.fields))
      for p in field_props:
        self._check_props(p)


def load_tests(loader, default_tests, pattern):
  """Generate test cases across many test schema."""
  suite = unittest.TestSuite()
  suite.addTests(loader.loadTestsFromTestCase(TestSchema))
  suite.addTests(SchemaParseTestCase(ex) for ex in EXAMPLES)
  suite.addTests(RoundTripParseTestCase(ex) for ex in VALID_EXAMPLES)
  suite.addTests(DocAttributesTestCase(ex) for ex in DOC_EXAMPLES)
  suite.addTests(OtherAttributesTestCase(ex) for ex in OTHER_PROP_EXAMPLES)
  return suite

if __name__ == '__main__':
  unittest.main()
