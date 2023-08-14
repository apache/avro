#!/usr/bin/env python3

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

"""Test the name parsing logic."""

import json
import unittest

import avro.errors
import avro.protocol
import avro.schema


class TestName(unittest.TestCase):
    """Test name parsing"""

    def test_name_is_none(self):
        """When a name is None its namespace is None."""
        self.assertIsNone(avro.schema.Name(None, None, None).fullname)
        self.assertIsNone(avro.schema.Name(None, None, None).space)

    def test_name_not_empty_string(self):
        """A name cannot be the empty string."""
        self.assertRaises(avro.errors.SchemaParseException, avro.schema.Name, "", None, None)

    def test_name_space_specified(self):
        """Space combines with a name to become the fullname."""
        # name and namespace specified
        fullname = avro.schema.Name("a", "o.a.h", None).fullname
        self.assertEqual(fullname, "o.a.h.a")

    def test_name_inlined_space(self):
        """Space inlined with name is correctly splitted out."""
        name = avro.schema.Name("o.a", None)
        self.assertEqual(name.fullname, "o.a")
        self.assertEqual(name.name, "a")
        self.assertEqual(name.space, "o")

        name = avro.schema.Name("o.a.h.a", None)
        self.assertEqual(name.fullname, "o.a.h.a")
        self.assertEqual(name.name, "a")
        self.assertEqual(name.space, "o.a.h")

    def test_fullname_space_specified(self):
        """When name contains dots, namespace should be ignored."""
        fullname = avro.schema.Name("a.b.c.d", "o.a.h", None).fullname
        self.assertEqual(fullname, "a.b.c.d")

    def test_name_default_specified(self):
        """Default space becomes the namespace when the namespace is None."""
        fullname = avro.schema.Name("a", None, "b.c.d").fullname
        self.assertEqual(fullname, "b.c.d.a")

    def test_fullname_default_specified(self):
        """When a name contains dots, default space should be ignored."""
        fullname = avro.schema.Name("a.b.c.d", None, "o.a.h").fullname
        self.assertEqual(fullname, "a.b.c.d")

    def test_fullname_space_default_specified(self):
        """When a name contains dots, namespace and default space should be ignored."""
        fullname = avro.schema.Name("a.b.c.d", "o.a.a", "o.a.h").fullname
        self.assertEqual(fullname, "a.b.c.d")

    def test_name_space_default_specified(self):
        """When name and space are specified, default space should be ignored."""
        fullname = avro.schema.Name("a", "o.a.a", "o.a.h").fullname
        self.assertEqual(fullname, "o.a.a.a")

    def test_equal_names(self):
        """Equality of names is defined on the fullname and is case-sensitive."""
        self.assertEqual(
            avro.schema.Name("a.b.c.d", None, None),
            avro.schema.Name("d", "a.b.c", None),
        )
        self.assertNotEqual(avro.schema.Name("C.d", None, None), avro.schema.Name("c.d", None, None))

    def test_invalid_name(self):
        """The name portion of a fullname, record field names, and enum symbols must:
        start with [A-Za-z_] and subsequently contain only [A-Za-z0-9_]"""
        self.assertRaises(
            avro.errors.InvalidName,
            avro.schema.Name,
            "an especially spacey cowboy",
            None,
            None,
        )
        self.assertRaises(
            avro.errors.InvalidName,
            avro.schema.Name,
            "99 problems but a name aint one",
            None,
            None,
        )
        # A name cannot start with dot.
        self.assertRaises(avro.errors.InvalidName, avro.schema.Name, ".a", None, None)
        self.assertRaises(avro.errors.InvalidName, avro.schema.Name, "o..a", None, None)
        self.assertRaises(avro.errors.InvalidName, avro.schema.Name, "a.", None, None)

    def test_null_namespace(self):
        """The empty string may be used as a namespace to indicate the null namespace."""
        name = avro.schema.Name("name", "", None)
        self.assertEqual(name.fullname, "name")
        self.assertIsNone(name.space)

    def test_disable_name_validation(self):
        """Test name validation disable."""
        # Test name class
        avro.schema.Name(name_attr="an especially spacey cowboy", space_attr=None, default_space=None, validate_name=False)
        avro.schema.Name(name_attr="cowboy", space_attr="an especially spacey ", default_space=None, validate_name=False)
        avro.schema.Name(name_attr="cowboy", space_attr=None, default_space="an especially spacey ", validate_name=False)
        avro.schema.Name(name_attr="name-space.with-dash.cowboy", space_attr=None, default_space=None, validate_name=False)
        avro.schema.Name(name_attr="cowboy", space_attr="name-space.with-dash", default_space=None, validate_name=False)

        # Test record schema
        cowboy_record_1 = avro.schema.RecordSchema(
            name="an especially spacey cowboy", namespace=None, fields=[{"name": "value", "type": "long"}], validate_names=False
        )
        cowboy_record_2 = avro.schema.RecordSchema(
            name="cowboy", namespace="an especially spacey ", fields=[{"name": "value", "type": "int"}], validate_names=False
        )

        # Test Names container class
        names = avro.schema.Names(default_namespace=None, validate_names=False)
        names.add_name(name_attr=cowboy_record_1.name, space_attr=cowboy_record_1.namespace, new_schema=cowboy_record_1)
        names.add_name(name_attr=cowboy_record_2.name, space_attr=cowboy_record_2.namespace, new_schema=cowboy_record_2)

        # Test fixed schema
        avro.schema.FixedSchema(name="an especially spacey cowboy", namespace=None, size=16, validate_names=False)
        avro.schema.FixedSchema(name="cowboy", namespace="an especially spacey", size=16, validate_names=False)

        # Test fixed decimal schema
        avro.schema.FixedDecimalSchema(name="an especially spacey cowboy", namespace=None, size=16, precision=2, validate_names=False)
        avro.schema.FixedDecimalSchema(name="cowboy", namespace="an especially spacey", size=16, precision=2, validate_names=False)

        # Test enum schema
        avro.schema.EnumSchema(name="an especially spacey cowboy", namespace=None, symbols=["A", "B"], validate_names=False)
        avro.schema.EnumSchema(name="cowboy", namespace="an especially spacey", symbols=["A", "B"], validate_names=False)


EXAMPLES = [
    # Enum
    {"type": "enum", "name": "invalid-name", "symbols": ["A", "B"]},
    {"type": "enum", "name": "invalid-ns.ab", "symbols": ["A", "B"]},
    {"type": "enum", "name": "ab", "namespace": "invalid-ns", "symbols": ["A", "B"]},
    # Record
    {"type": "record", "name": "invalid-name", "fields": [{"name": "distance", "type": "long"}]},
    {"type": "record", "name": "invalid-ns.journey", "fields": [{"name": "distance", "type": "long"}]},
    {"type": "record", "name": "journey", "namespace": "invalid-ns", "fields": [{"name": "distance", "type": "long"}]},
    # FixedSchema
    {"type": "fixed", "name": "invalid-name", "size": 10, "precision": 2},
    {"type": "fixed", "name": "invalid-ns.irrational", "size": 10, "precision": 2},
    {"type": "fixed", "name": "irrational", "namespace": "invalid-ns", "size": 10, "precision": 2},
    # FixedDecimalSchema / logical type
    {"type": "fixed", "logicalType": "decimal", "name": "invalid-name", "size": 10, "precision": 2},
    {"type": "fixed", "logicalType": "decimal", "name": "invalid-ns.irrational", "size": 10, "precision": 2},
    {"type": "fixed", "logicalType": "decimal", "name": "irrational", "namespace": "invalid-ns", "size": 10, "precision": 2},
    # In fields
    {
        "type": "record",
        "name": "world",
        "fields": [
            {
                "type": {"type": "record", "name": "invalid-name", "fields": [{"name": "distance", "type": "long"}]},
                "name": "cup",
            },
        ],
    },
    # In union
    [{"type": "string"}, {"type": "record", "name": "invalid-name", "fields": [{"name": "distance", "type": "long"}]}],
    # In array
    {
        "type": "record",
        "name": "world",
        "fields": [
            {
                "name": "journeys",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "invalid-name",
                        "fields": [{"name": "distance", "type": "long"}],
                    },
                },
            },
        ],
    },
    # In map
    {
        "type": "record",
        "name": "world",
        "fields": [
            {
                "name": "journeys",
                "type": {
                    "type": "map",
                    "values": {
                        "type": "record",
                        "name": "invalid-name",
                        "fields": [{"name": "distance", "type": "long"}],
                    },
                },
            },
        ],
    },
]


class ParseSchemaNameValidationDisabledTestCase(unittest.TestCase):
    """Enable generating parse test cases over all the valid and invalid example schema."""

    def __init__(self, test_schema_string):
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("parse_invalid_name")
        self.test_schema_string = test_schema_string

    def parse_invalid_name(self) -> None:
        """Parsing a schema with invalid name should not error"""
        schema_string = json.dumps(self.test_schema_string)
        # Parse with validation to ensure that correct exception is raised when validation enabled.
        with self.assertRaises(
            (avro.errors.AvroException, avro.errors.SchemaParseException), msg=f"Invalid schema should not have parsed: {self.test_schema_string!s}"
        ):
            avro.schema.parse(schema_string, validate_names=True)

        # The actual test with validation disabled.
        avro.schema.parse(schema_string, validate_names=False)


PROTOCOL_EXAMPLES = [
    # In record
    {
        "namespace": "lightyear",
        "protocol": "lightspeed",
        "types": [
            {"name": "current-speed", "type": "record", "fields": [{"name": "speed", "type": "int"}, {"name": "unit", "type": "string"}]},
            {"name": "over_c", "type": "error", "fields": [{"name": "message", "type": "string"}]},
        ],
        "messages": {
            "speedmessage": {"request": [{"name": "current_speed", "type": "current-speed"}], "response": "current-speed", "errors": ["over_c"]}
        },
    },
    # Error union
    {
        "namespace": "lightyear",
        "protocol": "lightspeed",
        "types": [
            {"name": "current_speed", "type": "record", "fields": [{"name": "speed", "type": "int"}, {"name": "unit", "type": "string"}]},
            {"name": "over-c", "type": "error", "fields": [{"name": "message", "type": "string"}]},
        ],
        "messages": {
            "speedmessage": {"request": [{"name": "current_speed", "type": "current_speed"}], "response": "current_speed", "errors": ["over-c"]}
        },
    },
    {
        "namespace": "lightyear",
        "protocol": "lightspeed",
        "types": [
            {"name": "current_speed", "type": "record", "fields": [{"name": "speed", "type": "int"}, {"name": "unit", "type": "string"}]},
            {"name": "over_c", "namespace": "error-speed", "type": "error", "fields": [{"name": "message", "type": "string"}]},
        ],
        "messages": {
            "speedmessage": {
                "request": [{"name": "current_speed", "type": "current_speed"}],
                "response": "current_speed",
                "errors": ["error-speed.over_c"],
            }
        },
    },
]


class ParseProtocolNameValidationDisabledTestCase(unittest.TestCase):
    """Test parsing of protocol when name validation is disabled."""

    def __init__(self, test_protocol_string):
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("parse_protocol_with_invalid_name")
        self.test_protocol_string = test_protocol_string

    def parse_protocol_with_invalid_name(self):
        """Test error union"""
        protocol = json.dumps(self.test_protocol_string)

        with self.assertRaises(
            (avro.errors.AvroException, avro.errors.SchemaParseException), msg=f"Invalid schema should not have parsed: {self.test_protocol_string!s}"
        ):
            avro.protocol.parse(protocol, validate_names=True)

        avro.protocol.parse(protocol, validate_names=False)


def load_tests(loader, default_tests, pattern):
    """Generate test cases across many test schema."""
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestName))
    suite.addTests(ParseSchemaNameValidationDisabledTestCase(ex) for ex in EXAMPLES)
    suite.addTests(ParseProtocolNameValidationDisabledTestCase(ex) for ex in PROTOCOL_EXAMPLES)
    return suite


if __name__ == "__main__":  # pragma: no coverage
    unittest.main()
