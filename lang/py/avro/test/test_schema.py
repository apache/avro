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

"""Test the schema parsing logic."""

import json
import unittest
import warnings
from typing import List

import avro.errors
import avro.schema


class TestSchema:
    """A proxy for a schema string that provides useful test metadata."""

    def __init__(self, data, name="", comment="", warnings=None):
        if not isinstance(data, str):
            data = json.dumps(data)
        self.data = data
        self.name = name or data  # default to data for name
        self.comment = comment
        self.warnings = warnings

    def parse(self):
        return avro.schema.parse(str(self))

    def __str__(self):
        return str(self.data)


class ValidTestSchema(TestSchema):
    """A proxy for a valid schema string that provides useful test metadata."""

    valid = True


class InvalidTestSchema(TestSchema):
    """A proxy for an invalid schema string that provides useful test metadata."""

    valid = False


PRIMITIVE_EXAMPLES: List[TestSchema] = [InvalidTestSchema('"True"')]
PRIMITIVE_EXAMPLES.append(InvalidTestSchema("True"))
PRIMITIVE_EXAMPLES.append(InvalidTestSchema('{"no_type": "test"}'))
PRIMITIVE_EXAMPLES.append(InvalidTestSchema('{"type": "panther"}'))
PRIMITIVE_EXAMPLES.extend([ValidTestSchema(f'"{t}"') for t in avro.constants.PRIMITIVE_TYPES])
PRIMITIVE_EXAMPLES.extend([ValidTestSchema({"type": t}) for t in avro.constants.PRIMITIVE_TYPES])

FIXED_EXAMPLES = [
    ValidTestSchema({"type": "fixed", "name": "Test", "size": 1}),
    ValidTestSchema(
        {
            "type": "fixed",
            "name": "MyFixed",
            "size": 1,
            "namespace": "org.apache.hadoop.avro",
        }
    ),
    ValidTestSchema({"type": "fixed", "name": "NullNamespace", "namespace": None, "size": 1}),
    ValidTestSchema({"type": "fixed", "name": "EmptyStringNamespace", "namespace": "", "size": 1}),
    InvalidTestSchema({"type": "fixed", "name": "Missing size"}),
    InvalidTestSchema({"type": "fixed", "size": 314}),
    InvalidTestSchema({"type": "fixed", "size": 314, "name": "dr. spaceman"}, comment="AVRO-621"),
]

ENUM_EXAMPLES = [
    ValidTestSchema({"type": "enum", "name": "Test", "symbols": ["A", "B"]}),
    ValidTestSchema({"type": "enum", "name": "AVRO2174", "symbols": ["nowhitespace"]}),
    InvalidTestSchema({"type": "enum", "name": "bad_default", "symbols": ["A"], "default": "B"}, comment="AVRO-3229"),
    InvalidTestSchema({"type": "enum", "name": "Status", "symbols": "Normal Caution Critical"}),
    InvalidTestSchema({"type": "enum", "name": [0, 1, 1, 2, 3, 5, 8], "symbols": ["Golden", "Mean"]}),
    InvalidTestSchema({"type": "enum", "symbols": ["I", "will", "fail", "no", "name"]}),
    InvalidTestSchema({"type": "enum", "name": "Test", "symbols": ["AA", "AA"]}),
    InvalidTestSchema({"type": "enum", "name": "AVRO2174", "symbols": ["white space"]}),
]

ARRAY_EXAMPLES = [
    ValidTestSchema({"type": "array", "items": "long"}),
    ValidTestSchema(
        {
            "type": "array",
            "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]},
        }
    ),
]

MAP_EXAMPLES = [
    ValidTestSchema({"type": "map", "values": "long"}),
    ValidTestSchema(
        {
            "type": "map",
            "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]},
        }
    ),
]

UNION_EXAMPLES = [
    ValidTestSchema(["string", "null", "long"]),
    InvalidTestSchema(["null", "null"]),
    InvalidTestSchema(["long", "long"]),
    InvalidTestSchema([{"type": "array", "items": "long"}, {"type": "array", "items": "string"}]),
]

NAME_EXAMPLES = [
    ValidTestSchema({"type": "enum", "name": "record", "symbols": ["A", "B"]}),
    ValidTestSchema({"type": "record", "name": "record", "fields": [{"name": "f", "type": "long"}]}),
    InvalidTestSchema({"type": "enum", "name": "int", "symbols": ["A", "B"]}),
    ValidTestSchema({"type": "enum", "name": "ns.int", "symbols": ["A", "B"]}),
    ValidTestSchema({"type": "enum", "namespace": "ns", "name": "int", "symbols": ["A", "B"]}),
    ValidTestSchema(
        {"type": "record", "name": "LinkedList", "fields": [{"name": "value", "type": "int"}, {"name": "next", "type": ["null", "LinkedList"]}]}
    ),
    ValidTestSchema({"type": "record", "name": "record", "fields": [{"name": "value", "type": "int"}, {"name": "next", "type": ["null", "record"]}]}),
    ValidTestSchema({"type": "record", "name": "ns.int", "fields": [{"name": "value", "type": "int"}, {"name": "next", "type": ["null", "ns.int"]}]}),
]

NAMED_IN_UNION_EXAMPLES = [
    ValidTestSchema(
        {
            "namespace": "org.apache.avro.test",
            "type": "record",
            "name": "Test",
            "fields": [
                {
                    "type": {
                        "symbols": ["one", "two"],
                        "type": "enum",
                        "name": "NamedEnum",
                    },
                    "name": "thenamedenum",
                },
                {"type": ["null", "NamedEnum"], "name": "unionwithreftoenum"},
            ],
        }
    )
]

RECORD_EXAMPLES = [
    ValidTestSchema({"type": "record", "name": "Test", "fields": [{"name": "f", "type": "long"}]}),
    ValidTestSchema({"type": "error", "name": "Test", "fields": [{"name": "f", "type": "long"}]}),
    ValidTestSchema(
        {
            "type": "record",
            "name": "Node",
            "fields": [
                {"name": "label", "type": "string"},
                {"name": "children", "type": {"type": "array", "items": "Node"}},
            ],
        }
    ),
    ValidTestSchema(
        {
            "type": "record",
            "name": "Lisp",
            "fields": [
                {
                    "name": "value",
                    "type": [
                        "null",
                        "string",
                        {
                            "type": "record",
                            "name": "Cons",
                            "fields": [
                                {"name": "car", "type": "Lisp"},
                                {"name": "cdr", "type": "Lisp"},
                            ],
                        },
                    ],
                }
            ],
        }
    ),
    ValidTestSchema(
        {
            "type": "record",
            "name": "HandshakeRequest",
            "namespace": "org.apache.avro.ipc",
            "fields": [
                {
                    "name": "clientHash",
                    "type": {"type": "fixed", "name": "MD5", "size": 16},
                },
                {"name": "clientProtocol", "type": ["null", "string"]},
                {"name": "serverHash", "type": "MD5"},
                {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]},
            ],
        }
    ),
    ValidTestSchema(
        {
            "type": "record",
            "name": "HandshakeResponse",
            "namespace": "org.apache.avro.ipc",
            "fields": [
                {
                    "name": "match",
                    "type": {
                        "type": "enum",
                        "name": "HandshakeMatch",
                        "symbols": ["BOTH", "CLIENT", "NONE"],
                    },
                },
                {"name": "serverProtocol", "type": ["null", "string"]},
                {
                    "name": "serverHash",
                    "type": ["null", {"name": "MD5", "size": 16, "type": "fixed"}],
                },
                {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]},
            ],
        }
    ),
    ValidTestSchema(
        {
            "type": "record",
            "name": "Interop",
            "namespace": "org.apache.avro",
            "fields": [
                {"name": "intField", "type": "int"},
                {"name": "longField", "type": "long"},
                {"name": "stringField", "type": "string"},
                {"name": "boolField", "type": "boolean"},
                {"name": "floatField", "type": "float"},
                {"name": "doubleField", "type": "double"},
                {"name": "bytesField", "type": "bytes"},
                {"name": "nullField", "type": "null"},
                {"name": "arrayField", "type": {"type": "array", "items": "double"}},
                {
                    "name": "mapField",
                    "type": {
                        "type": "map",
                        "values": {
                            "name": "Foo",
                            "type": "record",
                            "fields": [{"name": "label", "type": "string"}],
                        },
                    },
                },
                {
                    "name": "unionField",
                    "type": ["boolean", "double", {"type": "array", "items": "bytes"}],
                },
                {
                    "name": "enumField",
                    "type": {
                        "type": "enum",
                        "name": "Kind",
                        "symbols": ["A", "B", "C"],
                    },
                },
                {
                    "name": "fixedField",
                    "type": {"type": "fixed", "name": "MD5", "size": 16},
                },
                {
                    "name": "recordField",
                    "type": {
                        "type": "record",
                        "name": "Node",
                        "fields": [
                            {"name": "label", "type": "string"},
                            {
                                "name": "children",
                                "type": {"type": "array", "items": "Node"},
                            },
                        ],
                    },
                },
            ],
        }
    ),
    ValidTestSchema(
        {
            "type": "record",
            "name": "ipAddr",
            "fields": [
                {
                    "name": "addr",
                    "type": [
                        {"name": "IPv6", "type": "fixed", "size": 16},
                        {"name": "IPv4", "type": "fixed", "size": 4},
                    ],
                }
            ],
        }
    ),
    InvalidTestSchema(
        {
            "type": "record",
            "name": "Address",
            "fields": [{"type": "string"}, {"type": "string", "name": "City"}],
        }
    ),
    InvalidTestSchema(
        {
            "type": "record",
            "name": "Event",
            "fields": [{"name": "Sponsor"}, {"name": "City", "type": "string"}],
        }
    ),
    InvalidTestSchema(
        {
            "type": "record",
            "name": "Rainer",
            "fields": "His vision, from the constantly passing bars",
        }
    ),
    InvalidTestSchema(
        {
            "name": ["Tom", "Jerry"],
            "type": "record",
            "fields": [{"name": "name", "type": "string"}],
        }
    ),
]

DOC_EXAMPLES = [
    ValidTestSchema(
        {
            "type": "record",
            "name": "TestDoc",
            "doc": "Doc string",
            "fields": [{"name": "name", "type": "string", "doc": "Doc String"}],
        }
    ),
    ValidTestSchema({"type": "enum", "name": "Test", "symbols": ["A", "B"], "doc": "Doc String"}),
]

OTHER_PROP_EXAMPLES = [
    ValidTestSchema(
        {
            "type": "record",
            "name": "TestRecord",
            "cp_string": "string",
            "cp_int": 1,
            "cp_array": [1, 2, 3, 4],
            "fields": [
                {"name": "f1", "type": "string", "cp_object": {"a": 1, "b": 2}},
                {"name": "f2", "type": "long", "cp_null": None},
            ],
        }
    ),
    ValidTestSchema({"type": "map", "values": "long", "cp_boolean": True}),
    ValidTestSchema(
        {
            "type": "enum",
            "name": "TestEnum",
            "symbols": ["one", "two", "three"],
            "cp_float": 1.0,
        }
    ),
]

DECIMAL_LOGICAL_TYPE = [
    ValidTestSchema(
        {
            "type": "fixed",
            "logicalType": "decimal",
            "name": "TestDecimal",
            "precision": 4,
            "size": 10,
            "scale": 2,
        }
    ),
    ValidTestSchema({"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}),
    InvalidTestSchema(
        {
            "type": "fixed",
            "logicalType": "decimal",
            "name": "TestDecimal2",
            "precision": 2,
            "scale": 2,
            "size": -2,
        }
    ),
]

DATE_LOGICAL_TYPE = [ValidTestSchema({"type": "int", "logicalType": "date"})]

TIMEMILLIS_LOGICAL_TYPE = [ValidTestSchema({"type": "int", "logicalType": "time-millis"})]

TIMEMICROS_LOGICAL_TYPE = [ValidTestSchema({"type": "long", "logicalType": "time-micros"})]

TIMESTAMPMILLIS_LOGICAL_TYPE = [ValidTestSchema({"type": "long", "logicalType": "timestamp-millis"})]

TIMESTAMPMICROS_LOGICAL_TYPE = [ValidTestSchema({"type": "long", "logicalType": "timestamp-micros"})]

UUID_LOGICAL_TYPE = [ValidTestSchema({"type": "string", "logicalType": "uuid"})]

IGNORED_LOGICAL_TYPE = [
    ValidTestSchema(
        {"type": "string", "logicalType": "unknown-logical-type"},
        warnings=[avro.errors.IgnoredLogicalType("Unknown unknown-logical-type, using string.")],
    ),
    ValidTestSchema(
        {"type": "bytes", "logicalType": "decimal", "scale": 0},
        warnings=[avro.errors.IgnoredLogicalType("Invalid decimal precision None. Must be a positive integer.")],
    ),
    ValidTestSchema(
        {"type": "bytes", "logicalType": "decimal", "precision": 2.4, "scale": 0},
        warnings=[avro.errors.IgnoredLogicalType("Invalid decimal precision 2.4. Must be a positive integer.")],
    ),
    ValidTestSchema(
        {"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": -2},
        warnings=[avro.errors.IgnoredLogicalType("Invalid decimal scale -2. Must be a non-negative integer.")],
    ),
    ValidTestSchema(
        {"type": "bytes", "logicalType": "decimal", "precision": -2, "scale": 2},
        warnings=[avro.errors.IgnoredLogicalType("Invalid decimal precision -2. Must be a positive integer.")],
    ),
    ValidTestSchema(
        {"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": 3},
        warnings=[avro.errors.IgnoredLogicalType("Invalid decimal scale 3. Cannot be greater than precision 2.")],
    ),
    ValidTestSchema(
        {
            "type": "fixed",
            "logicalType": "decimal",
            "name": "TestIgnored",
            "precision": -10,
            "scale": 2,
            "size": 5,
        },
        warnings=[avro.errors.IgnoredLogicalType("Invalid decimal precision -10. Must be a positive integer.")],
    ),
    ValidTestSchema(
        {
            "type": "fixed",
            "logicalType": "decimal",
            "name": "TestIgnored",
            "scale": 2,
            "size": 5,
        },
        warnings=[avro.errors.IgnoredLogicalType("Invalid decimal precision None. Must be a positive integer.")],
    ),
    ValidTestSchema(
        {
            "type": "fixed",
            "logicalType": "decimal",
            "name": "TestIgnored",
            "precision": 2,
            "scale": 3,
            "size": 2,
        },
        warnings=[avro.errors.IgnoredLogicalType("Invalid decimal scale 3. Cannot be greater than precision 2.")],
    ),
    ValidTestSchema(
        {
            "type": "fixed",
            "logicalType": "decimal",
            "name": "TestIgnored",
            "precision": 311,
            "size": 129,
        },
        warnings=[avro.errors.IgnoredLogicalType("Invalid decimal precision 311. Max is 310.")],
    ),
    ValidTestSchema(
        {"type": "float", "logicalType": "decimal", "precision": 2, "scale": 0},
        warnings=[avro.errors.IgnoredLogicalType("Logical type decimal requires literal type bytes/fixed, not float.")],
    ),
    ValidTestSchema(
        {"type": "int", "logicalType": "date1"},
        warnings=[avro.errors.IgnoredLogicalType("Unknown date1, using int.")],
    ),
    ValidTestSchema(
        {"type": "long", "logicalType": "date"},
        warnings=[avro.errors.IgnoredLogicalType("Logical type date requires literal type int, not long.")],
    ),
    ValidTestSchema(
        {"type": "int", "logicalType": "time-milis"},
        warnings=[avro.errors.IgnoredLogicalType("Unknown time-milis, using int.")],
    ),
    ValidTestSchema(
        {"type": "long", "logicalType": "time-millis"},
        warnings=[avro.errors.IgnoredLogicalType("Logical type time-millis requires literal type int, not long.")],
    ),
    ValidTestSchema(
        {"type": "long", "logicalType": "time-micro"},
        warnings=[avro.errors.IgnoredLogicalType("Unknown time-micro, using long.")],
    ),
    ValidTestSchema(
        {"type": "int", "logicalType": "time-micros"},
        warnings=[avro.errors.IgnoredLogicalType("Logical type time-micros requires literal type long, not int.")],
    ),
    ValidTestSchema(
        {"type": "long", "logicalType": "timestamp-milis"},
        warnings=[avro.errors.IgnoredLogicalType("Unknown timestamp-milis, using long.")],
    ),
    ValidTestSchema(
        {"type": "int", "logicalType": "timestamp-millis"},
        warnings=[avro.errors.IgnoredLogicalType("Logical type timestamp-millis requires literal type long, not int.")],
    ),
    ValidTestSchema(
        {"type": "long", "logicalType": "timestamp-micro"},
        warnings=[avro.errors.IgnoredLogicalType("Unknown timestamp-micro, using long.")],
    ),
    ValidTestSchema(
        {"type": "int", "logicalType": "timestamp-micros"},
        warnings=[avro.errors.IgnoredLogicalType("Logical type timestamp-micros requires literal type long, not int.")],
    ),
]


# Fingerprint examples are in the form of tuples:
# - Value in Position 0 is schema
# - Value in Position 1 is an array of fingerprints:
#     - Position 0 is CRC-64-AVRO fingerprint
#     - Position 0 is MD5 fingerprint
#     - Position 0 is SHA256 fingerprint
FINGERPRINT_EXAMPLES = [
    ('"int"', ["8f5c393f1ad57572", "ef524ea1b91e73173d938ade36c1db32", "3f2b87a9fe7cc9b13835598c3981cd45e3e355309e5090aa0933d7becb6fba45"]),
    ('{"type": "int"}', ["8f5c393f1ad57572", "ef524ea1b91e73173d938ade36c1db32", "3f2b87a9fe7cc9b13835598c3981cd45e3e355309e5090aa0933d7becb6fba45"]),
    ('"float"', ["90d7a83ecb027c4d", "50a6b9db85da367a6d2df400a41758a6", "1e71f9ec051d663f56b0d8e1fc84d71aa56ccfe9fa93aa20d10547a7abeb5cc0"]),
    (
        '{"type": "float"}',
        ["90d7a83ecb027c4d", "50a6b9db85da367a6d2df400a41758a6", "1e71f9ec051d663f56b0d8e1fc84d71aa56ccfe9fa93aa20d10547a7abeb5cc0"],
    ),
    ('"long"', ["b71df49344e154d0", "e1dd9a1ef98b451b53690370b393966b", "c32c497df6730c97fa07362aa5023f37d49a027ec452360778114cf427965add"]),
    (
        '{"type": "long"}',
        ["b71df49344e154d0", "e1dd9a1ef98b451b53690370b393966b", "c32c497df6730c97fa07362aa5023f37d49a027ec452360778114cf427965add"],
    ),
    ('"double"', ["7e95ab32c035758e", "bfc71a62f38b99d6a93690deeb4b3af6", "730a9a8c611681d7eef442e03c16c70d13bca3eb8b977bb403eaff52176af254"]),
    (
        '{"type": "double"}',
        ["7e95ab32c035758e", "bfc71a62f38b99d6a93690deeb4b3af6", "730a9a8c611681d7eef442e03c16c70d13bca3eb8b977bb403eaff52176af254"],
    ),
    ('"bytes"', ["651920c3da16c04f", "b462f06cb909be57c85008867784cde6", "9ae507a9dd39ee5b7c7e285da2c0846521c8ae8d80feeae5504e0c981d53f5fa"]),
    (
        '{"type": "bytes"}',
        ["651920c3da16c04f", "b462f06cb909be57c85008867784cde6", "9ae507a9dd39ee5b7c7e285da2c0846521c8ae8d80feeae5504e0c981d53f5fa"],
    ),
    ('"string"', ["c70345637248018f", "095d71cf12556b9d5e330ad575b3df5d", "e9e5c1c9e4f6277339d1bcde0733a59bd42f8731f449da6dc13010a916930d48"]),
    (
        '{"type": "string"}',
        ["c70345637248018f", "095d71cf12556b9d5e330ad575b3df5d", "e9e5c1c9e4f6277339d1bcde0733a59bd42f8731f449da6dc13010a916930d48"],
    ),
    ('"boolean"', ["64f7d4a478fc429f", "01f692b30d4a1c8a3e600b1440637f8f", "a5b031ab62bc416d720c0410d802ea46b910c4fbe85c50a946ccc658b74e677e"]),
    (
        '{"type": "boolean"}',
        ["64f7d4a478fc429f", "01f692b30d4a1c8a3e600b1440637f8f", "a5b031ab62bc416d720c0410d802ea46b910c4fbe85c50a946ccc658b74e677e"],
    ),
    ('"null"', ["8a8f25cce724dd63", "9b41ef67651c18488a8b08bb67c75699", "f072cbec3bf8841871d4284230c5e983dc211a56837aed862487148f947d1a1f"]),
    (
        '{"type": "null"}',
        ["8a8f25cce724dd63", "9b41ef67651c18488a8b08bb67c75699", "f072cbec3bf8841871d4284230c5e983dc211a56837aed862487148f947d1a1f"],
    ),
    (
        '{"type": "fixed", "name": "Test", "size": 1}',
        ["6869897b4049355b", "db01bc515fcfcd2d4be82ed385288261", "f527116a6f44455697e935afc31dc60ad0f95caf35e1d9c9db62edb3ffeb9170"],
    ),
    (
        json.dumps({"type": "fixed", "name": "MyFixed", "namespace": "org.apache.hadoop.avro", "size": 1}),
        ["fadbd138e85bdf45", "d74b3726484422711c465d49e857b1ba", "28e493a44771cecc5deca4bd938cdc3d5a24cfe1f3760bc938fa1057df6334fc"],
    ),
    (
        '{"type": "enum", "name": "Test", "symbols": ["A", "B"]}',
        ["03a2f2c2e27f7a16", "d883f2a9b16ed085fcc5e4ca6c8f6ed1", "9b51286144f87ce5aebdc61ca834379effa5a41ce6ac0938630ff246297caca8"],
    ),
    (
        '{"type": "array", "items": "long"}',
        ["715e2ea28bc91654", "c1c387e8d6a58f0df749b698991b1f43", "f78e954167feb23dcb1ce01e8463cebf3408e0a4259e16f24bd38f6d0f1d578b"],
    ),
    (
        json.dumps({"type": "array", "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}),
        ["10d9ade1fa3a0387", "cfc7b861c7cfef082a6ef082948893fa", "0d8edd49d7f7e9553668f133577bc99f842852b55d9f84f1f7511e4961aa685c"],
    ),
    (
        '{"type": "map", "values": "long"}',
        ["6f74f4e409b1334e", "32b3f1a3177a0e73017920f00448b56e", "b8fad07d458971a07692206b8a7cf626c86c62fe6bcff7c1b11bc7295de34853"],
    ),
    (
        json.dumps({"type": "map", "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}),
        ["df2ab0626f6b812d", "c588da6ba99701c41e73fd30d23f994e", "3886747ed1669a8af476b549e97b34222afb2fed5f18bb27c6f367ea0351a576"],
    ),
    (
        '["string", "null", "long"]',
        ["65a5be410d687566", "b11cf95f0a55dd55f9ee515a37bf937a", "ed8d254116441bb35e237ad0563cf5432b8c975334bd222c1ee84609435d95bb"],
    ),
    (
        json.dumps({"type": "record", "name": "Test", "fields": [{"name": "f", "type": "long"}]}),
        ["ed94e5f5e6eb588e", "69531a03db788afe353244cd049b1e6d", "9670f15a8f96d23e92830d00b8bd57275e02e3e173ffef7c253c170b6beabeb8"],
    ),
    (
        json.dumps(
            {
                "type": "record",
                "name": "Node",
                "fields": [{"name": "label", "type": "string"}, {"name": "children", "type": {"type": "array", "items": "Node"}}],
            }
        ),
        ["52cba544c3e756b7", "99625b0cc02050363e89ef66b0f406c9", "65d80dc8c95c98a9671d92cf0415edfabfee2cb058df2138606656cd6ae4dc59"],
    ),
    (
        json.dumps(
            {
                "type": "record",
                "name": "Lisp",
                "fields": [
                    {
                        "name": "value",
                        "type": [
                            "null",
                            "string",
                            {"type": "record", "name": "Cons", "fields": [{"name": "car", "type": "Lisp"}, {"name": "cdr", "type": "Lisp"}]},
                        ],
                    }
                ],
            }
        ),
        ["68d91a23eda0b306", "9e1d0d15b52789fcb8e3a88b53059d5f", "e5ce4f4a15ce19fa1047cfe16a3b0e13a755db40f00f23284fdd376fc1c7dd21"],
    ),
    (
        json.dumps(
            {
                "type": "record",
                "name": "HandshakeRequest",
                "namespace": "org.apache.avro.ipc",
                "fields": [
                    {"name": "clientHash", "type": {"type": "fixed", "name": "MD5", "size": 16}},
                    {"name": "clientProtocol", "type": ["null", "string"]},
                    {"name": "serverHash", "type": "MD5"},
                    {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]},
                ],
            }
        ),
        ["43818703b7b5d769", "16ded8b5027e80a17704c6565c0c3f1b", "6c317314687da52a85c813a7f0c92298a60b79625b9acc072e4d9e4256a1d800"],
    ),
    (
        json.dumps(
            {
                "type": "record",
                "name": "HandshakeResponse",
                "namespace": "org.apache.avro.ipc",
                "fields": [
                    {"name": "match", "type": {"type": "enum", "name": "HandshakeMatch", "symbols": ["BOTH", "CLIENT", "NONE"]}},
                    {"name": "serverProtocol", "type": ["null", "string"]},
                    {"name": "serverHash", "type": ["null", {"name": "MD5", "size": 16, "type": "fixed"}]},
                    {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]},
                ],
            }
        ),
        ["00feee01de4ea50e", "afe529d01132daab7f4e2a6663e7a2f5", "a303cbbfe13958f880605d70c521a4b7be34d9265ac5a848f25916a67b11d889"],
    ),
    (
        json.dumps(
            {
                "type": "record",
                "name": "Interop",
                "namespace": "org.apache.avro",
                "fields": [
                    {"name": "intField", "type": "int"},
                    {"name": "longField", "type": "long"},
                    {"name": "stringField", "type": "string"},
                    {"name": "boolField", "type": "boolean"},
                    {"name": "floatField", "type": "float"},
                    {"name": "doubleField", "type": "double"},
                    {"name": "bytesField", "type": "bytes"},
                    {"name": "nullField", "type": "null"},
                    {"name": "arrayField", "type": {"type": "array", "items": "double"}},
                    {
                        "name": "mapField",
                        "type": {"type": "map", "values": {"name": "Foo", "type": "record", "fields": [{"name": "label", "type": "string"}]}},
                    },
                    {"name": "unionField", "type": ["boolean", "double", {"type": "array", "items": "bytes"}]},
                    {"name": "enumField", "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B", "C"]}},
                    {"name": "fixedField", "type": {"type": "fixed", "name": "MD5", "size": 16}},
                    {
                        "name": "recordField",
                        "type": {
                            "type": "record",
                            "name": "Node",
                            "fields": [{"name": "label", "type": "string"}, {"name": "children", "type": {"type": "array", "items": "Node"}}],
                        },
                    },
                ],
            }
        ),
        ["e82c0a93a6a0b5a4", "994fea1a1be7ff8603cbe40c3bc7e4ca", "cccfd6e3f917cf53b0f90c206342e6703b0d905071f724a1c1f85b731c74058d"],
    ),
    (
        json.dumps(
            {
                "type": "record",
                "name": "ipAddr",
                "fields": [{"name": "addr", "type": [{"name": "IPv6", "type": "fixed", "size": 16}, {"name": "IPv4", "type": "fixed", "size": 4}]}],
            }
        ),
        ["8d961b4e298a1844", "45d85c69b353a99b93d7c4f2fcf0c30d", "6f6fc8f685a4f07d99734946565d63108806d55a8620febea047cf52cb0ac181"],
    ),
    (
        json.dumps({"type": "record", "name": "TestDoc", "doc": "Doc string", "fields": [{"name": "name", "type": "string", "doc": "Doc String"}]}),
        ["0e6660f02bcdc109", "f2da75f5131f5ab80629538287b8beb2", "0b3644f7aa5ca2fc4bad93ca2d3609c12aa9dbda9c15e68b34c120beff08e7b9"],
    ),
    (
        '{"type": "enum", "name": "Test", "symbols": ["A", "B"], "doc": "Doc String"}',
        ["03a2f2c2e27f7a16", "d883f2a9b16ed085fcc5e4ca6c8f6ed1", "9b51286144f87ce5aebdc61ca834379effa5a41ce6ac0938630ff246297caca8"],
    ),
]

EXAMPLES = PRIMITIVE_EXAMPLES
EXAMPLES += FIXED_EXAMPLES
EXAMPLES += ENUM_EXAMPLES
EXAMPLES += ARRAY_EXAMPLES
EXAMPLES += MAP_EXAMPLES
EXAMPLES += UNION_EXAMPLES
EXAMPLES += NAME_EXAMPLES
EXAMPLES += NAMED_IN_UNION_EXAMPLES
EXAMPLES += RECORD_EXAMPLES
EXAMPLES += DOC_EXAMPLES
EXAMPLES += DECIMAL_LOGICAL_TYPE
EXAMPLES += DATE_LOGICAL_TYPE
EXAMPLES += TIMEMILLIS_LOGICAL_TYPE
EXAMPLES += TIMEMICROS_LOGICAL_TYPE
EXAMPLES += TIMESTAMPMILLIS_LOGICAL_TYPE
EXAMPLES += TIMESTAMPMICROS_LOGICAL_TYPE
EXAMPLES += UUID_LOGICAL_TYPE
EXAMPLES += IGNORED_LOGICAL_TYPE

VALID_EXAMPLES = [e for e in EXAMPLES if getattr(e, "valid", False)]
INVALID_EXAMPLES = [e for e in EXAMPLES if not getattr(e, "valid", True)]


class TestMisc(unittest.TestCase):
    """Miscellaneous tests for schema"""

    def test_correct_recursive_extraction(self):
        """A recursive reference within a schema should be the same type every time."""
        s = avro.schema.parse(
            """{
            "type": "record",
            "name": "X",
            "fields": [{
                "name": "y",
                "type": {
                    "type": "record",
                    "name": "Y",
                    "fields": [{"name": "Z", "type": "X"}]}
            }]
        }"""
        )
        t = avro.schema.parse(str(s.fields[0].type))
        # If we've made it this far, the subschema was reasonably stringified; it ccould be reparsed.
        self.assertEqual("X", t.fields[0].type.name)

    def test_exception_is_not_swallowed_on_parse_error(self):
        """A specific exception message should appear on a json parse error."""
        self.assertRaisesRegex(
            avro.errors.SchemaParseException,
            r"Error parsing JSON: /not/a/real/file",
            avro.schema.parse,
            "/not/a/real/file",
        )

    def test_decimal_valid_type(self):
        fixed_decimal_schema = ValidTestSchema(
            {
                "type": "fixed",
                "logicalType": "decimal",
                "name": "TestDecimal",
                "precision": 4,
                "scale": 2,
                "size": 2,
            }
        )

        fixed_decimal = fixed_decimal_schema.parse()
        self.assertEqual(4, fixed_decimal.get_prop("precision"))
        self.assertEqual(2, fixed_decimal.get_prop("scale"))
        self.assertEqual(2, fixed_decimal.get_prop("size"))

        bytes_decimal_schema = ValidTestSchema({"type": "bytes", "logicalType": "decimal", "precision": 4})
        bytes_decimal = bytes_decimal_schema.parse()
        self.assertEqual(4, bytes_decimal.get_prop("precision"))
        self.assertEqual(0, bytes_decimal.get_prop("scale"))
        self.assertEqual("decimal", bytes_decimal.get_prop("logicalType"))

    def test_fixed_decimal_valid_max_precision(self):
        # An 8 byte number can represent any 18 digit number.
        fixed_decimal_schema = ValidTestSchema(
            {
                "type": "fixed",
                "logicalType": "decimal",
                "name": "TestDecimal",
                "precision": 18,
                "scale": 0,
                "size": 8,
            }
        )

        fixed_decimal = fixed_decimal_schema.parse()
        self.assertIsInstance(fixed_decimal, avro.schema.FixedSchema)
        self.assertIsInstance(fixed_decimal, avro.schema.DecimalLogicalSchema)

    def test_fixed_decimal_invalid_max_precision(self):
        # An 8 byte number can't represent every 19 digit number, so the logical
        # type is not applied.
        fixed_decimal_schema = ValidTestSchema(
            {
                "type": "fixed",
                "logicalType": "decimal",
                "name": "TestDecimal",
                "precision": 19,
                "scale": 0,
                "size": 8,
            }
        )

        fixed_decimal = fixed_decimal_schema.parse()
        self.assertIsInstance(fixed_decimal, avro.schema.FixedSchema)
        self.assertNotIsInstance(fixed_decimal, avro.schema.DecimalLogicalSchema)

    def test_parse_invalid_symbol(self):
        """Disabling enumschema symbol validation should allow invalid symbols to pass."""
        test_schema_string = json.dumps({"type": "enum", "name": "AVRO2174", "symbols": ["white space"]})

        with self.assertRaises(avro.errors.InvalidName, msg="When enum symbol validation is enabled, an invalid symbol should raise InvalidName."):
            avro.schema.parse(test_schema_string, validate_enum_symbols=True)

        try:
            avro.schema.parse(test_schema_string, validate_enum_symbols=False)
        except avro.errors.InvalidName:  # pragma: no coverage
            self.fail("When enum symbol validation is disabled, an invalid symbol should not raise InvalidName.")

    def test_unsupported_fingerprint_algorithm(self):
        s = avro.schema.parse('"int"')
        self.assertRaises(avro.errors.UnknownFingerprintAlgorithmException, s.fingerprint, "foo")

    def test_less_popular_fingerprint_algorithm(self):
        s = avro.schema.parse('"int"')
        fingerprint = s.fingerprint("sha384")
        hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
        self.assertEqual(hex_fingerprint, "32ed5e4ac896570f044d1dab68f4c8ca9866ac06d22261f399316bf4799e16854750238085775107dfac905c82b2feaf")


class SchemaParseTestCase(unittest.TestCase):
    """Enable generating parse test cases over all the valid and invalid example schema."""

    def __init__(self, test_schema):
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("parse_valid" if test_schema.valid else "parse_invalid")
        self.test_schema = test_schema
        # Never hide repeated warnings when running this test case.
        warnings.simplefilter("always")

    def parse_valid(self) -> None:
        """Parsing a valid schema should not error, but may contain warnings."""
        test_warnings = self.test_schema.warnings or []
        try:
            warnings.filterwarnings(action="error", category=avro.errors.IgnoredLogicalType)
            self.test_schema.parse()
        except avro.errors.IgnoredLogicalType as e:
            self.assertIn(type(e), (type(w) for w in test_warnings))
            self.assertIn(str(e), (str(w) for w in test_warnings))
        except (avro.errors.AvroException, avro.errors.SchemaParseException):  # pragma: no coverage
            self.fail(f"Valid schema failed to parse: {self.test_schema!s}")
        else:
            self.assertEqual([], test_warnings)
        finally:
            warnings.filterwarnings(action="default", category=avro.errors.IgnoredLogicalType)

    def parse_invalid(self):
        """Parsing an invalid schema should error."""
        with self.assertRaises(
            (avro.errors.AvroException, avro.errors.SchemaParseException), msg=f"Invalid schema should not have parsed: {self.test_schema!s}"
        ):
            self.test_schema.parse()


class HashableTestCase(unittest.TestCase):
    """Ensure that Schema are hashable.

    While hashability is implemented with parsing canonical form fingerprinting,
    this test should be kept distinct to avoid coupling."""

    def __init__(self, test_schema):
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("parse_and_hash")
        self.test_schema = test_schema

    def parse_and_hash(self):
        """Ensure that every schema can be hashed."""
        try:
            hash(self.test_schema.parse())
        except TypeError as e:
            if "unhashable type" in str(e):
                self.fail(f"{self.test_schema} is not hashable")
            raise


class RoundTripParseTestCase(unittest.TestCase):
    """Enable generating round-trip parse test cases over all the valid test schema."""

    def __init__(self, test_schema):
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("parse_round_trip")
        self.test_schema = test_schema

    def parse_round_trip(self):
        """The string of a Schema should be parseable to the same Schema."""
        parsed = self.test_schema.parse()
        round_trip = avro.schema.parse(str(parsed))
        self.assertEqual(
            parsed,
            round_trip,
            {
                "original schema": parsed.to_json(),
                "round trip schema": round_trip.to_json(),
            },
        )


class DocAttributesTestCase(unittest.TestCase):
    """Enable generating document attribute test cases over all the document test schema."""

    def __init__(self, test_schema):
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("check_doc_attributes")
        self.test_schema = test_schema

    def check_doc_attributes(self):
        """Documentation attributes should be preserved."""
        sch = self.test_schema.parse()
        self.assertIsNotNone(sch.doc, f"Failed to preserve 'doc' in schema: {self.test_schema!s}")
        if sch.type == "record":
            for f in sch.fields:
                self.assertIsNotNone(
                    f.doc,
                    f"Failed to preserve 'doc' in fields: {self.test_schema!s}",
                )


class OtherAttributesTestCase(unittest.TestCase):
    """Enable generating attribute test cases over all the other-prop test schema."""

    _type_map = {
        "cp_array": list,
        "cp_boolean": bool,
        "cp_float": float,
        "cp_int": int,
        "cp_null": type(None),
        "cp_object": dict,
        "cp_string": str,
    }

    def __init__(self, test_schema):
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("check_attributes")
        self.test_schema = test_schema

    def _check_props(self, props):
        for k, v in props.items():
            self.assertIsInstance(v, self._type_map[k])

    def check_attributes(self):
        """Other attributes and their types on a schema should be preserved."""
        sch = self.test_schema.parse()
        try:
            self.assertNotEqual(sch, object(), "A schema is never equal to a non-schema instance.")
        except AttributeError:  # pragma: no coverage
            self.fail("Comparing a schema to a non-schema should be False, but not error.")
        round_trip = avro.schema.parse(str(sch))
        self.assertEqual(
            sch,
            round_trip,
            "A schema should be equal to another schema parsed from the same json.",
        )
        self.assertEqual(
            sch.other_props,
            round_trip.other_props,
            "Properties were not preserved in a round-trip parse.",
        )
        self._check_props(sch.other_props)
        if sch.type == "record":
            field_props = [f.other_props for f in sch.fields if f.other_props]
            self.assertEqual(len(field_props), len(sch.fields))
            for p in field_props:
                self._check_props(p)


class CanonicalFormTestCase(unittest.TestCase):
    r"""Enable generating canonical-form test cases over the valid schema.
    Transforming into Parsing Canonical Form
    Assuming an input schema (in JSON form) that's already UTF-8 text for a valid Avro schema (including all
    quotes as required by JSON), the following transformations will produce its Parsing Canonical Form:
        - [PRIMITIVES] Convert primitive schemas to their simple form (e.g., int instead of {"type":"int"}).
        - [FULLNAMES] Replace short names with fullnames, using applicable namespaces to do so. Then eliminate
            namespace attributes, which are now redundant.
        - [STRIP] Keep only attributes that are relevant to parsing data, which are: type, name, fields, symbols,
            items, values, size. Strip all others (e.g., doc and aliases).
        - [ORDER] Order the appearance of fields of JSON objects as follows: name, type, fields, symbols, items,
            values, size. For example, if an object has type, name, and size fields, then the name field should
            appear first, followed by the type and then the size fields.
        - [STRINGS] For all JSON string literals in the schema text, replace any escaped characters
            (e.g., \uXXXX escapes) with their UTF-8 equivalents.
        - [INTEGERS] Eliminate quotes around and any leading zeros in front of JSON integer literals
            (which appear in the size attributes of fixed schemas).
        - [WHITESPACE] Eliminate all whitespace in JSON outside of string literals.
    We depend on the Python json parser to properly handle the STRINGS and INTEGERS rules, so
    we don't test them here.
    """

    def compact_json_string(self, json_doc):
        """Returns compact-encoded JSON string representation for supplied document.

        Args:
            json_doc (json): JSON Document

        Returns:
            str: Compact-encoded, stringified JSON document
        """
        return json.dumps(json_doc, separators=(",", ":"))

    def test_primitive_int(self):
        """
        Convert primitive schemas to their simple form (e.g., int instead of {"type":"int"}).
        """
        s = avro.schema.parse(json.dumps("int"))
        self.assertEqual(s.canonical_form, '"int"')

        s = avro.schema.parse(json.dumps({"type": "int"}))
        self.assertEqual(s.canonical_form, '"int"')

    def test_primitive_float(self):
        s = avro.schema.parse(json.dumps("float"))
        self.assertEqual(s.canonical_form, '"float"')

        s = avro.schema.parse(json.dumps({"type": "float"}))
        self.assertEqual(s.canonical_form, '"float"')

    def test_primitive_double(self):
        s = avro.schema.parse(json.dumps("double"))
        self.assertEqual(s.canonical_form, '"double"')

        s = avro.schema.parse(json.dumps({"type": "double"}))
        self.assertEqual(s.canonical_form, '"double"')

    def test_primitive_null(self):
        s = avro.schema.parse(json.dumps("null"))
        self.assertEqual(s.canonical_form, '"null"')

        s = avro.schema.parse(json.dumps({"type": "null"}))
        self.assertEqual(s.canonical_form, '"null"')

    def test_primitive_bytes(self):
        s = avro.schema.parse(json.dumps("bytes"))
        self.assertEqual(s.canonical_form, '"bytes"')

        s = avro.schema.parse(json.dumps({"type": "bytes"}))
        self.assertEqual(s.canonical_form, '"bytes"')

    def test_primitive_long(self):
        s = avro.schema.parse(json.dumps("long"))
        self.assertEqual(s.canonical_form, '"long"')

        s = avro.schema.parse(json.dumps({"type": "long"}))
        self.assertEqual(s.canonical_form, '"long"')

    def test_primitive_boolean(self):
        s = avro.schema.parse(json.dumps("boolean"))
        self.assertEqual(s.canonical_form, '"boolean"')

        s = avro.schema.parse(json.dumps({"type": "boolean"}))
        self.assertEqual(s.canonical_form, '"boolean"')

    def test_primitive_string(self):
        s = avro.schema.parse(json.dumps("string"))
        self.assertEqual(s.canonical_form, '"string"')

        s = avro.schema.parse(json.dumps({"type": "string"}))
        self.assertEqual(s.canonical_form, '"string"')

    def test_integer_canonical_form(self):
        """
        Integer literals starting with 0 are illegal in python, because of ambiguity. This is a placeholder test
        for INTEGERS canonical form, which should generally succeed provided a valid integer has been supplied.
        """
        s = avro.schema.parse('{"name":"md5","type":"fixed","size":16}')
        self.assertEqual(
            s.canonical_form,
            self.compact_json_string({"name": "md5", "type": "fixed", "size": 16}),
        )

    def test_string_with_escaped_characters(self):
        """
        Replace any escaped characters (e.g., \u0031 escapes) with their UTF-8 equivalents.
        """
        s = avro.schema.parse('{"name":"\u0041","type":"fixed","size":16}')
        self.assertEqual(
            s.canonical_form,
            self.compact_json_string({"name": "A", "type": "fixed", "size": 16}),
        )

    def test_fullname(self):
        """
        Replace short names with fullnames, using applicable namespaces to do so. Then eliminate namespace attributes, which are now redundant.
        """
        s = avro.schema.parse(
            json.dumps(
                {
                    "namespace": "avro",
                    "name": "example",
                    "type": "enum",
                    "symbols": ["a", "b"],
                }
            )
        )
        self.assertEqual(
            s.canonical_form,
            self.compact_json_string({"name": "avro.example", "type": "enum", "symbols": ["a", "b"]}),
        )

    def test_strip(self):
        """
        Keep only attributes that are relevant to parsing data, which are: type, name, fields, symbols, items, values,
        size. Strip all others (e.g., doc and aliases).
        """
        s = avro.schema.parse(
            json.dumps(
                {
                    "name": "foo",
                    "type": "enum",
                    "doc": "test",
                    "aliases": ["bar"],
                    "symbols": ["a", "b"],
                }
            )
        )
        self.assertEqual(
            s.canonical_form,
            self.compact_json_string({"name": "foo", "type": "enum", "symbols": ["a", "b"]}),
        )

    def test_order(self):
        """
        Order the appearance of fields of JSON objects as follows: name, type, fields, symbols, items, values, size.
        For example, if an object has type, name, and size fields, then the name field should appear first, followed
        by the type and then the size fields.
        """
        s = avro.schema.parse(json.dumps({"symbols": ["a", "b"], "type": "enum", "name": "example"}))
        self.assertEqual(
            s.canonical_form,
            self.compact_json_string({"name": "example", "type": "enum", "symbols": ["a", "b"]}),
        )

    def test_whitespace(self):
        """
        Eliminate all whitespace in JSON outside of string literals.
        """
        s = avro.schema.parse(
            """{"type": "fixed",
            "size": 16,
            "name": "md5"}
                """
        )
        self.assertEqual(
            s.canonical_form,
            self.compact_json_string({"name": "md5", "type": "fixed", "size": 16}),
        )

    def test_record_field(self):
        """
        Ensure that record fields produce the correct parsing canonical form.
        """
        s = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "Test",
                    "doc": "This is a test schema",
                    "aliases": ["also", "known", "as"],
                    "fields": [
                        {
                            "type": {
                                "symbols": ["one", "two"],
                                "type": "enum",
                                "name": "NamedEnum",
                            },
                            "name": "thenamedenum",
                            "doc": "This is a named enum",
                        },
                        {"type": ["null", "NamedEnum"], "name": "unionwithreftoenum"},
                    ],
                }
            )
        )
        expected = self.compact_json_string(
            {
                "name": "Test",
                "type": "record",
                "fields": [
                    {
                        "name": "thenamedenum",
                        "type": {
                            "name": "NamedEnum",
                            "type": "enum",
                            "symbols": ["one", "two"],
                        },
                    },
                    {"name": "unionwithreftoenum", "type": ["null", "NamedEnum"]},
                ],
            }
        )
        self.assertEqual(s.canonical_form, expected)

    def test_array(self):
        """
        Ensure that array schema produce the correct parsing canonical form.
        """
        s = avro.schema.parse(json.dumps({"items": "long", "type": "array"}))
        self.assertEqual(
            s.canonical_form,
            self.compact_json_string({"type": "array", "items": "long"}),
        )

    def test_map(self):
        """
        Ensure that map schema produce the correct parsing canonical form.
        """
        s = avro.schema.parse(json.dumps({"values": "long", "type": "map"}))
        self.assertEqual(
            s.canonical_form,
            self.compact_json_string({"type": "map", "values": "long"}),
        )

    def test_union(self):
        """
        Ensure that a union schema produces the correct parsing canonical form.
        """
        s = avro.schema.parse(json.dumps(["string", "null", "long"]))
        self.assertEqual(s.canonical_form, '["string","null","long"]')

    def test_large_record_handshake_request(self):
        s = avro.schema.parse(
            """
            {
            "type": "record",
            "name": "HandshakeRequest",
            "namespace": "org.apache.avro.ipc",
            "fields": [
                {
                "name": "clientHash",
                "type": {"type": "fixed", "name": "MD5", "size": 16}
                },
                {"name": "clientProtocol", "type": ["null", "string"]},
                {"name": "serverHash", "type": "MD5"},
                {
                "name": "meta",
                "type": ["null", {"type": "map", "values": "bytes"}]
                }
            ]
            }
            """
        )
        self.assertEqual(
            s.canonical_form,
            (
                '{"name":"org.apache.avro.ipc.HandshakeRequest","type":"record",'
                '"fields":[{"name":"clientHash","type":{"name":"org.apache.avro.ipc.MD5",'
                '"type":"fixed","size":16}},{"name":"clientProtocol","type":["null","string"]},'
                '{"name":"serverHash","type":{"name":"org.apache.avro.ipc.MD5","type":"fixed","size":16}},'
                '{"name":"meta","type":["null",{"type":"map","values":"bytes"}]}]}'
            ),
        )

    def test_large_record_handshake_response(self):
        s = avro.schema.parse(
            """
            {
            "type": "record",
            "name": "HandshakeResponse",
            "namespace": "org.apache.avro.ipc",
            "fields": [
                {
                "name": "match",
                "type": {
                    "type": "enum",
                    "name": "HandshakeMatch",
                    "symbols": ["BOTH", "CLIENT", "NONE"]
                }
                },
                {"name": "serverProtocol", "type": ["null", "string"]},
                {
                "name": "serverHash",
                "type": ["null", {"name": "MD5", "size": 16, "type": "fixed"}]
                },
                {
                "name": "meta",
                "type": ["null", {"type": "map", "values": "bytes"}]}]
                }
            """
        )
        self.assertEqual(
            s.canonical_form,
            (
                '{"name":"org.apache.avro.ipc.HandshakeResponse","type":"rec'
                'ord","fields":[{"name":"match","type":{"name":"org.apache.a'
                'vro.ipc.HandshakeMatch","type":"enum","symbols":["BOTH","CL'
                'IENT","NONE"]}},{"name":"serverProtocol","type":["null","st'
                'ring"]},{"name":"serverHash","type":["null",{"name":"org.ap'
                'ache.avro.ipc.MD5","type":"fixed","size":16}]},{"name":"met'
                'a","type":["null",{"type":"map","values":"bytes"}]}]}'
            ),
        )

    def test_large_record_interop(self):
        s = avro.schema.parse(
            """
            {
            "type": "record",
            "name": "Interop",
            "namespace": "org.apache.avro",
            "fields": [
                {"name": "intField", "type": "int"},
                {"name": "longField", "type": "long"},
                {"name": "stringField", "type": "string"},
                {"name": "boolField", "type": "boolean"},
                {"name": "floatField", "type": "float"},
                {"name": "doubleField", "type": "double"},
                {"name": "bytesField", "type": "bytes"},
                {"name": "nullField", "type": "null"},
                {"name": "arrayField", "type": {"type": "array", "items": "double"}},
                {
                "name": "mapField",
                "type": {
                    "type": "map",
                    "values": {"name": "Foo",
                            "type": "record",
                            "fields": [{"name": "label", "type": "string"}]}
                }
                },
                {
                "name": "unionField",
                "type": ["boolean", "double", {"type": "array", "items": "bytes"}]
                },
                {
                "name": "enumField",
                "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B", "C"]}
                },
                {
                "name": "fixedField",
                "type": {"type": "fixed", "name": "MD5", "size": 16}
                },
                {
                "name": "recordField",
                "type": {"type": "record",
                        "name": "Node",
                        "fields": [{"name": "label", "type": "string"},
                                    {"name": "children",
                                    "type": {"type": "array",
                                                "items": "Node"}}]}
                }
            ]
            }
            """
        )
        self.assertEqual(
            s.canonical_form,
            (
                '{"name":"org.apache.avro.Interop","type":"record","fields":[{"na'
                'me":"intField","type":"int"},{"name":"longField","type":"long"},'
                '{"name":"stringField","type":"string"},{"name":"boolField","type'
                '":"boolean"},{"name":"floatField","type":"float"},{"name":"doubl'
                'eField","type":"double"},{"name":"bytesField","type":"bytes"},{"'
                'name":"nullField","type":"null"},{"name":"arrayField","type":{"t'
                'ype":"array","items":"double"}},{"name":"mapField","type":{"type'
                '":"map","values":{"name":"org.apache.avro.Foo","type":"record","'
                'fields":[{"name":"label","type":"string"}]}}},{"name":"unionFiel'
                'd","type":["boolean","double",{"type":"array","items":"bytes"}]}'
                ',{"name":"enumField","type":{"name":"org.apache.avro.Kind","type'
                '":"enum","symbols":["A","B","C"]}},{"name":"fixedField","type":{'
                '"name":"org.apache.avro.MD5","type":"fixed","size":16}},{"name":'
                '"recordField","type":{"name":"org.apache.avro.Node","type":"reco'
                'rd","fields":[{"name":"label","type":"string"},{"name":"children'
                '","type":{"type":"array","items":"org.apache.avro.Node"}}]}}]}'
            ),
        )


class FingerprintTestCase(unittest.TestCase):
    """
    Enable generating fingerprint test cases across algorithms.

    Fingerprint examples are in the form of tuples:
        - Value in Position 0 is schema
        - Value in Position 1 is an array of fingerprints:
            - Position 0 is CRC-64-AVRO fingerprint
            - Position 0 is MD5 fingerprint
            - Position 0 is SHA256 fingerprint
    """

    def __init__(self, test_schema, fingerprints):
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super(FingerprintTestCase, self).__init__("validate_fingerprint")
        self.test_schema = test_schema
        self.fingerprints = fingerprints

    def _hex_fingerprint(self, fingerprint):
        return "".join(format(b, "02x") for b in fingerprint).zfill(16)

    def validate_fingerprint(self):
        """The string of a Schema should be parseable to the same Schema."""
        s = avro.schema.parse(self.test_schema)
        self.assertEqual(self._hex_fingerprint(s.fingerprint()), self.fingerprints[0])
        self.assertEqual(self._hex_fingerprint(s.fingerprint("md5")), self.fingerprints[1])
        self.assertEqual(self._hex_fingerprint(s.fingerprint("sha256")), self.fingerprints[2])


def load_tests(loader, default_tests, pattern):
    """Generate test cases across many test schema."""
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestMisc))
    suite.addTests(SchemaParseTestCase(ex) for ex in EXAMPLES)
    suite.addTests(RoundTripParseTestCase(ex) for ex in VALID_EXAMPLES)
    suite.addTests(DocAttributesTestCase(ex) for ex in DOC_EXAMPLES)
    suite.addTests(OtherAttributesTestCase(ex) for ex in OTHER_PROP_EXAMPLES)
    suite.addTests(loader.loadTestsFromTestCase(CanonicalFormTestCase))
    suite.addTests(FingerprintTestCase(ex[0], ex[1]) for ex in FINGERPRINT_EXAMPLES)
    suite.addTests(HashableTestCase(ex) for ex in VALID_EXAMPLES)
    return suite


if __name__ == "__main__":  # pragma: no coverage
    unittest.main()
