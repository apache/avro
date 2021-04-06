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

import json

from avro.compatibility import ReaderWriterCompatibilityChecker, SchemaCompatibilityType, SchemaType
from avro.schema import ArraySchema, MapSchema, Names, PrimitiveSchema, Schema, UnionSchema, parse

BOOLEAN_SCHEMA = PrimitiveSchema(SchemaType.BOOLEAN)
NULL_SCHEMA = PrimitiveSchema(SchemaType.NULL)
INT_SCHEMA = PrimitiveSchema(SchemaType.INT)
LONG_SCHEMA = PrimitiveSchema(SchemaType.LONG)
STRING_SCHEMA = PrimitiveSchema(SchemaType.STRING)
BYTES_SCHEMA = PrimitiveSchema(SchemaType.BYTES)
FLOAT_SCHEMA = PrimitiveSchema(SchemaType.FLOAT)
DOUBLE_SCHEMA = PrimitiveSchema(SchemaType.DOUBLE)
INT_ARRAY_SCHEMA = ArraySchema(SchemaType.INT, names=Names())
LONG_ARRAY_SCHEMA = ArraySchema(SchemaType.LONG, names=Names())
STRING_ARRAY_SCHEMA = ArraySchema(SchemaType.STRING, names=Names())
INT_MAP_SCHEMA = MapSchema(SchemaType.INT, names=Names())
LONG_MAP_SCHEMA = MapSchema(SchemaType.LONG, names=Names())
STRING_MAP_SCHEMA = MapSchema(SchemaType.STRING, names=Names())
ENUM1_AB_SCHEMA = parse(json.dumps({"type": SchemaType.ENUM, "name": "Enum1", "symbols": ["A", "B"]}))
ENUM1_ABC_SCHEMA = parse(json.dumps({"type": SchemaType.ENUM, "name": "Enum1", "symbols": ["A", "B", "C"]}))
ENUM1_BC_SCHEMA = parse(json.dumps({"type": SchemaType.ENUM, "name": "Enum1", "symbols": ["B", "C"]}))
ENUM2_AB_SCHEMA = parse(json.dumps({"type": SchemaType.ENUM, "name": "Enum2", "symbols": ["A", "B"]}))
ENUM_ABC_ENUM_DEFAULT_A_SCHEMA = parse(
    json.dumps({
        "type": "enum",
        "name": "Enum",
        "symbols": ["A", "B", "C"],
        "default": "A"
    })
)
ENUM_AB_ENUM_DEFAULT_A_SCHEMA = parse(json.dumps({"type": SchemaType.ENUM, "name": "Enum", "symbols": ["A", "B"], "default": "A"}))
ENUM_ABC_ENUM_DEFAULT_A_RECORD = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record",
        "fields": [{
            "name": "Field",
            "type": {
                "type": SchemaType.ENUM,
                "name": "Enum",
                "symbols": ["A", "B", "C"],
                "default": "A"
            }
        }]
    })
)
ENUM_AB_ENUM_DEFAULT_A_RECORD = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record",
        "fields": [{
            "name": "Field",
            "type": {
                "type": SchemaType.ENUM,
                "name": "Enum",
                "symbols": ["A", "B"],
                "default": "A"
            }
        }]
    })
)
ENUM_ABC_FIELD_DEFAULT_B_ENUM_DEFAULT_A_RECORD = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record",
        "fields": [{
            "name": "Field",
            "type": {
                "type": SchemaType.ENUM,
                "name": "Enum",
                "symbols": ["A", "B", "C"],
                "default": "A"
            },
            "default": "B"
        }]
    })
)
ENUM_AB_FIELD_DEFAULT_A_ENUM_DEFAULT_B_RECORD = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record",
        "fields": [{
            "name": "Field",
            "type": {
                "type": SchemaType.ENUM,
                "name": "Enum",
                "symbols": ["A", "B"],
                "default": "B"
            },
            "default": "A"
        }]
    })
)
EMPTY_UNION_SCHEMA = UnionSchema([], names=Names())
NULL_UNION_SCHEMA = UnionSchema([SchemaType.NULL], names=Names())
INT_UNION_SCHEMA = UnionSchema([SchemaType.INT], names=Names())
LONG_UNION_SCHEMA = UnionSchema([SchemaType.LONG], names=Names())
FLOAT_UNION_SCHEMA = UnionSchema([SchemaType.FLOAT], names=Names())
DOUBLE_UNION_SCHEMA = UnionSchema([SchemaType.DOUBLE], names=Names())
STRING_UNION_SCHEMA = UnionSchema([SchemaType.STRING], names=Names())
BYTES_UNION_SCHEMA = UnionSchema([SchemaType.BYTES], names=Names())
INT_STRING_UNION_SCHEMA = UnionSchema([SchemaType.INT, SchemaType.STRING], names=Names())
STRING_INT_UNION_SCHEMA = UnionSchema([SchemaType.STRING, SchemaType.INT], names=Names())
INT_FLOAT_UNION_SCHEMA = UnionSchema([SchemaType.INT, SchemaType.FLOAT], names=Names())
INT_LONG_UNION_SCHEMA = UnionSchema([SchemaType.INT, SchemaType.LONG], names=Names())
INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA = UnionSchema([SchemaType.INT, SchemaType.LONG, SchemaType.FLOAT, SchemaType.DOUBLE], names=Names())
NULL_INT_ARRAY_UNION_SCHEMA = UnionSchema([{"type": SchemaType.NULL}, {"type": SchemaType.ARRAY, "items": SchemaType.INT}], names=Names())
NULL_INT_MAP_UNION_SCHEMA = UnionSchema([{"type": SchemaType.NULL}, {"type": SchemaType.MAP, "values": SchemaType.INT}], names=Names())
EMPTY_RECORD1 = parse(json.dumps({"type": SchemaType.RECORD, "name": "Record1", "fields": []}))
EMPTY_RECORD2 = parse(json.dumps({"type": SchemaType.RECORD, "name": "Record2", "fields": []}))
A_INT_RECORD1 = parse(json.dumps({"type": SchemaType.RECORD, "name": "Record1", "fields": [{"name": "a", "type": SchemaType.INT}]}))
A_LONG_RECORD1 = parse(json.dumps({"type": SchemaType.RECORD, "name": "Record1", "fields": [{"name": "a", "type": SchemaType.LONG}]}))
A_INT_B_INT_RECORD1 = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": SchemaType.INT
        }, {
            "name": "b",
            "type": SchemaType.INT
        }]
    })
)
A_DINT_RECORD1 = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": SchemaType.INT,
            "default": 0
        }]
    })
)
A_INT_B_DINT_RECORD1 = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": SchemaType.INT
        }, {
            "name": "b",
            "type": SchemaType.INT,
            "default": 0
        }]
    })
)
A_DINT_B_DINT_RECORD1 = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": SchemaType.INT,
            "default": 0
        }, {
            "name": "b",
            "type": SchemaType.INT,
            "default": 0
        }]
    })
)
A_DINT_B_DFIXED_4_BYTES_RECORD1 = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": SchemaType.INT,
            "default": 0
        }, {
            "name": "b",
            "type": {
                "type": SchemaType.FIXED,
                "name": "Fixed",
                "size": 4
            }
        }]
    })
)
A_DINT_B_DFIXED_8_BYTES_RECORD1 = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": SchemaType.INT,
            "default": 0
        }, {
            "name": "b",
            "type": {
                "type": SchemaType.FIXED,
                "name": "Fixed",
                "size": 8
            }
        }]
    })
)
A_DINT_B_DINT_STRING_UNION_RECORD1 = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": SchemaType.INT,
            "default": 0
        }, {
            "name": "b",
            "type": [SchemaType.INT, SchemaType.STRING],
            "default": 0
        }]
    })
)
A_DINT_B_DINT_UNION_RECORD1 = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": SchemaType.INT,
            "default": 0
        }, {
            "name": "b",
            "type": [SchemaType.INT],
            "default": 0
        }]
    })
)
A_DINT_B_DENUM_1_RECORD1 = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": SchemaType.INT,
            "default": 0
        }, {
            "name": "b",
            "type": {
                "type": SchemaType.ENUM,
                "name": "Enum1",
                "symbols": ["A", "B"]
            }
        }]
    })
)
A_DINT_B_DENUM_2_RECORD1 = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": SchemaType.INT,
            "default": 0
        }, {
            "name": "b",
            "type": {
                "type": SchemaType.ENUM,
                "name": "Enum2",
                "symbols": ["A", "B"]
            }
        }]
    })
)
FIXED_4_BYTES = parse(json.dumps({"type": SchemaType.FIXED, "name": "Fixed", "size": 4}))
FIXED_8_BYTES = parse(json.dumps({"type": SchemaType.FIXED, "name": "Fixed", "size": 8}))
NS_RECORD1 = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record1",
        "fields": [{
            "name": "f1",
            "type": [
                SchemaType.NULL, {
                    "type": SchemaType.ARRAY,
                    "items": {
                        "type": SchemaType.RECORD,
                        "name": "InnerRecord1",
                        "namespace": "ns1",
                        "fields": [{
                            "name": "a",
                            "type": SchemaType.INT
                        }]
                    }
                }
            ]
        }]
    })
)
NS_RECORD2 = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record1",
        "fields": [{
            "name": "f1",
            "type": [
                SchemaType.NULL, {
                    "type": SchemaType.ARRAY,
                    "items": {
                        "type": SchemaType.RECORD,
                        "name": "InnerRecord1",
                        "namespace": "ns2",
                        "fields": [{
                            "name": "a",
                            "type": SchemaType.INT
                        }]
                    }
                }
            ]
        }]
    })
)

UNION_INT_RECORD1 = UnionSchema([
    {"type": SchemaType.INT},
    {"type": SchemaType.RECORD, "name": "Record1", "fields": [{"name": "field1", "type": SchemaType.INT}]}
])
UNION_INT_RECORD2 = UnionSchema([
    {"type": SchemaType.INT},
    {"type": "record", "name": "Record2", "fields": [{"name": "field1", "type": SchemaType.INT}]}
])
UNION_INT_ENUM1_AB = UnionSchema([{"type": SchemaType.INT}, ENUM1_AB_SCHEMA.to_json()])
UNION_INT_FIXED_4_BYTES = UnionSchema([{"type": SchemaType.INT}, FIXED_4_BYTES.to_json()])
UNION_INT_BOOLEAN = UnionSchema([{"type": SchemaType.INT}, {"type": SchemaType.BOOLEAN}])
UNION_INT_ARRAY_INT = UnionSchema([{"type": SchemaType.INT}, INT_ARRAY_SCHEMA.to_json()])
UNION_INT_MAP_INT = UnionSchema([{"type": SchemaType.INT}, INT_MAP_SCHEMA.to_json()])
UNION_INT_NULL = UnionSchema([{"type": SchemaType.INT}, {"type": SchemaType.NULL}])
FIXED_4_ANOTHER_NAME = parse(json.dumps({"type": SchemaType.FIXED, "name": "AnotherName", "size": 4}))
RECORD1_WITH_ENUM_AB = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record1",
        "fields": [{
            "name": "field1",
            "type": dict(ENUM1_AB_SCHEMA.to_json())
        }]
    })
)
RECORD1_WITH_ENUM_ABC = parse(
    json.dumps({
        "type": SchemaType.RECORD,
        "name": "Record1",
        "fields": [{
            "name": "field1",
            "type": dict(ENUM1_ABC_SCHEMA.to_json())
        }]
    })
)


def test_simple_schema_promotion():
    field_alias_reader = parse(
        json.dumps({
            "name": "foo",
            "type": "record",
            "fields": [{
                "type": "int",
                "name": "bar",
                "aliases": ["f1"]
            }]
        })
    )
    record_alias_reader = parse(
        json.dumps({
            "name": "other",
            "type": "record",
            "fields": [{
                "type": "int",
                "name": "f1"
            }],
            "aliases": ["foo"]
        })
    )

    writer = parse(
        json.dumps({
            "name": "foo",
            "type": "record",
            "fields": [{
                "type": "int",
                "name": "f1"
            }, {
                "type": "string",
                "name": "f2",
            }]
        })
    )
    # alias testing
    res = ReaderWriterCompatibilityChecker().get_compatibility(field_alias_reader, writer)
    assert res.compatibility is SchemaCompatibilityType.compatible, res.locations
    res = ReaderWriterCompatibilityChecker().get_compatibility(record_alias_reader, writer)
    assert res.compatibility is SchemaCompatibilityType.compatible, res.locations


def test_schema_compatibility():
    # testValidateSchemaPairMissingField
    writer = parse(
        json.dumps({
            "type": SchemaType.RECORD,
            "name": "Record",
            "fields": [{
                "name": "oldField1",
                "type": SchemaType.INT
            }, {
                "name": "oldField2",
                "type": SchemaType.STRING
            }]
        })
    )
    reader = parse(json.dumps({"type": SchemaType.RECORD, "name": "Record", "fields": [{"name": "oldField1", "type": SchemaType.INT}]}))
    assert are_compatible(reader, writer)
    # testValidateSchemaPairMissingSecondField
    reader = parse(json.dumps({"type": SchemaType.RECORD, "name": "Record", "fields": [{"name": "oldField2", "type": SchemaType.STRING}]}))
    assert are_compatible(reader, writer)
    # testValidateSchemaPairAllFields
    reader = parse(
        json.dumps({
            "type": SchemaType.RECORD,
            "name": "Record",
            "fields": [{
                "name": "oldField1",
                "type": SchemaType.INT
            }, {
                "name": "oldField2",
                "type": SchemaType.STRING
            }]
        })
    )
    assert are_compatible(reader, writer)
    # testValidateSchemaNewFieldWithDefault
    reader = parse(
        json.dumps({
            "type": SchemaType.RECORD,
            "name": "Record",
            "fields": [{
                "name": "oldField1",
                "type": SchemaType.INT
            }, {
                "name": "newField2",
                "type": SchemaType.INT,
                "default": 42
            }]
        })
    )
    assert are_compatible(reader, writer)
    # testValidateSchemaNewField
    reader = parse(
        json.dumps({
            "type": SchemaType.RECORD,
            "name": "Record",
            "fields": [{
                "name": "oldField1",
                "type": SchemaType.INT
            }, {
                "name": "newField2",
                "type": SchemaType.INT
            }]
        })
    )
    assert not are_compatible(reader, writer)
    # testValidateArrayWriterSchema
    writer = parse(json.dumps({"type": SchemaType.ARRAY, "items": {"type": SchemaType.STRING}}))
    reader = parse(json.dumps({"type": SchemaType.ARRAY, "items": {"type": SchemaType.STRING}}))
    assert are_compatible(reader, writer)
    reader = parse(json.dumps({"type": SchemaType.MAP, "values": {"type": SchemaType.STRING}}))
    assert not are_compatible(reader, writer)
    # testValidatePrimitiveWriterSchema
    writer = parse(json.dumps({"type": SchemaType.STRING}))
    reader = parse(json.dumps({"type": SchemaType.STRING}))
    assert are_compatible(reader, writer)
    reader = parse(json.dumps({"type": SchemaType.INT}))
    assert not are_compatible(reader, writer)
    # testUnionReaderWriterSubsetIncompatibility
    writer = parse(
        json.dumps({
            "name": "Record",
            "type": "record",
            "fields": [{
                "name": "f1",
                "type": [SchemaType.INT, SchemaType.STRING, SchemaType.LONG]
            }]
        })
    )
    reader = parse(json.dumps({"name": "Record", "type": SchemaType.RECORD, "fields": [{"name": "f1", "type": [SchemaType.INT, SchemaType.STRING]}]}))
    reader = reader.fields[0].type
    writer = writer.fields[0].type
    assert isinstance(reader, UnionSchema)
    assert isinstance(writer, UnionSchema)
    assert not are_compatible(reader, writer)
    # testReaderWriterCompatibility
    compatible_reader_writer_test_cases = [
        (BOOLEAN_SCHEMA, BOOLEAN_SCHEMA),
        (INT_SCHEMA, INT_SCHEMA),
        (LONG_SCHEMA, INT_SCHEMA),
        (LONG_SCHEMA, LONG_SCHEMA),
        (FLOAT_SCHEMA, INT_SCHEMA),
        (FLOAT_SCHEMA, LONG_SCHEMA),
        (DOUBLE_SCHEMA, LONG_SCHEMA),
        (DOUBLE_SCHEMA, INT_SCHEMA),
        (DOUBLE_SCHEMA, FLOAT_SCHEMA),
        (STRING_SCHEMA, STRING_SCHEMA),
        (BYTES_SCHEMA, BYTES_SCHEMA),
        (STRING_SCHEMA, BYTES_SCHEMA),
        (BYTES_SCHEMA, STRING_SCHEMA),
        (INT_ARRAY_SCHEMA, INT_ARRAY_SCHEMA),
        (LONG_ARRAY_SCHEMA, INT_ARRAY_SCHEMA),
        (INT_MAP_SCHEMA, INT_MAP_SCHEMA),
        (LONG_MAP_SCHEMA, INT_MAP_SCHEMA),
        (ENUM1_AB_SCHEMA, ENUM1_AB_SCHEMA),
        (ENUM1_ABC_SCHEMA, ENUM1_AB_SCHEMA),
        # Union related pairs
        (EMPTY_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
        (FLOAT_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
        (FLOAT_UNION_SCHEMA, INT_UNION_SCHEMA),
        (FLOAT_UNION_SCHEMA, LONG_UNION_SCHEMA),
        (FLOAT_UNION_SCHEMA, INT_LONG_UNION_SCHEMA),
        (INT_UNION_SCHEMA, INT_UNION_SCHEMA),
        (INT_STRING_UNION_SCHEMA, STRING_INT_UNION_SCHEMA),
        (INT_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
        (LONG_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
        (LONG_UNION_SCHEMA, INT_UNION_SCHEMA),
        (FLOAT_UNION_SCHEMA, INT_UNION_SCHEMA),
        (DOUBLE_UNION_SCHEMA, INT_UNION_SCHEMA),
        (FLOAT_UNION_SCHEMA, LONG_UNION_SCHEMA),
        (DOUBLE_UNION_SCHEMA, LONG_UNION_SCHEMA),
        (FLOAT_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
        (DOUBLE_UNION_SCHEMA, FLOAT_UNION_SCHEMA),
        (STRING_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
        (STRING_UNION_SCHEMA, BYTES_UNION_SCHEMA),
        (BYTES_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
        (BYTES_UNION_SCHEMA, STRING_UNION_SCHEMA),
        (DOUBLE_UNION_SCHEMA, INT_FLOAT_UNION_SCHEMA),
        # Readers capable of reading all branches of a union are compatible
        (FLOAT_SCHEMA, INT_FLOAT_UNION_SCHEMA),
        (LONG_SCHEMA, INT_LONG_UNION_SCHEMA),
        (DOUBLE_SCHEMA, INT_FLOAT_UNION_SCHEMA),
        (DOUBLE_SCHEMA, INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA),
        # Special case of singleton unions:
        (FLOAT_SCHEMA, FLOAT_UNION_SCHEMA),
        (INT_UNION_SCHEMA, INT_SCHEMA),
        (INT_SCHEMA, INT_UNION_SCHEMA),
        # Fixed types
        (FIXED_4_BYTES, FIXED_4_BYTES),
        # Tests involving records:
        (EMPTY_RECORD1, EMPTY_RECORD1),
        (EMPTY_RECORD1, A_INT_RECORD1),
        (A_INT_RECORD1, A_INT_RECORD1),
        (A_DINT_RECORD1, A_INT_RECORD1),
        (A_DINT_RECORD1, A_DINT_RECORD1),
        (A_INT_RECORD1, A_DINT_RECORD1),
        (A_LONG_RECORD1, A_INT_RECORD1),
        (A_INT_RECORD1, A_INT_B_INT_RECORD1),
        (A_DINT_RECORD1, A_INT_B_INT_RECORD1),
        (A_INT_B_DINT_RECORD1, A_INT_RECORD1),
        (A_DINT_B_DINT_RECORD1, EMPTY_RECORD1),
        (A_DINT_B_DINT_RECORD1, A_INT_RECORD1),
        (A_INT_B_INT_RECORD1, A_DINT_B_DINT_RECORD1),
        (parse(json.dumps({"type": "null"})), parse(json.dumps({"type": "null"}))),
        (NULL_SCHEMA, NULL_SCHEMA),
        (ENUM_AB_ENUM_DEFAULT_A_RECORD, ENUM_ABC_ENUM_DEFAULT_A_RECORD),
        (ENUM_AB_FIELD_DEFAULT_A_ENUM_DEFAULT_B_RECORD, ENUM_ABC_FIELD_DEFAULT_B_ENUM_DEFAULT_A_RECORD),
        (NS_RECORD1, NS_RECORD2),
    ]

    for (reader, writer) in compatible_reader_writer_test_cases:
        assert are_compatible(reader, writer)


def test_schema_compatibility_fixed_size_mismatch():
    incompatible_fixed_pairs = [
        (FIXED_4_BYTES, FIXED_8_BYTES, "expected: 8, found: 4", "/size"),
        (FIXED_8_BYTES, FIXED_4_BYTES, "expected: 4, found: 8", "/size"),
        (A_DINT_B_DFIXED_8_BYTES_RECORD1, A_DINT_B_DFIXED_4_BYTES_RECORD1, "expected: 4, found: 8", "/fields/1/type/size"),
        (A_DINT_B_DFIXED_4_BYTES_RECORD1, A_DINT_B_DFIXED_8_BYTES_RECORD1, "expected: 8, found: 4", "/fields/1/type/size"),
    ]
    for (reader, writer, message, location) in incompatible_fixed_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert location in result.locations, "expected {}, found {}".format(location, result)
        assert message in result.messages, "expected {}, found {}".format(location, result)


def test_schema_compatibility_missing_enum_symbols():
    incompatible_pairs = [
        # str(set) representation
        (ENUM1_AB_SCHEMA, ENUM1_ABC_SCHEMA, "{'C'}", "/symbols"),
        (ENUM1_BC_SCHEMA, ENUM1_ABC_SCHEMA, "{'A'}", "/symbols"),
        (RECORD1_WITH_ENUM_AB, RECORD1_WITH_ENUM_ABC, "{'C'}", "/fields/0/type/symbols"),
    ]
    for (reader, writer, message, location) in incompatible_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert message in result.messages
        assert location in result.locations


def test_schema_compatibility_missing_union_branch():
    incompatible_pairs = [
        (INT_UNION_SCHEMA, INT_STRING_UNION_SCHEMA, {"reader union lacking writer type: STRING"}, {"/1"}),
        (STRING_UNION_SCHEMA, INT_STRING_UNION_SCHEMA, {"reader union lacking writer type: INT"}, {"/0"}),
        (INT_UNION_SCHEMA, UNION_INT_RECORD1, {"reader union lacking writer type: RECORD"}, {"/1"}),
        (INT_UNION_SCHEMA, UNION_INT_RECORD2, {"reader union lacking writer type: RECORD"}, {"/1"}),
        (UNION_INT_RECORD1, UNION_INT_RECORD2, {"reader union lacking writer type: RECORD"}, {"/1"}),
        (INT_UNION_SCHEMA, UNION_INT_ENUM1_AB, {"reader union lacking writer type: ENUM"}, {"/1"}),
        (INT_UNION_SCHEMA, UNION_INT_FIXED_4_BYTES, {"reader union lacking writer type: FIXED"}, {"/1"}),
        (INT_UNION_SCHEMA, UNION_INT_BOOLEAN, {"reader union lacking writer type: BOOLEAN"}, {"/1"}),
        (INT_UNION_SCHEMA, LONG_UNION_SCHEMA, {"reader union lacking writer type: LONG"}, {"/0"}),
        (INT_UNION_SCHEMA, FLOAT_UNION_SCHEMA, {"reader union lacking writer type: FLOAT"}, {"/0"}),
        (INT_UNION_SCHEMA, DOUBLE_UNION_SCHEMA, {"reader union lacking writer type: DOUBLE"}, {"/0"}),
        (INT_UNION_SCHEMA, BYTES_UNION_SCHEMA, {"reader union lacking writer type: BYTES"}, {"/0"}),
        (INT_UNION_SCHEMA, UNION_INT_ARRAY_INT, {"reader union lacking writer type: ARRAY"}, {"/1"}),
        (INT_UNION_SCHEMA, UNION_INT_MAP_INT, {"reader union lacking writer type: MAP"}, {"/1"}),
        (INT_UNION_SCHEMA, UNION_INT_NULL, {"reader union lacking writer type: NULL"}, {"/1"}),
        (
            INT_UNION_SCHEMA, INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA, {
                "reader union lacking writer type: LONG", "reader union lacking writer type: FLOAT",
                "reader union lacking writer type: DOUBLE"
            }, {"/1", "/2", "/3"}
        ),
        (
            A_DINT_B_DINT_UNION_RECORD1, A_DINT_B_DINT_STRING_UNION_RECORD1, {"reader union lacking writer type: STRING"},
            {"/fields/1/type/1"}
        ),
    ]

    for (reader, writer, message, location) in incompatible_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert result.messages == message
        assert result.locations == location


def test_schema_compatibility_name_mismatch():
    incompatible_pairs = [(ENUM1_AB_SCHEMA, ENUM2_AB_SCHEMA, "expected: Enum2", "/name"),
                          (EMPTY_RECORD2, EMPTY_RECORD1, "expected: Record1", "/name"),
                          (FIXED_4_BYTES, FIXED_4_ANOTHER_NAME, "expected: AnotherName", "/name"),
                          (A_DINT_B_DENUM_1_RECORD1, A_DINT_B_DENUM_2_RECORD1, "expected: Enum2", "/fields/1/type/name")]

    for (reader, writer, message, location) in incompatible_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert message in result.messages
        assert location in result.locations


def test_schema_compatibility_reader_field_missing_default_value():
    incompatible_pairs = [
        (A_INT_RECORD1, EMPTY_RECORD1, "a", "/fields/0"),
        (A_INT_B_DINT_RECORD1, EMPTY_RECORD1, "a", "/fields/0"),
    ]
    for (reader, writer, message, location) in incompatible_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert len(result.messages) == 1 and len(result.locations) == 1
        assert message == "".join(result.messages)
        assert location == "".join(result.locations)


def test_schema_compatibility_type_mismatch():
    incompatible_pairs = [
        (NULL_SCHEMA, INT_SCHEMA, "reader type: NULL not compatible with writer type: INT", "/"),
        (NULL_SCHEMA, LONG_SCHEMA, "reader type: NULL not compatible with writer type: LONG", "/"),
        (BOOLEAN_SCHEMA, INT_SCHEMA, "reader type: BOOLEAN not compatible with writer type: INT", "/"),
        (INT_SCHEMA, NULL_SCHEMA, "reader type: INT not compatible with writer type: NULL", "/"),
        (INT_SCHEMA, BOOLEAN_SCHEMA, "reader type: INT not compatible with writer type: BOOLEAN", "/"),
        (INT_SCHEMA, LONG_SCHEMA, "reader type: INT not compatible with writer type: LONG", "/"),
        (INT_SCHEMA, FLOAT_SCHEMA, "reader type: INT not compatible with writer type: FLOAT", "/"),
        (INT_SCHEMA, DOUBLE_SCHEMA, "reader type: INT not compatible with writer type: DOUBLE", "/"),
        (LONG_SCHEMA, FLOAT_SCHEMA, "reader type: LONG not compatible with writer type: FLOAT", "/"),
        (LONG_SCHEMA, DOUBLE_SCHEMA, "reader type: LONG not compatible with writer type: DOUBLE", "/"),
        (FLOAT_SCHEMA, DOUBLE_SCHEMA, "reader type: FLOAT not compatible with writer type: DOUBLE", "/"),
        (DOUBLE_SCHEMA, STRING_SCHEMA, "reader type: DOUBLE not compatible with writer type: STRING", "/"),
        (FIXED_4_BYTES, STRING_SCHEMA, "reader type: FIXED not compatible with writer type: STRING", "/"),
        (STRING_SCHEMA, BOOLEAN_SCHEMA, "reader type: STRING not compatible with writer type: BOOLEAN", "/"),
        (STRING_SCHEMA, INT_SCHEMA, "reader type: STRING not compatible with writer type: INT", "/"),
        (BYTES_SCHEMA, NULL_SCHEMA, "reader type: BYTES not compatible with writer type: NULL", "/"),
        (BYTES_SCHEMA, INT_SCHEMA, "reader type: BYTES not compatible with writer type: INT", "/"),
        (A_INT_RECORD1, INT_SCHEMA, "reader type: RECORD not compatible with writer type: INT", "/"),
        (INT_ARRAY_SCHEMA, LONG_ARRAY_SCHEMA, "reader type: INT not compatible with writer type: LONG", "/items"),
        (INT_MAP_SCHEMA, INT_ARRAY_SCHEMA, "reader type: MAP not compatible with writer type: ARRAY", "/"),
        (INT_ARRAY_SCHEMA, INT_MAP_SCHEMA, "reader type: ARRAY not compatible with writer type: MAP", "/"),
        (INT_MAP_SCHEMA, LONG_MAP_SCHEMA, "reader type: INT not compatible with writer type: LONG", "/values"),
        (INT_SCHEMA, ENUM2_AB_SCHEMA, "reader type: INT not compatible with writer type: ENUM", "/"),
        (ENUM2_AB_SCHEMA, INT_SCHEMA, "reader type: ENUM not compatible with writer type: INT", "/"),
        (
            FLOAT_SCHEMA, INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA, "reader type: FLOAT not compatible with writer type: DOUBLE",
            "/"
        ),
        (LONG_SCHEMA, INT_FLOAT_UNION_SCHEMA, "reader type: LONG not compatible with writer type: FLOAT", "/"),
        (INT_SCHEMA, INT_FLOAT_UNION_SCHEMA, "reader type: INT not compatible with writer type: FLOAT", "/"),
        # (INT_LIST_RECORD, LONG_LIST_RECORD, "reader type: INT not compatible with writer type: LONG", "/fields/0/type"),
        (NULL_SCHEMA, INT_SCHEMA, "reader type: NULL not compatible with writer type: INT", "/"),
    ]
    for (reader, writer, message, location) in incompatible_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert message in result.messages
        assert location in result.locations


def are_compatible(reader: Schema, writer: Schema) -> bool:
    return ReaderWriterCompatibilityChecker(
    ).get_compatibility(reader, writer).compatibility is SchemaCompatibilityType.compatible
