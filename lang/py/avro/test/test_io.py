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

import binascii
import datetime
import decimal
import io
import itertools
import json
import os
import unittest
import unittest.mock
import uuid
import warnings
from typing import BinaryIO, Collection, Dict, List, Optional, Tuple, Union, cast

import avro.io
import avro.schema
import avro.timezones
from avro.utils import TypedDict


class DefaultValueTestCaseType(TypedDict):
    H: object


SCHEMAS_TO_VALIDATE = tuple(
    (json.dumps(schema), datum)
    for schema, datum in (
        ("null", None),
        ("boolean", True),
        ("string", "adsfasdf09809dsf-=adsf"),
        ("bytes", b"12345abcd"),
        ("int", 1234),
        ("long", 1234),
        ("float", 1234.0),
        ("double", 1234.0),
        ({"type": "fixed", "name": "Test", "size": 1}, b"B"),
        (
            {
                "type": "fixed",
                "logicalType": "decimal",
                "name": "Test",
                "size": 8,
                "precision": 5,
                "scale": 4,
            },
            decimal.Decimal("3.1415"),
        ),
        (
            {
                "type": "fixed",
                "logicalType": "decimal",
                "name": "Test",
                "size": 8,
                "precision": 5,
                "scale": 4,
            },
            decimal.Decimal("-3.1415"),
        ),
        (
            {
                "type": "fixed",
                "logicalType": "decimal",
                "name": "Test",
                "size": 8,
                "precision": 1,
            },
            decimal.Decimal("3"),
        ),
        (
            {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 4},
            decimal.Decimal("3.1415"),
        ),
        (
            {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 4},
            decimal.Decimal("-3.1415"),
        ),
        (
            {"type": "bytes", "logicalType": "decimal", "precision": 1},
            decimal.Decimal("3"),
        ),
        ({"type": "enum", "name": "Test", "symbols": ["A", "B"]}, "B"),
        ({"type": "array", "items": "long"}, [1, 3, 2]),
        ({"type": "map", "values": "long"}, {"a": 1, "b": 3, "c": 2}),
        (["string", "null", "long"], None),
        ({"type": "int", "logicalType": "date"}, datetime.date(2000, 1, 1)),
        (
            {"type": "int", "logicalType": "time-millis"},
            datetime.time(23, 59, 59, 999000),
        ),
        ({"type": "int", "logicalType": "time-millis"}, datetime.time(0, 0, 0, 000000)),
        (
            {"type": "long", "logicalType": "time-micros"},
            datetime.time(23, 59, 59, 999999),
        ),
        (
            {"type": "long", "logicalType": "time-micros"},
            datetime.time(0, 0, 0, 000000),
        ),
        (
            {"type": "long", "logicalType": "timestamp-millis"},
            datetime.datetime(1000, 1, 1, 0, 0, 0, 000000, tzinfo=avro.timezones.utc),
        ),
        (
            {"type": "long", "logicalType": "timestamp-millis"},
            datetime.datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=avro.timezones.utc),
        ),
        (
            {"type": "long", "logicalType": "timestamp-millis"},
            datetime.datetime(2000, 1, 18, 2, 2, 1, 100000, tzinfo=avro.timezones.tst),
        ),
        (
            {"type": "long", "logicalType": "timestamp-micros"},
            datetime.datetime(1000, 1, 1, 0, 0, 0, 000000, tzinfo=avro.timezones.utc),
        ),
        (
            {"type": "long", "logicalType": "timestamp-micros"},
            datetime.datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=avro.timezones.utc),
        ),
        (
            {"type": "long", "logicalType": "timestamp-micros"},
            datetime.datetime(2000, 1, 18, 2, 2, 1, 123499, tzinfo=avro.timezones.tst),
        ),
        (
            {"type": "string", "logicalType": "uuid"},
            "a4818e1c-8e59-11eb-8dcd-0242ac130003",
        ),  # UUID1
        (
            {"type": "string", "logicalType": "uuid"},
            "570feebe-2bbc-4937-98df-285944e1dbbd",
        ),  # UUID4
        ({"type": "string", "logicalType": "unknown-logical-type"}, "12345abcd"),
        ({"type": "string", "logicalType": "timestamp-millis"}, "12345abcd"),
        (
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "f", "type": "long"}],
            },
            {"f": 5},
        ),
        (
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
            },
            {"value": {"car": {"value": "head"}, "cdr": {"value": None}}},
        ),
        (
            {
                "type": "record",
                "name": "record",
                "fields": [
                    {"name": "value", "type": "int"},
                    {"name": "next", "type": ["null", "record"]},
                ],
            },
            {"value": 0, "next": {"value": 1, "next": None}},
        ),
        (
            {
                "type": "record",
                "name": "ns.long",
                "fields": [
                    {"name": "value", "type": "int"},
                    {"name": "next", "type": ["null", "ns.long"]},
                ],
            },
            {"value": 0, "next": {"value": 1, "next": None}},
        ),
        # Optional logical types.
        (
            [{"logicalType": "uuid", "type": "string"}, "null"],
            None,
        ),
        (
            [{"logicalType": "uuid", "type": "string"}, "null"],
            uuid.uuid4().hex,
        ),
        (
            [{"type": "long", "logicalType": "timestamp-millis"}, "null"],
            datetime.datetime(1000, 1, 1, 0, 0, 0, 0, tzinfo=avro.timezones.utc),
        ),
        (
            [{"type": "long", "logicalType": "timestamp-millis"}, "null"],
            None,
        ),
    )
)

BINARY_ENCODINGS = (
    (0, b"00"),
    (-1, b"01"),
    (1, b"02"),
    (-2, b"03"),
    (2, b"04"),
    (-64, b"7f"),
    (64, b"80 01"),
    (8192, b"80 80 01"),
    (-8193, b"81 80 01"),
)

DEFAULT_VALUE_EXAMPLES = (
    ("null", None),
    ("boolean", True),
    ("string", "foo"),
    ("bytes", "\xff\xff"),
    ("int", 5),
    ("long", 5),
    ("float", 1.1),
    ("double", 1.1),
    ({"type": "fixed", "name": "F", "size": 2}, "\xff\xff"),
    ({"type": "enum", "name": "F", "symbols": ["FOO", "BAR"]}, "FOO"),
    ({"type": "array", "items": "int"}, [1, 2, 3]),
    ({"type": "map", "values": "int"}, {"a": 1, "b": 2}),
    (["int", "null"], 5),
    (
        {"type": "record", "name": "F", "fields": [{"name": "A", "type": "int"}]},
        {"A": 5},
    ),
)

LONG_RECORD_SCHEMA = avro.schema.parse(
    json.dumps(
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
                {"name": "G", "type": "int"},
            ],
        }
    )
)

LONG_RECORD_DATUM = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6, "G": 7}


def avro_hexlify(reader: BinaryIO) -> bytes:
    """Return the hex value, as a string, of a binary-encoded int or long."""
    b = []
    current_byte = reader.read(1)
    b.append(binascii.hexlify(current_byte))
    while (ord(current_byte) & 0x80) != 0:
        current_byte = reader.read(1)
        b.append(binascii.hexlify(current_byte))
    return b" ".join(b)


def write_datum(datum: object, writers_schema: avro.schema.Schema) -> Tuple[io.BytesIO, avro.io.BinaryEncoder, avro.io.DatumWriter]:
    writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(writer)
    datum_writer = avro.io.DatumWriter(writers_schema)
    datum_writer.write(datum, encoder)
    return writer, encoder, datum_writer


def read_datum(
    buffer: io.BytesIO,
    writers_schema: avro.schema.Schema,
    readers_schema: Optional[avro.schema.Schema] = None,
) -> object:
    reader = io.BytesIO(buffer.getvalue())
    decoder = avro.io.BinaryDecoder(reader)
    datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
    return datum_reader.read(decoder)


class IoValidateTestCase(unittest.TestCase):
    def __init__(self, test_schema: str, test_datum: object) -> None:
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("io_valid")
        self.test_schema = avro.schema.parse(test_schema)
        self.test_datum = test_datum
        # Never hide repeated warnings when running this test case.
        warnings.simplefilter("always")

    def io_valid(self) -> None:
        """
        In these cases, the provided data should be valid with the given schema.
        """
        with warnings.catch_warnings(record=True) as _actual_warnings:
            self.assertTrue(
                avro.io.validate(self.test_schema, self.test_datum),
                f"{self.test_datum} did not validate in the schema {self.test_schema}",
            )


class RoundTripTestCase(unittest.TestCase):
    def __init__(self, test_schema: str, test_datum: object) -> None:
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("io_round_trip")
        self.test_schema = avro.schema.parse(test_schema)
        self.test_datum = test_datum
        # Never hide repeated warnings when running this test case.
        warnings.simplefilter("always")

    def io_round_trip(self) -> None:
        """
        A datum should be the same after being encoded and then decoded.
        """
        with warnings.catch_warnings(record=True) as _actual_warnings:
            writer, encoder, datum_writer = write_datum(self.test_datum, self.test_schema)
            round_trip_datum = read_datum(writer, self.test_schema)
            expected: object
            round_trip: object
            if isinstance(round_trip_datum, decimal.Decimal):
                expected, round_trip, message = (
                    str(self.test_datum),
                    round_trip_datum.to_eng_string(),
                    "Decimal datum changed value after encode and decode",
                )
            elif isinstance(round_trip_datum, datetime.datetime):
                expected, round_trip, message = (
                    cast(datetime.datetime, self.test_datum).astimezone(tz=avro.timezones.utc),
                    round_trip_datum,
                    "DateTime datum changed value after encode and decode",
                )
            else:
                expected, round_trip, message = (
                    self.test_datum,
                    round_trip_datum,
                    "Datum changed value after encode and decode",
                )
            self.assertEqual(expected, round_trip, message)


class BinaryEncodingTestCase(unittest.TestCase):
    def __init__(self, skip: bool, test_type: str, test_datum: object, test_hex: bytes) -> None:
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__(f"check_{'skip' if skip else 'binary'}_encoding")
        self.writers_schema = avro.schema.parse(f'"{test_type}"')
        self.test_datum = test_datum
        self.test_hex = test_hex
        # Never hide repeated warnings when running this test case.
        warnings.simplefilter("always")

    def check_binary_encoding(self) -> None:
        with warnings.catch_warnings(record=True) as _actual_warnings:
            writer, encoder, datum_writer = write_datum(self.test_datum, self.writers_schema)
            writer.seek(0)
            hex_val = avro_hexlify(writer)
            self.assertEqual(
                self.test_hex,
                hex_val,
                "Binary encoding did not match expected hex representation.",
            )

    def check_skip_encoding(self) -> None:
        VALUE_TO_READ = 6253
        with warnings.catch_warnings(record=True) as _actual_warnings:
            # write the value to skip and a known value
            writer, encoder, datum_writer = write_datum(self.test_datum, self.writers_schema)
            datum_writer.write(VALUE_TO_READ, encoder)

            # skip the value
            reader = io.BytesIO(writer.getvalue())
            decoder = avro.io.BinaryDecoder(reader)
            decoder.skip_long()

            # read data from string buffer
            datum_reader = avro.io.DatumReader(self.writers_schema)
            read_value = datum_reader.read(decoder)

            self.assertEqual(
                read_value,
                VALUE_TO_READ,
                "Unexpected value after skipping a binary encoded value.",
            )


class SchemaPromotionTestCase(unittest.TestCase):
    def __init__(self, write_type: str, read_type: str) -> None:
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("check_schema_promotion")
        self.writers_schema = avro.schema.parse(f'"{write_type}"')
        self.readers_schema = avro.schema.parse(f'"{read_type}"')
        # Never hide repeated warnings when running this test case.
        warnings.simplefilter("always")

    def check_schema_promotion(self) -> None:
        """Test schema promotion"""
        # note that checking writers_schema.type in read_data
        # allows us to handle promotion correctly
        DATUM_TO_WRITE = 219
        with warnings.catch_warnings(record=True) as _actual_warnings:
            writer, enc, dw = write_datum(DATUM_TO_WRITE, self.writers_schema)
            datum_read = read_datum(writer, self.writers_schema, self.readers_schema)
            self.assertEqual(
                datum_read,
                DATUM_TO_WRITE,
                f"Datum changed between schema that were supposed to promote: writer: {self.writers_schema} reader: {self.readers_schema}.",
            )


class DefaultValueTestCase(unittest.TestCase):
    def __init__(
        self,
        field_type: Collection[str],
        default: Union[Dict[str, int], List[int], None, float, str],
    ) -> None:
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("check_default_value")
        self.field_type = field_type
        self.default = default
        # Never hide repeated warnings when running this test case.
        warnings.simplefilter("always")

    def check_default_value(self) -> None:
        datum_read: DefaultValueTestCaseType
        with warnings.catch_warnings(record=True) as _actual_warnings:
            datum_to_read = cast(DefaultValueTestCaseType, {"H": self.default})
            readers_schema = avro.schema.parse(
                json.dumps(
                    {
                        "type": "record",
                        "name": "Test",
                        "fields": [
                            {
                                "name": "H",
                                "type": self.field_type,
                                "default": self.default,
                            }
                        ],
                    }
                )
            )
            writer, _, _ = write_datum(LONG_RECORD_DATUM, LONG_RECORD_SCHEMA)
            datum_read_ = cast(
                DefaultValueTestCaseType,
                read_datum(writer, LONG_RECORD_SCHEMA, readers_schema),
            )
            datum_read = {"H": cast(bytes, datum_read_["H"]).decode()} if isinstance(datum_read_["H"], bytes) else datum_read_
            self.assertEqual(datum_to_read, datum_read)


class TestIncompatibleSchemaReading(unittest.TestCase):
    def test_deserialization_fails(self) -> None:
        reader_schema = avro.schema.parse(
            json.dumps(
                {
                    "namespace": "example.avro",
                    "type": "record",
                    "name": "User",
                    "fields": [
                        {"name": "name", "type": "string"},
                        {"name": "age", "type": "int"},
                        {"name": "location", "type": "string"},
                    ],
                }
            )
        )
        writer_schema = avro.schema.parse(
            json.dumps(
                {
                    "namespace": "example.avro",
                    "type": "record",
                    "name": "IncompatibleUser",
                    "fields": [
                        {"name": "name", "type": "int"},
                        {"name": "age", "type": "int"},
                        {"name": "location", "type": "string"},
                    ],
                }
            )
        )

        incompatibleUserRecord = {"name": 100, "age": 21, "location": "Woodford"}
        writer = avro.io.DatumWriter(writer_schema)
        with io.BytesIO() as writer_bio:
            enc = avro.io.BinaryEncoder(writer_bio)
            writer.write(incompatibleUserRecord, enc)
            enc_bytes = writer_bio.getvalue()
        reader = avro.io.DatumReader(reader_schema)
        with io.BytesIO(enc_bytes) as reader_bio:
            self.assertRaises(
                avro.errors.InvalidAvroBinaryEncoding,
                reader.read,
                avro.io.BinaryDecoder(reader_bio),
            )

        incompatibleUserRecord = {"name": -10, "age": 21, "location": "Woodford"}
        with io.BytesIO() as writer_bio:
            enc = avro.io.BinaryEncoder(writer_bio)
            writer.write(incompatibleUserRecord, enc)
            enc_bytes = writer_bio.getvalue()
        reader = avro.io.DatumReader(reader_schema)
        with io.BytesIO(enc_bytes) as reader_bio:
            self.assertRaises(
                avro.errors.InvalidAvroBinaryEncoding,
                reader.read,
                avro.io.BinaryDecoder(reader_bio),
            )


class TestBinaryDecoderAvailableBytes(unittest.TestCase):
    """A bytes/string value declares a length prefix; a malicious or truncated
    input can declare far more bytes than actually exist. On a seekable reader
    that is rejected before allocating for it."""

    @staticmethod
    def _encode_length_prefix(length: int) -> bytes:
        buf = io.BytesIO()
        avro.io.BinaryEncoder(buf).write_long(length)
        return buf.getvalue()

    def test_read_rejects_length_beyond_stream(self) -> None:
        # Declares 100 MiB but provides no data.
        prefix = self._encode_length_prefix(100 * 1024 * 1024)
        with io.BytesIO(prefix) as bio:
            decoder = avro.io.BinaryDecoder(bio)
            self.assertRaises(avro.errors.InvalidAvroBinaryEncoding, decoder.read_bytes)

    def test_read_within_stream_still_reads(self) -> None:
        # A well-formed large value whose data is actually present still reads.
        payload = b"x" * (2 * 1024 * 1024)
        buf = io.BytesIO()
        avro.io.BinaryEncoder(buf).write_bytes(payload)
        with io.BytesIO(buf.getvalue()) as bio:
            decoder = avro.io.BinaryDecoder(bio)
            self.assertEqual(decoder.read_bytes(), payload)

    def test_bytes_remaining_restores_position(self) -> None:
        # bytes_remaining() must leave the reader position unchanged.
        with io.BytesIO(b"abcdefghij") as bio:
            bio.seek(3)
            decoder = avro.io.BinaryDecoder(bio)
            self.assertEqual(decoder.bytes_remaining(), 7)
            self.assertEqual(bio.tell(), 3)

    def test_bytes_remaining_restores_position_on_error(self) -> None:
        # If reading the end offset fails after seeking, the original position
        # must still be restored (via the finally block).
        class FailingEndStream(io.BytesIO):
            def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
                # Fail only when seeking to the end.
                if whence == os.SEEK_END:
                    raise OSError("cannot seek to end")
                return super().seek(offset, whence)

        stream = FailingEndStream(b"abcdefghij")
        stream.seek(4)
        decoder = avro.io.BinaryDecoder(stream)
        self.assertIsNone(decoder.bytes_remaining())
        self.assertEqual(stream.tell(), 4)

    def test_read_non_seekable_falls_back(self) -> None:
        # A non-seekable reader skips the pre-check, but the post-read length
        # check still rejects a truncated oversized value.
        prefix = self._encode_length_prefix(100 * 1024 * 1024)

        class NonSeekable:
            def __init__(self, data: bytes) -> None:
                self._bio = io.BytesIO(data)

            def read(self, n: int = -1) -> bytes:
                return self._bio.read(n)

            def seekable(self) -> bool:
                return False

        decoder = avro.io.BinaryDecoder(NonSeekable(prefix))  # type: ignore[arg-type]
        self.assertRaises(avro.errors.InvalidAvroBinaryEncoding, decoder.read_bytes)


class TestDatumReaderCollectionAvailableBytes(unittest.TestCase):
    """An array/map block declares an element count; a malicious or truncated
    input can declare far more elements than the remaining bytes could hold.
    The count is validated against the bytes remaining before iterating, using
    the minimum on-wire size of the element schema (so 0-byte elements such as
    ``null`` are not falsely rejected)."""

    @staticmethod
    def _decode(schema_json: str, encoded: bytes) -> object:
        schema = avro.schema.parse(schema_json)
        reader = avro.io.DatumReader(schema)
        with io.BytesIO(encoded) as bio:
            return reader.read(avro.io.BinaryDecoder(bio))

    def test_array_rejects_count_beyond_stream(self) -> None:
        # A long array block count (1,000,000 long elements) with no element
        # data following it.
        buf = io.BytesIO()
        avro.io.BinaryEncoder(buf).write_long(1_000_000)
        self.assertRaises(
            avro.errors.InvalidAvroBinaryEncoding,
            self._decode,
            '{"type": "array", "items": "long"}',
            buf.getvalue(),
        )

    def test_map_rejects_count_beyond_stream(self) -> None:
        buf = io.BytesIO()
        avro.io.BinaryEncoder(buf).write_long(1_000_000)
        self.assertRaises(
            avro.errors.InvalidAvroBinaryEncoding,
            self._decode,
            '{"type": "map", "values": "long"}',
            buf.getvalue(),
        )

    def test_array_of_null_not_falsely_rejected(self) -> None:
        # null elements occupy zero bytes, so a large count is legitimate and
        # must not be rejected.
        count = 100_000
        buf = io.BytesIO()
        enc = avro.io.BinaryEncoder(buf)
        enc.write_long(count)  # one block of `count` nulls
        enc.write_long(0)  # end-of-array marker
        result = self._decode('{"type": "array", "items": "null"}', buf.getvalue())
        self.assertEqual(result, [None] * count)

    def test_array_within_stream_still_reads(self) -> None:
        schema_json = '{"type": "array", "items": "long"}'
        schema = avro.schema.parse(schema_json)
        buf = io.BytesIO()
        writer = avro.io.DatumWriter(schema)
        writer.write([1, 2, 3], avro.io.BinaryEncoder(buf))
        self.assertEqual(self._decode(schema_json, buf.getvalue()), [1, 2, 3])


class TestMisc(unittest.TestCase):
    def test_decimal_bytes_small_scale(self) -> None:
        """Avro should raise an AvroTypeException when attempting to write a decimal with a larger exponent than the schema's scale."""
        datum = decimal.Decimal("3.1415")
        _, _, exp = datum.as_tuple()
        scale = -1 * int(exp) - 1
        schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 5,
                    "scale": scale,
                }
            )
        )
        self.assertRaises(avro.errors.AvroOutOfScaleException, write_datum, datum, schema)

    def test_decimal_fixed_small_scale(self) -> None:
        """Avro should raise an AvroTypeException when attempting to write a decimal with a larger exponent than the schema's scale."""
        datum = decimal.Decimal("3.1415")
        _, _, exp = datum.as_tuple()
        scale = -1 * int(exp) - 1
        schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "fixed",
                    "logicalType": "decimal",
                    "name": "Test",
                    "size": 8,
                    "precision": 5,
                    "scale": scale,
                }
            )
        )
        self.assertRaises(avro.errors.AvroOutOfScaleException, write_datum, datum, schema)

    def test_unknown_symbol(self) -> None:
        datum_to_write = "FOO"
        writers_schema = avro.schema.parse(json.dumps({"type": "enum", "name": "Test", "symbols": ["FOO", "BAR"]}))
        readers_schema = avro.schema.parse(json.dumps({"type": "enum", "name": "Test", "symbols": ["BAR", "BAZ"]}))

        writer, encoder, datum_writer = write_datum(datum_to_write, writers_schema)
        reader = io.BytesIO(writer.getvalue())
        decoder = avro.io.BinaryDecoder(reader)
        datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
        self.assertRaises(avro.errors.SchemaResolutionException, datum_reader.read, decoder)

    def test_union_index_out_of_range(self) -> None:
        # A union branch index that is negative or >= the number of branches is
        # malformed and must be rejected before indexing (a negative index would
        # otherwise wrap in Python and silently select the wrong branch).
        schema = avro.schema.parse(json.dumps(["null", "long"]))
        datum_reader = avro.io.DatumReader(schema)
        for encoded in (b"\x0a", b"\x01"):  # zig-zag long 5, and -1
            decoder = avro.io.BinaryDecoder(io.BytesIO(encoded))
            self.assertRaises(avro.errors.SchemaResolutionException, datum_reader.read, decoder)

    def test_enum_index_out_of_range(self) -> None:
        # An enum symbol index that is negative or >= the number of symbols is
        # malformed and must be rejected before indexing.
        schema = avro.schema.parse(json.dumps({"type": "enum", "name": "E", "symbols": ["A", "B"]}))
        datum_reader = avro.io.DatumReader(schema)
        for encoded in (b"\x12", b"\x01"):  # zig-zag int 9, and -1
            decoder = avro.io.BinaryDecoder(io.BytesIO(encoded))
            self.assertRaises(avro.errors.SchemaResolutionException, datum_reader.read, decoder)

    def test_read_long_rejects_overlong_varint(self) -> None:
        # A 64-bit value uses at most 10 bytes; an 11th continuation byte is
        # malformed and must be rejected rather than accepted as an arbitrarily
        # large integer.
        encoded = b"\x80" * 10 + b"\x01"
        decoder = avro.io.BinaryDecoder(io.BytesIO(encoded))
        self.assertRaises(avro.errors.InvalidAvroBinaryEncoding, decoder.read_long)

    def test_no_default_value(self) -> None:
        writers_schema = LONG_RECORD_SCHEMA
        datum_to_write = LONG_RECORD_DATUM

        readers_schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "Test",
                    "fields": [{"name": "H", "type": "int"}],
                }
            )
        )

        writer, encoder, datum_writer = write_datum(datum_to_write, writers_schema)
        reader = io.BytesIO(writer.getvalue())
        decoder = avro.io.BinaryDecoder(reader)
        datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
        self.assertRaises(avro.errors.SchemaResolutionException, datum_reader.read, decoder)

    def test_projection(self) -> None:
        writers_schema = LONG_RECORD_SCHEMA
        datum_to_write = LONG_RECORD_DATUM

        readers_schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "Test",
                    "fields": [
                        {"name": "E", "type": "int"},
                        {"name": "F", "type": "int"},
                    ],
                }
            )
        )
        datum_to_read = {"E": 5, "F": 6}

        writer, encoder, datum_writer = write_datum(datum_to_write, writers_schema)
        datum_read = read_datum(writer, writers_schema, readers_schema)
        self.assertEqual(datum_to_read, datum_read)

    def test_field_order(self) -> None:
        writers_schema = LONG_RECORD_SCHEMA
        datum_to_write = LONG_RECORD_DATUM

        readers_schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "Test",
                    "fields": [
                        {"name": "F", "type": "int"},
                        {"name": "E", "type": "int"},
                    ],
                }
            )
        )
        datum_to_read = {"E": 5, "F": 6}

        writer, encoder, datum_writer = write_datum(datum_to_write, writers_schema)
        datum_read = read_datum(writer, writers_schema, readers_schema)
        self.assertEqual(datum_to_read, datum_read)

    def test_type_exception_int(self) -> None:
        writers_schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "Test",
                    "fields": [
                        {"name": "F", "type": "int"},
                        {"name": "E", "type": "int"},
                    ],
                }
            )
        )
        datum_to_write = {"E": 5, "F": "Bad"}
        with self.assertRaises(avro.errors.AvroTypeException) as exc:
            write_datum(datum_to_write, writers_schema)
        assert str(exc.exception) == 'The datum "Bad" provided for "F" is not an example of the schema "int"'

    def test_type_exception_long(self) -> None:
        writers_schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "Test",
                    "fields": [{"name": "foo", "type": "long"}],
                }
            )
        )
        datum_to_write = {"foo": 5.0}

        with self.assertRaises(avro.errors.AvroTypeException) as exc:
            write_datum(datum_to_write, writers_schema)
        assert str(exc.exception) == 'The datum "5.0" provided for "foo" is not an example of the schema "long"'

    def test_type_exception_record(self) -> None:
        writers_schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "Test",
                    "fields": [{"name": "foo", "type": "long"}],
                }
            )
        )
        datum_to_write = ("foo", 5.0)

        with self.assertRaisesRegex(
            avro.errors.AvroTypeException,
            r"The datum \".*\" provided for \".*\" is not an example of the schema [\s\S]*",
        ):
            write_datum(datum_to_write, writers_schema)


class TestDatumReaderCollectionSizeLimit(unittest.TestCase):
    """Elements whose schema encodes to zero bytes (``null``, a zero-length
    ``fixed``, or a record with only zero-byte fields) consume no input, so the
    bytes-remaining check cannot bound them. A huge declared block count of such
    elements is capped so a tiny payload cannot exhaust memory. The limit is
    configurable via the ``AVRO_MAX_COLLECTION_ITEMS`` environment variable."""

    @staticmethod
    def _decode(schema_json: str, encoded: bytes) -> object:
        schema = avro.schema.parse(schema_json)
        reader = avro.io.DatumReader(schema)
        with io.BytesIO(encoded) as bio:
            return reader.read(avro.io.BinaryDecoder(bio))

    @staticmethod
    def _skip(schema_json: str, encoded: bytes) -> None:
        schema = avro.schema.parse(schema_json)
        reader = avro.io.DatumReader(schema)
        with io.BytesIO(encoded) as bio:
            reader.skip_data(schema, avro.io.BinaryDecoder(bio))

    @staticmethod
    def _array_block(count: int, *, negative: bool = False) -> bytes:
        """One array/map block of ``count`` zero-byte elements + end marker."""
        buf = io.BytesIO()
        enc = avro.io.BinaryEncoder(buf)
        if negative:
            enc.write_long(-count)  # negative count is followed by a block byte-size
            enc.write_long(0)
        else:
            enc.write_long(count)
        enc.write_long(0)  # end-of-collection marker
        return buf.getvalue()

    def test_array_of_null_exceeds_default_limit(self) -> None:
        # The reported exploit: a ~6 byte payload declaring 200,000,000 nulls.
        self.assertRaises(
            avro.errors.AvroCollectionSizeException,
            self._decode,
            '{"type": "array", "items": "null"}',
            self._array_block(200_000_000),
        )

    def test_array_of_null_int64_min_block_count(self) -> None:
        # INT64_MIN as a block count is the pathological negation case. Python
        # integers do not overflow, so negating it yields 2**63, which the cap
        # rejects. INT64_MIN zig-zag encodes as the 10-byte varint below,
        # followed by a block byte-size (0) that the negative-block path reads.
        payload = b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01\x00"
        self.assertRaises(
            avro.errors.AvroCollectionSizeException,
            self._decode,
            '{"type": "array", "items": "null"}',
            payload,
        )

    def test_array_of_null_within_configured_limit_still_reads(self) -> None:
        with unittest.mock.patch.dict(os.environ, {"AVRO_MAX_COLLECTION_ITEMS": "1000"}):
            result = self._decode('{"type": "array", "items": "null"}', self._array_block(1000))
        self.assertEqual(result, [None] * 1000)

    def test_array_of_null_exceeds_configured_limit(self) -> None:
        with unittest.mock.patch.dict(os.environ, {"AVRO_MAX_COLLECTION_ITEMS": "1000"}):
            self.assertRaises(
                avro.errors.AvroCollectionSizeException,
                self._decode,
                '{"type": "array", "items": "null"}',
                self._array_block(1001),
            )

    def test_array_of_null_cumulative_across_blocks(self) -> None:
        # Two blocks of 600 nulls each (1200 > 1000) must be rejected on the second.
        buf = io.BytesIO()
        enc = avro.io.BinaryEncoder(buf)
        enc.write_long(600)
        enc.write_long(600)
        enc.write_long(0)
        with unittest.mock.patch.dict(os.environ, {"AVRO_MAX_COLLECTION_ITEMS": "1000"}):
            self.assertRaises(
                avro.errors.AvroCollectionSizeException,
                self._decode,
                '{"type": "array", "items": "null"}',
                buf.getvalue(),
            )

    def test_map_duplicate_keys_counted_cumulatively(self) -> None:
        # Two blocks of 600 pairs that repeat the SAME key: len(read_items) would
        # be 1, so a separate decoded-pair counter is needed to reject 1200 > 1000.
        buf = io.BytesIO()
        enc = avro.io.BinaryEncoder(buf)
        for _ in range(2):
            enc.write_long(600)
            for _ in range(600):
                enc.write_utf8("k")  # same key; value is null (zero bytes)
        enc.write_long(0)
        with unittest.mock.patch.dict(os.environ, {"AVRO_MAX_COLLECTION_ITEMS": "1000"}):
            self.assertRaises(
                avro.errors.AvroCollectionSizeException,
                self._decode,
                '{"type": "map", "values": "null"}',
                buf.getvalue(),
            )

    def test_array_of_null_negative_block_count(self) -> None:
        # A negative count encodes abs(count) elements preceded by a block size;
        # after normalization it must still be bounded.
        with unittest.mock.patch.dict(os.environ, {"AVRO_MAX_COLLECTION_ITEMS": "1000"}):
            self.assertRaises(
                avro.errors.AvroCollectionSizeException,
                self._decode,
                '{"type": "array", "items": "null"}',
                self._array_block(200_000, negative=True),
            )

    def test_array_of_zero_length_fixed_exceeds_limit(self) -> None:
        schema_json = '{"type": "array", "items": {"type": "fixed", "name": "empty", "size": 0}}'
        with unittest.mock.patch.dict(os.environ, {"AVRO_MAX_COLLECTION_ITEMS": "1000"}):
            self.assertRaises(avro.errors.AvroCollectionSizeException, self._decode, schema_json, self._array_block(2000))

    def test_array_of_all_null_record_exceeds_limit(self) -> None:
        schema_json = '{"type": "array", "items": {"type": "record", "name": "R", "fields": [{"name": "n", "type": "null"}]}}'
        with unittest.mock.patch.dict(os.environ, {"AVRO_MAX_COLLECTION_ITEMS": "1000"}):
            self.assertRaises(avro.errors.AvroCollectionSizeException, self._decode, schema_json, self._array_block(2000))

    def test_map_of_null_rejected_by_available_bytes(self) -> None:
        # Map entries always carry a >= 1 byte key, so a huge map<null> is bounded
        # by the bytes-remaining check rather than the zero-byte cap.
        self.assertRaises(
            avro.errors.InvalidAvroBinaryEncoding,
            self._decode,
            '{"type": "map", "values": "null"}',
            self._array_block(200_000_000),
        )

    def test_skip_array_of_null_respects_limit(self) -> None:
        with unittest.mock.patch.dict(os.environ, {"AVRO_MAX_COLLECTION_ITEMS": "1000"}):
            self.assertRaises(
                avro.errors.AvroCollectionSizeException,
                self._skip,
                '{"type": "array", "items": "null"}',
                self._array_block(2000),
            )

    def test_skip_array_of_null_negative_block_respects_limit(self) -> None:
        # A negative (byte-sized) block count must also be bounded when skipping,
        # so it cannot be used to bypass the collection cap during resolution.
        with unittest.mock.patch.dict(os.environ, {"AVRO_MAX_COLLECTION_ITEMS": "1000"}):
            self.assertRaises(
                avro.errors.AvroCollectionSizeException,
                self._skip,
                '{"type": "array", "items": "null"}',
                self._array_block(2000, negative=True),
            )

    def test_skip_block_bytes_wraps_seek_error(self) -> None:
        # A sized (negative-count) skip block seeks by the declared byte size. If
        # the underlying reader rejects the seek, surface it as an Avro decoding
        # error rather than leaking a raw OSError/ValueError/OverflowError, or an
        # AttributeError/TypeError from a reader lacking seek()/tell().
        for exc in (OSError, ValueError, OverflowError, AttributeError, TypeError):

            class FailingSkipDecoder:
                def bytes_remaining(self) -> None:
                    return None

                def skip(self, n: int, _exc: type = exc) -> None:
                    raise _exc("cannot skip")

            self.assertRaises(
                avro.errors.InvalidAvroBinaryEncoding,
                avro.io.DatumReader._skip_block_bytes,
                cast(avro.io.BinaryDecoder, FailingSkipDecoder()),
                10,  # block_size
                5,  # block_count
                0,  # min_bytes (zero-byte element, so the lower-bound check is skipped)
            )

    def test_invalid_env_override_falls_back_to_default(self) -> None:
        with unittest.mock.patch.dict(os.environ, {"AVRO_MAX_COLLECTION_ITEMS": "not-a-number"}):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                self.assertEqual(avro.io._max_collection_items(), avro.io.DEFAULT_MAX_COLLECTION_ITEMS)

    def test_negative_env_override_falls_back_to_default(self) -> None:
        with unittest.mock.patch.dict(os.environ, {"AVRO_MAX_COLLECTION_ITEMS": "-5"}):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                self.assertEqual(avro.io._max_collection_items(), avro.io.DEFAULT_MAX_COLLECTION_ITEMS)

    def test_env_override_is_honored(self) -> None:
        with unittest.mock.patch.dict(os.environ, {"AVRO_MAX_COLLECTION_ITEMS": "7"}):
            self.assertEqual(avro.io._max_collection_items(), 7)
            result = self._decode('{"type": "array", "items": "null"}', self._array_block(7))
        self.assertEqual(result, [None] * 7)

    def test_collection_limits_env_caps_both(self) -> None:
        # When set, AVRO_MAX_COLLECTION_ITEMS caps both the zero-byte and the
        # structural limit; unset, they differ (tighter zero-byte default).
        with unittest.mock.patch.dict(os.environ, {"AVRO_MAX_COLLECTION_ITEMS": "1234"}):
            self.assertEqual(avro.io._collection_limits(), (1234, 1234))
        # Assert the unset behavior inside a patch that snapshots os.environ, so
        # the test never mutates the real process environment.
        with unittest.mock.patch.dict(os.environ):
            os.environ.pop("AVRO_MAX_COLLECTION_ITEMS", None)
            self.assertEqual(
                avro.io._collection_limits(),
                (avro.io.DEFAULT_MAX_COLLECTION_ITEMS, avro.io.DEFAULT_MAX_COLLECTION_STRUCTURAL),
            )

    def test_non_zero_collection_bounded_by_structural_cap_when_unseekable(self) -> None:
        # On a non-seekable reader the bytes-remaining check cannot run, so a huge
        # non-zero-byte collection is bounded by the structural cap instead.
        class NoTell:
            def __init__(self, data: bytes) -> None:
                self._b = io.BytesIO(data)

            def read(self, n: int = -1) -> bytes:
                return self._b.read(n)

        schema = avro.schema.parse('{"type": "array", "items": "long"}')
        reader = avro.io.DatumReader(schema)
        with unittest.mock.patch.dict(os.environ, {"AVRO_MAX_COLLECTION_ITEMS": "1000"}):
            decoder = avro.io.BinaryDecoder(cast(BinaryIO, NoTell(self._array_block(2000))))
            self.assertIsNone(decoder.bytes_remaining())
            self.assertRaises(avro.errors.AvroCollectionSizeException, reader.read, decoder)


def load_tests(loader: unittest.TestLoader, default_tests: None, pattern: None) -> unittest.TestSuite:
    """Generate test cases across many test schema."""
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestMisc))
    suite.addTests(IoValidateTestCase(schema_str, datum) for schema_str, datum in SCHEMAS_TO_VALIDATE)
    suite.addTests(RoundTripTestCase(schema_str, datum) for schema_str, datum in SCHEMAS_TO_VALIDATE)
    for skip in False, True:
        for type_ in "int", "long":
            suite.addTests(BinaryEncodingTestCase(skip, type_, datum, hex_) for datum, hex_ in BINARY_ENCODINGS)
    suite.addTests(
        SchemaPromotionTestCase(write_type, read_type) for write_type, read_type in itertools.combinations(("int", "long", "float", "double"), 2)
    )
    suite.addTests(DefaultValueTestCase(field_type, default) for field_type, default in DEFAULT_VALUE_EXAMPLES)
    suite.addTests(loader.loadTestsFromTestCase(TestIncompatibleSchemaReading))
    suite.addTests(loader.loadTestsFromTestCase(TestBinaryDecoderAvailableBytes))
    suite.addTests(loader.loadTestsFromTestCase(TestDatumReaderCollectionAvailableBytes))
    suite.addTests(loader.loadTestsFromTestCase(TestDatumReaderCollectionSizeLimit))
    return suite


if __name__ == "__main__":  # pragma: no coverage
    unittest.main()
