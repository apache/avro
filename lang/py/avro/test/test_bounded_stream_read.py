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

"""AVRO-4303: bound bytes/string allocation from a length prefix on a stream."""

import io
import unittest

import avro.errors
import avro.io


def _encode_long(value: int) -> bytes:
    """Zig-zag + varint encode a long, matching BinaryEncoder.write_long."""
    datum = (value << 1) ^ (value >> 63)
    out = bytearray()
    while (datum & ~0x7F) != 0:
        out.append((datum & 0x7F) | 0x80)
        datum >>= 7
    out.append(datum)
    return bytes(out)


class NonSeekable:
    """A minimal non-seekable, tell-less stream wrapper (socket/pipe-like)."""

    def __init__(self, data: bytes) -> None:
        self._bio = io.BytesIO(data)

    def read(self, n: int = -1) -> bytes:
        return self._bio.read(n)

    def seekable(self) -> bool:
        return False


class TestBoundedStreamRead(unittest.TestCase):
    # A near-2GB declared length a single up-front allocation could not satisfy,
    # so reaching a bounded decode error proves no full allocation was attempted.
    HUGE_LENGTH = (1 << 31) - 1 - 8

    @staticmethod
    def _length_prefixed(declared: int, payload: bytes) -> bytes:
        return _encode_long(declared) + payload

    def test_huge_bytes_length_on_stream_rejected_without_huge_allocation(self) -> None:
        data = self._length_prefixed(self.HUGE_LENGTH, b"\x01\x02\x03\x04\x05")
        decoder = avro.io.BinaryDecoder(NonSeekable(data))  # type: ignore[arg-type]
        self.assertRaises(avro.errors.InvalidAvroBinaryEncoding, decoder.read_bytes)

    def test_huge_string_length_on_stream_rejected_without_huge_allocation(self) -> None:
        data = self._length_prefixed(self.HUGE_LENGTH, b"abc")
        decoder = avro.io.BinaryDecoder(NonSeekable(data))  # type: ignore[arg-type]
        self.assertRaises(avro.errors.InvalidAvroBinaryEncoding, decoder.read_utf8)

    def test_legitimate_large_bytes_round_trips_on_stream(self) -> None:
        # Larger than the per-chunk bound so it exercises the chunked-read path,
        # but a genuinely present payload must still decode intact.
        payload = bytes((i & 0xFF) for i in range(2 * 1024 * 1024))
        data = self._length_prefixed(len(payload), payload)
        decoder = avro.io.BinaryDecoder(NonSeekable(data))  # type: ignore[arg-type]
        self.assertEqual(decoder.read_bytes(), payload)


if __name__ == "__main__":
    unittest.main()
