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

"""AVRO-4302: bound decode recursion depth to prevent stack exhaustion."""

import io
import json
import unittest

import avro.errors
import avro.io
import avro.schema


def _encode_long(value: int) -> bytes:
    """Zig-zag + varint encode a long, matching BinaryEncoder.write_long."""
    datum = (value << 1) ^ (value >> 63)
    out = bytearray()
    while (datum & ~0x7F) != 0:
        out.append((datum & 0x7F) | 0x80)
        datum >>= 7
    out.append(datum)
    return bytes(out)


class TestDecodeRecursionDepth(unittest.TestCase):
    # A self-referencing linked-list schema: the classic recursion-bomb shape.
    NODE = avro.schema.parse(json.dumps({"type": "record", "name": "Node", "fields": [{"name": "next", "type": ["null", "Node"]}]}))

    @staticmethod
    def _linked_list(depth: int) -> bytes:
        """Encode a Node linked list nested ``depth`` levels deep.

        Each level selects the ``Node`` union branch (index 1); the final level
        selects ``null`` (index 0) to terminate. ~1 byte per level.
        """
        return _encode_long(1) * depth + _encode_long(0)

    def _read(self, data: bytes) -> object:
        reader = avro.io.DatumReader(self.NODE, self.NODE)
        return reader.read(avro.io.BinaryDecoder(io.BytesIO(data)))

    def test_deeply_nested_input_rejected_with_bounded_error(self) -> None:
        # ~100k levels: far beyond the default depth limit and enough to overflow
        # the stack if it were left unbounded, yet only ~100kB of input. Must fail
        # with a bounded AvroException rather than a RecursionError / crash.
        bomb = self._linked_list(100_000)
        self.assertRaises(avro.errors.AvroException, self._read, bomb)

    def test_moderately_nested_input_within_limit_still_decodes(self) -> None:
        # Two structural descents (union + record) are counted per list level, so
        # keep the level count well under half the default limit.
        result = self._read(self._linked_list(20))
        self.assertIsInstance(result, dict)

    def test_custom_depth_limit_env_is_honored(self) -> None:
        import os

        os.environ[avro.io.MAX_DECODE_DEPTH_ENV] = "6"
        try:
            # 6 allows only 3 list levels (2 descents each); 10 levels must fail.
            self.assertRaises(avro.errors.AvroException, self._read, self._linked_list(10))
        finally:
            del os.environ[avro.io.MAX_DECODE_DEPTH_ENV]


if __name__ == "__main__":
    unittest.main()
