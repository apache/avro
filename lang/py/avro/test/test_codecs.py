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

"""Tests that data-file block decompression enforces a maximum output size.

A block with a very high compression ratio (or a malformed block) can expand to
far more memory than its compressed size; these tests ensure that decompressing
such a block raises an error instead of allocating without bound.
"""

import io
import os
import unittest
from typing import Type

import avro.codecs
import avro.errors
import avro.io


class TestDecompressionSizeLimit(unittest.TestCase):
    LIMIT = 1024

    def setUp(self) -> None:
        self._previous = os.environ.get(avro.codecs.MAX_DECOMPRESS_LENGTH_ENV)
        os.environ[avro.codecs.MAX_DECOMPRESS_LENGTH_ENV] = str(self.LIMIT)

    def tearDown(self) -> None:
        if self._previous is None:
            os.environ.pop(avro.codecs.MAX_DECOMPRESS_LENGTH_ENV, None)
        else:
            os.environ[avro.codecs.MAX_DECOMPRESS_LENGTH_ENV] = self._previous

    def _decoder_for(self, codec: Type[avro.codecs.Codec], data: bytes) -> avro.io.BinaryDecoder:
        """Compress `data` with the codec and wrap it as a data-file block."""
        compressed, _ = codec.compress(data)
        buffer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(buffer)
        encoder.write_long(len(compressed))
        buffer.write(compressed)
        return avro.io.BinaryDecoder(io.BytesIO(buffer.getvalue()))

    def _assert_over_limit_rejected(self, codec: Type[avro.codecs.Codec]) -> None:
        # A block of zeros far larger than the limit but that compresses tiny.
        decoder = self._decoder_for(codec, b"\x00" * (self.LIMIT * 8))
        with self.assertRaises(avro.errors.AvroDecompressionSizeException):
            codec.decompress(decoder)

    def _assert_within_limit_ok(self, codec: Type[avro.codecs.Codec]) -> None:
        payload = b"the quick brown fox " * 10  # well under the limit
        decoder = self._decoder_for(codec, payload)
        self.assertEqual(payload, codec.decompress(decoder).reader.read())

    def test_deflate_over_limit(self) -> None:
        self._assert_over_limit_rejected(avro.codecs.DeflateCodec)

    def test_deflate_within_limit(self) -> None:
        self._assert_within_limit_ok(avro.codecs.DeflateCodec)

    @unittest.skipUnless(avro.codecs.has_bzip2, "bzip2 not available")
    def test_bzip2_over_limit(self) -> None:
        self._assert_over_limit_rejected(avro.codecs.BZip2Codec)

    @unittest.skipUnless(avro.codecs.has_bzip2, "bzip2 not available")
    def test_bzip2_within_limit(self) -> None:
        self._assert_within_limit_ok(avro.codecs.BZip2Codec)

    @unittest.skipUnless(avro.codecs.has_snappy, "snappy not available")
    def test_snappy_over_limit(self) -> None:
        self._assert_over_limit_rejected(avro.codecs.SnappyCodec)

    @unittest.skipUnless(avro.codecs.has_snappy, "snappy not available")
    def test_snappy_within_limit(self) -> None:
        self._assert_within_limit_ok(avro.codecs.SnappyCodec)

    @unittest.skipUnless(avro.codecs.has_zstandard, "zstandard not available")
    def test_zstandard_over_limit(self) -> None:
        self._assert_over_limit_rejected(avro.codecs.ZstandardCodec)

    @unittest.skipUnless(avro.codecs.has_zstandard, "zstandard not available")
    def test_zstandard_within_limit(self) -> None:
        self._assert_within_limit_ok(avro.codecs.ZstandardCodec)


if __name__ == "__main__":
    unittest.main()
