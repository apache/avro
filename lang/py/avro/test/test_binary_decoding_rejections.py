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

"""AVRO-4304: Python harness for the shared cross-SDK must-reject binary vectors.

Loads ``share/test/data/binary-rejections.json`` and asserts that every vector
is rejected by the Avro binary decoder with a bounded, well-defined error rather
than being accepted or crashing. The shared fixtures guarantee that all language
SDKs reject the same malformed inputs identically and do not drift.
"""

import io
import json
import unittest
from pathlib import Path

import avro
import avro.errors
import avro.io
import avro.schema


def _find_manifest() -> Path:
    """Locate share/test/data/binary-rejections.json in the source tree."""
    here = Path(avro.__file__).resolve()
    for parent in here.parents:
        candidate = parent / "share" / "test" / "data" / "binary-rejections.json"
        if candidate.is_file():
            return candidate
    raise unittest.SkipTest("shared reject-vector fixture not found (not running from a source checkout)")


class TestSharedRejectionVectors(unittest.TestCase):
    def test_all_vectors_are_rejected(self) -> None:
        manifest = _find_manifest()
        vectors = json.loads(manifest.read_text())["vectors"]
        self.assertTrue(vectors, "no reject vectors found")
        for vector in vectors:
            name = vector["name"]
            schema = avro.schema.parse(vector["schema"])
            payload = bytes.fromhex(vector["bytesHex"])
            with self.subTest(vector=name):
                reader = avro.io.DatumReader(schema, schema)
                decoder = avro.io.BinaryDecoder(io.BytesIO(payload))
                # A conformant decoder must reject the payload with a bounded Avro
                # error rather than accepting it or crashing.
                self.assertRaises(avro.errors.AvroException, reader.read, decoder)


if __name__ == "__main__":
    unittest.main()
