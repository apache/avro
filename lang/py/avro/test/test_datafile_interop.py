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

import os
import unittest

import avro
import avro.datafile
import avro.io

_INTEROP_DATA_DIR = os.path.join(os.path.dirname(avro.__file__), "test", "interop", "data")


@unittest.skipUnless(os.path.exists(_INTEROP_DATA_DIR), f"{_INTEROP_DATA_DIR} does not exist")
class TestDataFileInterop(unittest.TestCase):
    def test_interop(self):
        """Test Interop"""
        for f in os.listdir(_INTEROP_DATA_DIR):
            filename = os.path.join(_INTEROP_DATA_DIR, f)
            assert os.stat(filename).st_size > 0
            base_ext = os.path.splitext(os.path.basename(f))[0].split("_", 1)
            if len(base_ext) < 2 or base_ext[1] in avro.datafile.VALID_CODECS:
                print(f"READING {f}\n")

                # read data in binary from file
                datum_reader = avro.io.DatumReader()
                with open(filename, "rb") as reader:
                    dfr = avro.datafile.DataFileReader(reader, datum_reader)
                    i = 0
                    for i, datum in enumerate(dfr, 1):
                        assert datum is not None
                    assert i > 0
            else:
                print(f"SKIPPING {f} due to an unsupported codec\n")


if __name__ == "__main__":
    unittest.main()
