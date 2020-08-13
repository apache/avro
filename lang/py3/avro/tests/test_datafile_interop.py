#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import glob
import os
import unittest

from avro import datafile
from avro import io


class TestDataFileInterop(unittest.TestCase):
  def testInterop(self):
    datum_reader = io.DatumReader()
    for avro_file in glob.glob('../../build/interop/data/*.avro'):
      base_ext = os.path.splitext(os.path.basename(avro_file))[0].split('_', 1)
      if len(base_ext) < 2 or base_ext[1] in datafile.VALID_CODECS:
        print('Reading {}'.format(avro_file))
        with open(avro_file, 'rb') as reader, \
          datafile.DataFileReader(reader, datum_reader) as dfr:
          i = 0
          for i, datum in enumerate(dfr, 1):
            self.assertIsNotNone(datum)
          self.assertGreater(i, 0)
      else:
        print('Skipping {} due to an unsupported codec'.format(avro_file))


if __name__ == '__main__':
  raise Exception('Use run_tests.py')
