# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import unittest
from avro import io
from avro import datafile

class TestDataFileInterop(unittest.TestCase):
  def test_interop(self):
    print ''
    print 'TEST INTEROP'
    print '============'
    print ''
    for f in os.listdir('@INTEROP_DATA_DIR@'):
      print 'READING %s' % f
      print ''

      # read data in binary from file
      reader = open(os.path.join('@INTEROP_DATA_DIR@', f), 'rb')
      datum_reader = io.DatumReader()
      dfr = datafile.DataFileReader(reader, datum_reader)
      for datum in dfr:
        assert datum is not None

if __name__ == '__main__':
  unittest.main()
