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

"""Runs all tests.

Usage:

- Run tests from all modules:
    ./run_tests.py discover [-v]

- Run tests in a specific module:
    ./run_tests.py test_schema [-v]

- Run a specific test:
    ./run_tests.py test_schema.TestSchema.testParse [-v]

- Set logging level:
    PYTHON_LOG_LEVEL=<log-level> ./run_tests.py ...
    log-level  0 includes all logging.
    log-level 10 includes debug logging.
    log-level 20 includes info logging.

- Command-line help:
  ./run_tests.py -h
  ./run_tests.py discover -h
"""

import logging
import os
import sys
import unittest

from avro.tests.test_datafile import *
from avro.tests.test_datafile_interop import *
from avro.tests.test_io import *
from avro.tests.test_ipc import *
from avro.tests.test_protocol import *
from avro.tests.test_schema import *
from avro.tests.test_script import *
from avro.tests.test_enum import *


def SetupLogging():
  log_level = int(os.environ.get('PYTHON_LOG_LEVEL', logging.INFO))

  log_formatter = logging.Formatter(
      '%(asctime)s %(levelname)s %(filename)s:%(lineno)s : %(message)s')
  logging.root.handlers = list()  # list.clear() only exists in python 3.3+
  logging.root.setLevel(log_level)
  console_handler = logging.StreamHandler()
  console_handler.setFormatter(log_formatter)
  console_handler.setLevel(logging.DEBUG)
  logging.root.addHandler(console_handler)


SetupLogging()


if __name__ == '__main__':
  unittest.main()
