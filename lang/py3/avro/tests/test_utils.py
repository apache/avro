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
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test utility classes and functions."""

import math
from unittest import TestCase

from avro.utils import bytes_for_digits, digits_for_bytes


class TestUtils(TestCase):

  def test_bytes_for_digits(self):
    for num_digits in range(1, 100):
      num_bytes = bytes_for_digits(num_digits)
      max_number = 10 ** num_digits - 1
      # bin returns the binary representation of an int as a string
      # prefixed with 0b, e.g. bin(7) -> '0b111'. Add one bit for sign.
      required_bytes = (1 + len(bin(max_number)[2:])) / 8

      self.assertLess(num_bytes - 1, required_bytes)
      self.assertLessEqual(required_bytes, num_bytes)

  def test_digits_for_bytes(self):
    for num_bytes in range(1, 100):
      num_digits = digits_for_bytes(num_bytes)
      max_number = 2 ** (num_bytes * 8 - 1)
      required_digits = int(math.log10(max_number + 1))

      self.assertLess(num_digits - 1, required_digits)
      self.assertLessEqual(required_digits, num_digits)
