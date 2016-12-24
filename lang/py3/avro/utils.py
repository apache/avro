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

"""Utility classes and functions."""

import math


class ImmutableDict(dict):
  """Dictionary guaranteed immutable.

  All mutations raise an exception.
  Behaves exactly as a dict otherwise.
  """

  def __init__(self, items=None, **kwargs):
    if items is not None:
      super(ImmutableDict, self).__init__(items)
      assert (len(kwargs) == 0)
    else:
      super(ImmutableDict, self).__init__(**kwargs)

  def __setitem__(self, key, value):
    raise Exception(
        'Attempting to map key %r to value %r in ImmutableDict %r'
        % (key, value, self))

  def __delitem__(self, key):
    raise Exception(
        'Attempting to remove mapping for key %r in ImmutableDict %r'
        % (key, self))

  def clear(self):
    raise Exception('Attempting to clear ImmutableDict %r' % self)

  def update(self, items=None, **kwargs):
    raise Exception(
        'Attempting to update ImmutableDict %r with items=%r, kwargs=%r'
        % (self, items, kwargs))

  def pop(self, key, default=None):
    raise Exception(
        'Attempting to pop key %r from ImmutableDict %r' % (key, self))

  def popitem(self):
    raise Exception('Attempting to pop item from ImmutableDict %r' % self)


def bytes_for_digits(num_digits):
  """Max number of bytes required to store n base-10 digits."""
  return math.ceil((math.log2(math.pow(10, num_digits) + 1) + 1) / 8)


def digits_for_bytes(num_bytes):
  """Max number of base-10 digits able to be represented by n bytes."""
  return int(math.log10(math.pow(2, 8 * num_bytes - 1) - 1))
