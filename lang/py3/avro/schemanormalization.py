#!/usr/bin/env python
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

import hashlib
from io import StringIO

from avro.schema import ARRAY, ENUM, ERROR, FIXED, MAP, RECORD, UNION


def ToParsingCanonicalForm(schema):
  """Returns the "Parsing Canonical Form" of a schema.

  The Parsing Canonical Form is defined by the Avro specification.

  Args:
      schema: The Schema to be normalized.
  Returns:
      A string containing the canonical JSON schema.
  """

  env = {}
  with StringIO() as output:
    return _BuildCanonicalForm(env, schema, output).getvalue()


def _BuildCanonicalForm(env, s, o):
    first_time = True
    st = s.type

    # The Java Avro implementation represents error records as records with
    # a separate error flag. For canonicalization to be consistent across
    # implementations we must normalize errors to have record type here.
    if st == ERROR:
      st = RECORD

    if st == UNION:
      o.write('[')
      for b in s.schemas:
        if not first_time:
          o.write(',')
        else:
          first_time = False
        _BuildCanonicalForm(env, b, o)
      o.write(']')
      return o

    elif st in {ARRAY, MAP}:
      o.write('{"type":"')
      o.write(st)
      o.write('"')
      if st == ARRAY:
        o.write(',"items":')
        _BuildCanonicalForm(env, s.items, o)
      else:
        o.write(',"values":')
        _BuildCanonicalForm(env, s.values, o)
      o.write('}')
      return o

    elif st in {ENUM, FIXED, RECORD}:
      name = s.fullname
      if name in env:
        o.write(env[name])
        return o
      qname = '"%s"' % name
      env[name] = qname
      o.write('{"name":')
      o.write(qname)
      o.write(',"type":"')
      o.write(st)
      o.write('"')
      if st == ENUM:
        o.write(',"symbols":[')
        for enum_symbol in s.symbols:
          if not first_time:
            o.write(',')
          else:
            first_time = False
          o.write('"')
          o.write(enum_symbol)
          o.write('"')
        o.write("]")
      elif st == FIXED:
        o.write(',"size":')
        o.write(str(s.size))
      else: # st == RECORD or st == ERROR
        o.write(',"fields":[')
        for f in s.fields:
          if not first_time:
            o.write(',')
          else:
            first_time = False
          o.write('{"name":"')
          o.write(f.name)
          o.write('"')
          o.write(',"type":')
          _BuildCanonicalForm(env, f.type, o)
          o.write('}')
        o.write(']')
      o.write('}')
      return o
    else:
      # boolean, bytes, double, float, int, long, null, string
      o.write('"')
      o.write(st)
      o.write('"')
      return o


def Fingerprint(parsing_normal_form_schema, fingerprint_algorithm_name):
  """Returns a fingerprint of a string of bytes.

  Args:
    parsing_normal_form_schema: A string containing an Avro
      schema in parsing normal form, such as one obtained
      by passing a schema object to ToParsingCanonicalForm()
    fingerprint_algorithm_name: One of the algorithm names
      returned by FingerprintAlgorithmNames(), typically
      'CRC-64-AVRO', 'md5' or 'sha256'. See the Avro
      specification for guidance on selecting a
      fingerprinting algorithm.
  Returns:
      A bytes object containing the schema fingerprint.
  """
  if fingerprint_algorithm_name not in FingerprintAlgorithmNames():
    raise ValueError("Unknown schema fingerprint algorithm {!r}"
                     .format(fingerprint_algorithm_name))
  fingerprint_algorithm = _FINGERPRINT_ALIASES_TO_NAMES[fingerprint_algorithm_name]
  data = parsing_normal_form_schema.encode('utf-8')
  if fingerprint_algorithm in _CRC_64_AVRO:
    return _Crc64AvroFingerprint(data)
  h = hashlib.new(fingerprint_algorithm, data)
  return h.digest()


_CRC_64_AVRO = frozenset({'CRC-64-AVRO'})
_PYTHON_DIGEST_NAMES = frozenset(hashlib.algorithms_guaranteed | _CRC_64_AVRO)

# These are the only three algorithms which Java implementations are
# *required* to support. We provide aliases for them here when the
# Python implementation also supports them, so that key fingerprint
# algorithm names can interoperate between Java and Python.
_JAVA_TO_PYTHON_DIGEST_NAMES = {
  'MD5': 'md5',
  'SHA-1': 'sha1',
  'SHA-256': 'sha256'}

_AVAILABLE_JAVA_TO_PYTHON_DIGEST_NAMES = {
  j: p for j, p in _JAVA_TO_PYTHON_DIGEST_NAMES.items()
  if p in hashlib.algorithms_guaranteed}

_FINGERPRINT_ALIASES_TO_NAMES = {name: name for name in _PYTHON_DIGEST_NAMES}
_FINGERPRINT_ALIASES_TO_NAMES.update(_AVAILABLE_JAVA_TO_PYTHON_DIGEST_NAMES)


def FingerprintAlgorithmNames():
  """A collection of fingerprint algorithm names.

  The same algorithm may be associated with more than one entry
  in this collection. For example, 'SHA-1' and 'sha1' might both
  be present in the result, in order to facilitate the interoperability
  of algorithm names between Python and, say, Java Avro
  implementations.

  Returns:
    A set of strings containing algorithm names, any
    one of which can be used as the fingerprint_algorithm
    argument of Fingerprint()
  """
  return _FINGERPRINT_ALIASES_TO_NAMES.keys()


_EMPTY64 = 0xc15d213aa4d7a795


def _Crc64AvroFingerprint(data):
  """The 64-bit Rabin Fingerprint.

  As described in the Avro specification.

  Args:
    data: A bytes object containing the UTF-8 encoded parsing canonical
      form of an Avro schema.
  Returns:
    A bytes object with a length of eight.
  """
  if _FP_TABLE is None:
    _PopulateFpTable()
  result = _EMPTY64
  for b in data:
    result = (result >> 8) ^ _FP_TABLE[(result ^ b) & 0xff]
  # Although not mentioned in the Avro specification, the Java
  # implementation gives fingerprint bytes in little-endian order
  return result.to_bytes(length=8, byteorder='little', signed=False)

_FP_TABLE = None


def _PopulateFpTable():
  global _FP_TABLE
  _FP_TABLE = []
  for i in range(256):
    fp = i
    for j in range(8):
      mask = -(fp & 1)
      fp = (fp >> 1) ^ (_EMPTY64 & mask)
    _FP_TABLE.append(fp)
