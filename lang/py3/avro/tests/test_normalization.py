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

import unittest

from avro.schema import parse
from avro.schemanormalization import Fingerprint, FingerprintAlgorithmNames, ToParsingCanonicalForm


class TestSchemaNormalization(unittest.TestCase):

  def testCanonicalization1(self):
    pre='"float"'
    post='"float"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization2(self):
    pre='{"type": "float"}'
    post='"float"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization3(self):
    pre='"int"'
    post='"int"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization4(self):
    pre='{"type": "int"}'
    post='"int"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization5(self):
    pre='"double"'
    post='"double"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization6(self):
    pre='{"type": "double"}'
    post='"double"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization7(self):
    pre='"null"'
    post='"null"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization8(self):
    pre='{"type": "null"}'
    post='"null"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization9(self):
    pre='"bytes"'
    post='"bytes"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization10(self):
    pre='{"type": "bytes"}'
    post='"bytes"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization11(self):
    pre='"long"'
    post='"long"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization12(self):
    pre='{"type": "long"}'
    post='"long"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization13(self):
    pre='"boolean"'
    post='"boolean"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization14(self):
    pre='{"type": "boolean"}'
    post='"boolean"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization15(self):
    pre='"string"'
    post='"string"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization16(self):
    pre='{"type": "string"}'
    post='"string"'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization17(self):
    pre='{"type": "fixed", "name": "Test", "size": 1}'
    post='{"name":"Test","type":"fixed","size":1}'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization18(self):
    pre="""
    {
      "type": "fixed",
      "name": "MyFixed",
      "namespace": "org.apache.hadoop.avro",
      "size": 1
    }
    """
    post='{"name":"org.apache.hadoop.avro.MyFixed","type":"fixed","size":1}'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization19(self):
    pre='{"type": "enum", "name": "Test", "symbols": ["A", "B"]}'
    post='{"name":"Test","type":"enum","symbols":["A","B"]}'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization20(self):
    pre='{"type": "array", "items": "long"}'
    post='{"type":"array","items":"long"}'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization21(self):
    pre="""
    {
      "type": "array",
      "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
    }
    """
    post=('{"type":"array","items":{"name":"Test'
          '","type":"enum","symbols":["A","B"]}}')
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization22(self):
    pre='{"type": "map", "values": "long"}'
    post='{"type":"map","values":"long"}'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization23(self):
    pre="""
    {
      "type": "map",
      "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
    }
    """
    post=('{"type":"map","values":{"name":"Test"'
          ',"type":"enum","symbols":["A","B"]}}')
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization24(self):
    pre='["string", "null", "long"]'
    post='["string","null","long"]'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization25(self):
    pre="""
    {
      "type": "record",
      "name": "Test",
      "fields": [{"name": "f", "type": "long"}]
    }
    """
    post='{"name":"Test","type":"record","fields":[{"name":"f","type":"long"}]}'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization26(self):
    pre="""
    {
      "type": "error",
      "name": "Test",
      "fields": [{"name": "f", "type": "long"}]
    }
    """
    post='{"name":"Test","type":"record","fields":[{"name":"f","type":"long"}]}'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization27(self):
    pre="""
    {
      "type": "record",
      "name": "Node",
      "fields": [
        {"name": "label", "type": "string"},
        {"name": "children", "type": {"type": "array", "items": "Node"}}
      ]
    }
    """
    post=('{"name":"Node","type":"record","fields":[{"na'
          'me":"label","type":"string"},{"name":"childre'
          'n","type":{"type":"array","items":"Node"}}]}')
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization28(self):
    pre="""
    {
      "type": "record",
      "name": "Lisp",
      "fields": [{
        "name": "value",
        "type": [
          "null",
          "string",
          {
            "type": "record",
            "name": "Cons",
            "fields": [{"name": "car", "type": "Lisp"},
                       {"name": "cdr", "type": "Lisp"}]
          }
        ]
      }]
    }
    """
    post=('{"name":"Lisp","type":"record","fields":[{"name":"value","type'
          '":["null","string",{"name":"Cons","type":"record","fields":[{"'
          'name":"car","type":"Lisp"},{"name":"cdr","type":"Lisp"}]}]}]}')
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization29(self):
    pre="""
    {
      "type": "record",
      "name": "HandshakeRequest",
      "namespace": "org.apache.avro.ipc",
      "fields": [
        {
          "name": "clientHash",
          "type": {"type": "fixed", "name": "MD5", "size": 16}
        },
        {"name": "clientProtocol", "type": ["null", "string"]},
        {"name": "serverHash", "type": "MD5"},
        {
          "name": "meta",
          "type": ["null", {"type": "map", "values": "bytes"}]
        }
      ]
    }
    """
    post=('{"name":"org.apache.avro.ipc.HandshakeRequest","type":"r'
          'ecord","fields":[{"name":"clientHash","type":{"name":"or'
          'g.apache.avro.ipc.MD5","type":"fixed","size":16}},{"name'
          '":"clientProtocol","type":["null","string"]},{"name":"se'
          'rverHash","type":"org.apache.avro.ipc.MD5"},{"name":"met'
          'a","type":["null",{"type":"map","values":"bytes"}]}]}')
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization30(self):
    pre="""
    {
      "type": "record",
      "name": "HandshakeResponse",
      "namespace": "org.apache.avro.ipc",
      "fields": [
        {
          "name": "match",
          "type": {
            "type": "enum",
            "name": "HandshakeMatch",
            "symbols": ["BOTH", "CLIENT", "NONE"]
          }
        },
        {"name": "serverProtocol", "type": ["null", "string"]},
        {
          "name": "serverHash",
          "type": ["null", {"name": "MD5", "size": 16, "type": "fixed"}]
        },
        {
          "name": "meta",
          "type": ["null", {"type": "map", "values": "bytes"}]}]
        }
    """
    post=('{"name":"org.apache.avro.ipc.HandshakeResponse","type":"rec'
          'ord","fields":[{"name":"match","type":{"name":"org.apache.a'
          'vro.ipc.HandshakeMatch","type":"enum","symbols":["BOTH","CL'
          'IENT","NONE"]}},{"name":"serverProtocol","type":["null","st'
          'ring"]},{"name":"serverHash","type":["null",{"name":"org.ap'
          'ache.avro.ipc.MD5","type":"fixed","size":16}]},{"name":"met'
          'a","type":["null",{"type":"map","values":"bytes"}]}]}')
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization31(self):
    pre="""
    {
      "type": "record",
      "name": "Interop",
      "namespace": "org.apache.avro",
      "fields": [
        {"name": "intField", "type": "int"},
        {"name": "longField", "type": "long"},
        {"name": "stringField", "type": "string"},
        {"name": "boolField", "type": "boolean"},
        {"name": "floatField", "type": "float"},
        {"name": "doubleField", "type": "double"},
        {"name": "bytesField", "type": "bytes"},
        {"name": "nullField", "type": "null"},
        {"name": "arrayField", "type": {"type": "array", "items": "double"}},
        {
          "name": "mapField",
          "type": {
            "type": "map",
            "values": {"name": "Foo",
                       "type": "record",
                       "fields": [{"name": "label", "type": "string"}]}
          }
        },
        {
          "name": "unionField",
          "type": ["boolean", "double", {"type": "array", "items": "bytes"}]
        },
        {
          "name": "enumField",
          "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B", "C"]}
        },
        {
          "name": "fixedField",
          "type": {"type": "fixed", "name": "MD5", "size": 16}
        },
        {
          "name": "recordField",
          "type": {"type": "record",
                   "name": "Node",
                   "fields": [{"name": "label", "type": "string"},
                              {"name": "children",
                               "type": {"type": "array",
                                        "items": "Node"}}]}
        }
      ]
    }
    """
    post=('{"name":"org.apache.avro.Interop","type":"record","fields":[{"na'
          'me":"intField","type":"int"},{"name":"longField","type":"long"},'
          '{"name":"stringField","type":"string"},{"name":"boolField","type'
          '":"boolean"},{"name":"floatField","type":"float"},{"name":"doubl'
          'eField","type":"double"},{"name":"bytesField","type":"bytes"},{"'
          'name":"nullField","type":"null"},{"name":"arrayField","type":{"t'
          'ype":"array","items":"double"}},{"name":"mapField","type":{"type'
          '":"map","values":{"name":"org.apache.avro.Foo","type":"record","'
          'fields":[{"name":"label","type":"string"}]}}},{"name":"unionFiel'
          'd","type":["boolean","double",{"type":"array","items":"bytes"}]}'
          ',{"name":"enumField","type":{"name":"org.apache.avro.Kind","type'
          '":"enum","symbols":["A","B","C"]}},{"name":"fixedField","type":{'
          '"name":"org.apache.avro.MD5","type":"fixed","size":16}},{"name":'
          '"recordField","type":{"name":"org.apache.avro.Node","type":"reco'
          'rd","fields":[{"name":"label","type":"string"},{"name":"children'
          '","type":{"type":"array","items":"org.apache.avro.Node"}}]}}]}')
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization32(self):
    pre="""
    {
      "type": "record",
      "name": "ipAddr",
      "fields": [{
        "name": "addr",
        "type": [
          {"name": "IPv6", "type": "fixed", "size": 16},
          {"name": "IPv4", "type": "fixed", "size": 4}
        ]
      }]
    }
    """
    post=('{"name":"ipAddr","type":"record","fields":[{"name"'
          ':"addr","type":[{"name":"IPv6","type":"fixed","siz'
          'e":16},{"name":"IPv4","type":"fixed","size":4}]}]}')
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization33(self):
    pre="""
    {
      "type": "record",
      "name": "TestDoc",
      "doc":  "Doc string",
      "fields": [{"name": "name", "type": "string", "doc": "Doc String"}]
    }
    """
    post=('{"name":"TestDoc","type":"record","fiel'
          'ds":[{"name":"name","type":"string"}]}')
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)

  def testCanonicalization34(self):
    pre="""
    {"type": "enum", "name": "Test", "symbols": ["A", "B"], "doc": "Doc String"}
    """
    post='{"name":"Test","type":"enum","symbols":["A","B"]}'
    pre_schema = parse(pre)
    canonical_form = ToParsingCanonicalForm(pre_schema)
    self.assertEqual(canonical_form, post)


class TestSchemaFingerprintAlgorithms(unittest.TestCase):

  def testCrc64AvroIsAvailable(self):
    self.assertIn('CRC-64-AVRO', FingerprintAlgorithmNames())

  def testMd5IsAvailable(self):
    self.assertIn('md5', FingerprintAlgorithmNames())

  def testSha256IsAvailable(self):
    self.assertIn('sha256', FingerprintAlgorithmNames())

  def testMd5JavaNameIsAvailable(self):
    self.assertIn('MD5', FingerprintAlgorithmNames())

  def testSha256JavaNameIsAvailable(self):
    self.assertIn('SHA-256', FingerprintAlgorithmNames())

class TestSchemaFingerprinting(unittest.TestCase):

  def testUnsupportedFingerprintAlgorithmRaisesValueError(self):
    schema='"int"'
    algorithm='UNKNOWN'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    with self.assertRaises(ValueError) as context:
      Fingerprint(normal_form, algorithm)
    self.assertIn(algorithm, str(context.exception))

  def testFingerprint1(self):
    schema='"int"'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='8f5c393f1ad57572'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint2(self):
    schema='"int"'
    algorithm='md5'
    expected_hex_fingerprint='ef524ea1b91e73173d938ade36c1db32'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint3(self):
    schema='"int"'
    algorithm='sha256'
    expected_hex_fingerprint='3f2b87a9fe7cc9b13835598c3981cd45e3e355309e5090aa0933d7becb6fba45'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint4(self):
    schema='{"type": "int"}'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='8f5c393f1ad57572'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint5(self):
    schema='{"type": "int"}'
    algorithm='md5'
    expected_hex_fingerprint='ef524ea1b91e73173d938ade36c1db32'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint6(self):
    schema='{"type": "int"}'
    algorithm='sha256'
    expected_hex_fingerprint='3f2b87a9fe7cc9b13835598c3981cd45e3e355309e5090aa0933d7becb6fba45'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint7(self):
    schema='"float"'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='90d7a83ecb027c4d'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint8(self):
    schema='"float"'
    algorithm='md5'
    expected_hex_fingerprint='50a6b9db85da367a6d2df400a41758a6'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint9(self):
    schema='"float"'
    algorithm='sha256'
    expected_hex_fingerprint='1e71f9ec051d663f56b0d8e1fc84d71aa56ccfe9fa93aa20d10547a7abeb5cc0'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint10(self):
    schema='{"type": "float"}'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='90d7a83ecb027c4d'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint11(self):
    schema='{"type": "float"}'
    algorithm='md5'
    expected_hex_fingerprint='50a6b9db85da367a6d2df400a41758a6'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint12(self):
    schema='{"type": "float"}'
    algorithm='sha256'
    expected_hex_fingerprint='1e71f9ec051d663f56b0d8e1fc84d71aa56ccfe9fa93aa20d10547a7abeb5cc0'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint13(self):
    schema='"long"'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='b71df49344e154d0'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint14(self):
    schema='"long"'
    algorithm='md5'
    expected_hex_fingerprint='e1dd9a1ef98b451b53690370b393966b'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint15(self):
    schema='"long"'
    algorithm='sha256'
    expected_hex_fingerprint='c32c497df6730c97fa07362aa5023f37d49a027ec452360778114cf427965add'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint16(self):
    schema='{"type": "long"}'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='b71df49344e154d0'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint17(self):
    schema='{"type": "long"}'
    algorithm='md5'
    expected_hex_fingerprint='e1dd9a1ef98b451b53690370b393966b'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint18(self):
    schema='{"type": "long"}'
    algorithm='sha256'
    expected_hex_fingerprint='c32c497df6730c97fa07362aa5023f37d49a027ec452360778114cf427965add'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint19(self):
    schema='"double"'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='7e95ab32c035758e'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint20(self):
    schema='"double"'
    algorithm='md5'
    expected_hex_fingerprint='bfc71a62f38b99d6a93690deeb4b3af6'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint21(self):
    schema='"double"'
    algorithm='sha256'
    expected_hex_fingerprint='730a9a8c611681d7eef442e03c16c70d13bca3eb8b977bb403eaff52176af254'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint22(self):
    schema='{"type": "double"}'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='7e95ab32c035758e'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint23(self):
    schema='{"type": "double"}'
    algorithm='md5'
    expected_hex_fingerprint='bfc71a62f38b99d6a93690deeb4b3af6'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint24(self):
    schema='{"type": "double"}'
    algorithm='sha256'
    expected_hex_fingerprint='730a9a8c611681d7eef442e03c16c70d13bca3eb8b977bb403eaff52176af254'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint25(self):
    schema='"bytes"'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='651920c3da16c04f'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint26(self):
    schema='"bytes"'
    algorithm='md5'
    expected_hex_fingerprint='b462f06cb909be57c85008867784cde6'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint27(self):
    schema='"bytes"'
    algorithm='sha256'
    expected_hex_fingerprint='9ae507a9dd39ee5b7c7e285da2c0846521c8ae8d80feeae5504e0c981d53f5fa'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint28(self):
    schema='{"type": "bytes"}'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='651920c3da16c04f'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint29(self):
    schema='{"type": "bytes"}'
    algorithm='md5'
    expected_hex_fingerprint='b462f06cb909be57c85008867784cde6'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint30(self):
    schema='{"type": "bytes"}'
    algorithm='sha256'
    expected_hex_fingerprint='9ae507a9dd39ee5b7c7e285da2c0846521c8ae8d80feeae5504e0c981d53f5fa'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint31(self):
    schema='"string"'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='c70345637248018f'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint32(self):
    schema='"string"'
    algorithm='md5'
    expected_hex_fingerprint='095d71cf12556b9d5e330ad575b3df5d'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint33(self):
    schema='"string"'
    algorithm='sha256'
    expected_hex_fingerprint='e9e5c1c9e4f6277339d1bcde0733a59bd42f8731f449da6dc13010a916930d48'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint34(self):
    schema='{"type": "string"}'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='c70345637248018f'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint35(self):
    schema='{"type": "string"}'
    algorithm='md5'
    expected_hex_fingerprint='095d71cf12556b9d5e330ad575b3df5d'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint36(self):
    schema='{"type": "string"}'
    algorithm='sha256'
    expected_hex_fingerprint='e9e5c1c9e4f6277339d1bcde0733a59bd42f8731f449da6dc13010a916930d48'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint37(self):
    schema='"boolean"'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='64f7d4a478fc429f'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint38(self):
    schema='"boolean"'
    algorithm='md5'
    expected_hex_fingerprint='01f692b30d4a1c8a3e600b1440637f8f'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint39(self):
    schema='"boolean"'
    algorithm='sha256'
    expected_hex_fingerprint='a5b031ab62bc416d720c0410d802ea46b910c4fbe85c50a946ccc658b74e677e'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint40(self):
    schema='{"type": "boolean"}'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='64f7d4a478fc429f'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint41(self):
    schema='{"type": "boolean"}'
    algorithm='md5'
    expected_hex_fingerprint='01f692b30d4a1c8a3e600b1440637f8f'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint42(self):
    schema='{"type": "boolean"}'
    algorithm='sha256'
    expected_hex_fingerprint='a5b031ab62bc416d720c0410d802ea46b910c4fbe85c50a946ccc658b74e677e'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint43(self):
    schema='"null"'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='8a8f25cce724dd63'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint44(self):
    schema='"null"'
    algorithm='md5'
    expected_hex_fingerprint='9b41ef67651c18488a8b08bb67c75699'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint45(self):
    schema='"null"'
    algorithm='sha256'
    expected_hex_fingerprint='f072cbec3bf8841871d4284230c5e983dc211a56837aed862487148f947d1a1f'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint46(self):
    schema='{"type": "null"}'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='8a8f25cce724dd63'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint47(self):
    schema='{"type": "null"}'
    algorithm='md5'
    expected_hex_fingerprint='9b41ef67651c18488a8b08bb67c75699'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint48(self):
    schema='{"type": "null"}'
    algorithm='sha256'
    expected_hex_fingerprint='f072cbec3bf8841871d4284230c5e983dc211a56837aed862487148f947d1a1f'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint49(self):
    schema='{"type": "fixed", "name": "Test", "size": 1}'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='6869897b4049355b'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint50(self):
    schema='{"type": "fixed", "name": "Test", "size": 1}'
    algorithm='md5'
    expected_hex_fingerprint='db01bc515fcfcd2d4be82ed385288261'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint51(self):
    schema='{"type": "fixed", "name": "Test", "size": 1}'
    algorithm='sha256'
    expected_hex_fingerprint='f527116a6f44455697e935afc31dc60ad0f95caf35e1d9c9db62edb3ffeb9170'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint52(self):
    schema="""
    {
      "type": "fixed",
      "name": "MyFixed",
      "namespace": "org.apache.hadoop.avro",
      "size": 1
    }
    """
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='fadbd138e85bdf45'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint53(self):
    schema="""
    {
      "type": "fixed",
      "name": "MyFixed",
      "namespace": "org.apache.hadoop.avro",
      "size": 1
    }
    """
    algorithm='md5'
    expected_hex_fingerprint='d74b3726484422711c465d49e857b1ba'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint54(self):
    schema="""
    {
      "type": "fixed",
      "name": "MyFixed",
      "namespace": "org.apache.hadoop.avro",
      "size": 1
    }
    """
    algorithm='sha256'
    expected_hex_fingerprint='28e493a44771cecc5deca4bd938cdc3d5a24cfe1f3760bc938fa1057df6334fc'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint55(self):
    schema='{"type": "enum", "name": "Test", "symbols": ["A", "B"]}'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='03a2f2c2e27f7a16'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint56(self):
    schema='{"type": "enum", "name": "Test", "symbols": ["A", "B"]}'
    algorithm='md5'
    expected_hex_fingerprint='d883f2a9b16ed085fcc5e4ca6c8f6ed1'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint57(self):
    schema='{"type": "enum", "name": "Test", "symbols": ["A", "B"]}'
    algorithm='sha256'
    expected_hex_fingerprint='9b51286144f87ce5aebdc61ca834379effa5a41ce6ac0938630ff246297caca8'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint58(self):
    schema='{"type": "array", "items": "long"}'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='715e2ea28bc91654'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint59(self):
    schema='{"type": "array", "items": "long"}'
    algorithm='md5'
    expected_hex_fingerprint='c1c387e8d6a58f0df749b698991b1f43'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint60(self):
    schema='{"type": "array", "items": "long"}'
    algorithm='sha256'
    expected_hex_fingerprint='f78e954167feb23dcb1ce01e8463cebf3408e0a4259e16f24bd38f6d0f1d578b'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint61(self):
    schema="""
    {
      "type": "array",
      "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
    }
    """
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='10d9ade1fa3a0387'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint62(self):
    schema="""
    {
      "type": "array",
      "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
    }
    """
    algorithm='md5'
    expected_hex_fingerprint='cfc7b861c7cfef082a6ef082948893fa'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint63(self):
    schema="""
    {
      "type": "array",
      "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
    }
    """
    algorithm='sha256'
    expected_hex_fingerprint='0d8edd49d7f7e9553668f133577bc99f842852b55d9f84f1f7511e4961aa685c'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint64(self):
    schema='{"type": "map", "values": "long"}'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='6f74f4e409b1334e'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint65(self):
    schema='{"type": "map", "values": "long"}'
    algorithm='md5'
    expected_hex_fingerprint='32b3f1a3177a0e73017920f00448b56e'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint66(self):
    schema='{"type": "map", "values": "long"}'
    algorithm='sha256'
    expected_hex_fingerprint='b8fad07d458971a07692206b8a7cf626c86c62fe6bcff7c1b11bc7295de34853'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint67(self):
    schema="""
    {
      "type": "map",
      "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
    }
    """
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='df2ab0626f6b812d'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint68(self):
    schema="""
    {
      "type": "map",
      "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
    }
    """
    algorithm='md5'
    expected_hex_fingerprint='c588da6ba99701c41e73fd30d23f994e'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint69(self):
    schema="""
    {
      "type": "map",
      "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
    }
    """
    algorithm='sha256'
    expected_hex_fingerprint='3886747ed1669a8af476b549e97b34222afb2fed5f18bb27c6f367ea0351a576'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint70(self):
    schema='["string", "null", "long"]'
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='65a5be410d687566'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint71(self):
    schema='["string", "null", "long"]'
    algorithm='md5'
    expected_hex_fingerprint='b11cf95f0a55dd55f9ee515a37bf937a'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint72(self):
    schema='["string", "null", "long"]'
    algorithm='sha256'
    expected_hex_fingerprint='ed8d254116441bb35e237ad0563cf5432b8c975334bd222c1ee84609435d95bb'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint73(self):
    schema="""
    {
      "type": "record",
      "name": "Test",
      "fields": [{"name": "f", "type": "long"}]
    }
    """
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='ed94e5f5e6eb588e'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint74(self):
    schema="""
    {
      "type": "record",
      "name": "Test",
      "fields": [{"name": "f", "type": "long"}]
    }
    """
    algorithm='md5'
    expected_hex_fingerprint='69531a03db788afe353244cd049b1e6d'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint75(self):
    schema="""
    {
      "type": "record",
      "name": "Test",
      "fields": [{"name": "f", "type": "long"}]
    }
    """
    algorithm='sha256'
    expected_hex_fingerprint='9670f15a8f96d23e92830d00b8bd57275e02e3e173ffef7c253c170b6beabeb8'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint76(self):
    schema="""
    {
      "type": "error",
      "name": "Test",
      "fields": [{"name": "f", "type": "long"}]
    }
    """
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='ed94e5f5e6eb588e'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint77(self):
    schema="""
    {
      "type": "error",
      "name": "Test",
      "fields": [{"name": "f", "type": "long"}]
    }
    """
    algorithm='md5'
    expected_hex_fingerprint='69531a03db788afe353244cd049b1e6d'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint78(self):
    schema="""
    {
      "type": "error",
      "name": "Test",
      "fields": [{"name": "f", "type": "long"}]
    }
    """
    algorithm='sha256'
    expected_hex_fingerprint='9670f15a8f96d23e92830d00b8bd57275e02e3e173ffef7c253c170b6beabeb8'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint79(self):
    schema="""
    {
      "type": "record",
      "name": "Node",
      "fields": [
        {"name": "label", "type": "string"},
        {"name": "children", "type": {"type": "array", "items": "Node"}}
      ]
    }
    """
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='52cba544c3e756b7'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint80(self):
    schema="""
    {
      "type": "record",
      "name": "Node",
      "fields": [
        {"name": "label", "type": "string"},
        {"name": "children", "type": {"type": "array", "items": "Node"}}
      ]
    }
    """
    algorithm='md5'
    expected_hex_fingerprint='99625b0cc02050363e89ef66b0f406c9'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint81(self):
    schema="""
    {
      "type": "record",
      "name": "Node",
      "fields": [
        {"name": "label", "type": "string"},
        {"name": "children", "type": {"type": "array", "items": "Node"}}
      ]
    }
    """
    algorithm='sha256'
    expected_hex_fingerprint='65d80dc8c95c98a9671d92cf0415edfabfee2cb058df2138606656cd6ae4dc59'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint82(self):
    schema="""
    {
      "type": "record",
      "name": "Lisp",
      "fields": [{
        "name": "value",
        "type": [
          "null",
          "string",
          {
            "type": "record",
            "name": "Cons",
            "fields": [{"name": "car", "type": "Lisp"},
                       {"name": "cdr", "type": "Lisp"}]
          }
        ]
      }]
    }
    """
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='68d91a23eda0b306'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint83(self):
    schema="""
    {
      "type": "record",
      "name": "Lisp",
      "fields": [{
        "name": "value",
        "type": [
          "null",
          "string",
          {
            "type": "record",
            "name": "Cons",
            "fields": [{"name": "car", "type": "Lisp"},
                       {"name": "cdr", "type": "Lisp"}]
          }
        ]
      }]
    }
    """
    algorithm='md5'
    expected_hex_fingerprint='9e1d0d15b52789fcb8e3a88b53059d5f'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint84(self):
    schema="""
    {
      "type": "record",
      "name": "Lisp",
      "fields": [{
        "name": "value",
        "type": [
          "null",
          "string",
          {
            "type": "record",
            "name": "Cons",
            "fields": [{"name": "car", "type": "Lisp"},
                       {"name": "cdr", "type": "Lisp"}]
          }
        ]
      }]
    }
    """
    algorithm='sha256'
    expected_hex_fingerprint='e5ce4f4a15ce19fa1047cfe16a3b0e13a755db40f00f23284fdd376fc1c7dd21'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint85(self):
    schema="""
    {
      "type": "record",
      "name": "HandshakeRequest",
      "namespace": "org.apache.avro.ipc",
      "fields": [
        {
          "name": "clientHash",
          "type": {"type": "fixed", "name": "MD5", "size": 16}
        },
        {"name": "clientProtocol", "type": ["null", "string"]},
        {"name": "serverHash", "type": "MD5"},
        {
          "name": "meta",
          "type": ["null", {"type": "map", "values": "bytes"}]
        }
      ]
    }
    """
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='b96ad79e5a7c5757'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint86(self):
    schema="""
    {
      "type": "record",
      "name": "HandshakeRequest",
      "namespace": "org.apache.avro.ipc",
      "fields": [
        {
          "name": "clientHash",
          "type": {"type": "fixed", "name": "MD5", "size": 16}
        },
        {"name": "clientProtocol", "type": ["null", "string"]},
        {"name": "serverHash", "type": "MD5"},
        {
          "name": "meta",
          "type": ["null", {"type": "map", "values": "bytes"}]
        }
      ]
    }
    """
    algorithm='md5'
    expected_hex_fingerprint='4c822af2e17eecd92422827eede97f5b'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint87(self):
    schema="""
    {
      "type": "record",
      "name": "HandshakeRequest",
      "namespace": "org.apache.avro.ipc",
      "fields": [
        {
          "name": "clientHash",
          "type": {"type": "fixed", "name": "MD5", "size": 16}
        },
        {"name": "clientProtocol", "type": ["null", "string"]},
        {"name": "serverHash", "type": "MD5"},
        {
          "name": "meta",
          "type": ["null", {"type": "map", "values": "bytes"}]
        }
      ]
    }
    """
    algorithm='sha256'
    expected_hex_fingerprint='2b2f7a9b22991fe0df9134cb6b5ff7355343e797aaea337e0150e20f3a35800e'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint88(self):
    schema="""
    {
      "type": "record",
      "name": "HandshakeResponse",
      "namespace": "org.apache.avro.ipc",
      "fields": [
        {
          "name": "match",
          "type": {
            "type": "enum",
            "name": "HandshakeMatch",
            "symbols": ["BOTH", "CLIENT", "NONE"]
          }
        },
        {"name": "serverProtocol", "type": ["null", "string"]},
        {
          "name": "serverHash",
          "type": ["null", {"name": "MD5", "size": 16, "type": "fixed"}]
        },
        {
          "name": "meta",
          "type": ["null", {"type": "map", "values": "bytes"}]}]
        }
    """
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='00feee01de4ea50e'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint89(self):
    schema="""
    {
      "type": "record",
      "name": "HandshakeResponse",
      "namespace": "org.apache.avro.ipc",
      "fields": [
        {
          "name": "match",
          "type": {
            "type": "enum",
            "name": "HandshakeMatch",
            "symbols": ["BOTH", "CLIENT", "NONE"]
          }
        },
        {"name": "serverProtocol", "type": ["null", "string"]},
        {
          "name": "serverHash",
          "type": ["null", {"name": "MD5", "size": 16, "type": "fixed"}]
        },
        {
          "name": "meta",
          "type": ["null", {"type": "map", "values": "bytes"}]}]
        }
    """
    algorithm='md5'
    expected_hex_fingerprint='afe529d01132daab7f4e2a6663e7a2f5'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint90(self):
    schema="""
    {
      "type": "record",
      "name": "HandshakeResponse",
      "namespace": "org.apache.avro.ipc",
      "fields": [
        {
          "name": "match",
          "type": {
            "type": "enum",
            "name": "HandshakeMatch",
            "symbols": ["BOTH", "CLIENT", "NONE"]
          }
        },
        {"name": "serverProtocol", "type": ["null", "string"]},
        {
          "name": "serverHash",
          "type": ["null", {"name": "MD5", "size": 16, "type": "fixed"}]
        },
        {
          "name": "meta",
          "type": ["null", {"type": "map", "values": "bytes"}]}]
        }
    """
    algorithm='sha256'
    expected_hex_fingerprint='a303cbbfe13958f880605d70c521a4b7be34d9265ac5a848f25916a67b11d889'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint91(self):
    schema="""
    {
      "type": "record",
      "name": "Interop",
      "namespace": "org.apache.avro",
      "fields": [
        {"name": "intField", "type": "int"},
        {"name": "longField", "type": "long"},
        {"name": "stringField", "type": "string"},
        {"name": "boolField", "type": "boolean"},
        {"name": "floatField", "type": "float"},
        {"name": "doubleField", "type": "double"},
        {"name": "bytesField", "type": "bytes"},
        {"name": "nullField", "type": "null"},
        {"name": "arrayField", "type": {"type": "array", "items": "double"}},
        {
          "name": "mapField",
          "type": {
            "type": "map",
            "values": {"name": "Foo",
                       "type": "record",
                       "fields": [{"name": "label", "type": "string"}]}
          }
        },
        {
          "name": "unionField",
          "type": ["boolean", "double", {"type": "array", "items": "bytes"}]
        },
        {
          "name": "enumField",
          "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B", "C"]}
        },
        {
          "name": "fixedField",
          "type": {"type": "fixed", "name": "MD5", "size": 16}
        },
        {
          "name": "recordField",
          "type": {"type": "record",
                   "name": "Node",
                   "fields": [{"name": "label", "type": "string"},
                              {"name": "children",
                               "type": {"type": "array",
                                        "items": "Node"}}]}
        }
      ]
    }
    """
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='e82c0a93a6a0b5a4'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint92(self):
    schema="""
    {
      "type": "record",
      "name": "Interop",
      "namespace": "org.apache.avro",
      "fields": [
        {"name": "intField", "type": "int"},
        {"name": "longField", "type": "long"},
        {"name": "stringField", "type": "string"},
        {"name": "boolField", "type": "boolean"},
        {"name": "floatField", "type": "float"},
        {"name": "doubleField", "type": "double"},
        {"name": "bytesField", "type": "bytes"},
        {"name": "nullField", "type": "null"},
        {"name": "arrayField", "type": {"type": "array", "items": "double"}},
        {
          "name": "mapField",
          "type": {
            "type": "map",
            "values": {"name": "Foo",
                       "type": "record",
                       "fields": [{"name": "label", "type": "string"}]}
          }
        },
        {
          "name": "unionField",
          "type": ["boolean", "double", {"type": "array", "items": "bytes"}]
        },
        {
          "name": "enumField",
          "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B", "C"]}
        },
        {
          "name": "fixedField",
          "type": {"type": "fixed", "name": "MD5", "size": 16}
        },
        {
          "name": "recordField",
          "type": {"type": "record",
                   "name": "Node",
                   "fields": [{"name": "label", "type": "string"},
                              {"name": "children",
                               "type": {"type": "array",
                                        "items": "Node"}}]}
        }
      ]
    }
    """
    algorithm='md5'
    expected_hex_fingerprint='994fea1a1be7ff8603cbe40c3bc7e4ca'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint93(self):
    schema="""
    {
      "type": "record",
      "name": "Interop",
      "namespace": "org.apache.avro",
      "fields": [
        {"name": "intField", "type": "int"},
        {"name": "longField", "type": "long"},
        {"name": "stringField", "type": "string"},
        {"name": "boolField", "type": "boolean"},
        {"name": "floatField", "type": "float"},
        {"name": "doubleField", "type": "double"},
        {"name": "bytesField", "type": "bytes"},
        {"name": "nullField", "type": "null"},
        {"name": "arrayField", "type": {"type": "array", "items": "double"}},
        {
          "name": "mapField",
          "type": {
            "type": "map",
            "values": {"name": "Foo",
                       "type": "record",
                       "fields": [{"name": "label", "type": "string"}]}
          }
        },
        {
          "name": "unionField",
          "type": ["boolean", "double", {"type": "array", "items": "bytes"}]
        },
        {
          "name": "enumField",
          "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B", "C"]}
        },
        {
          "name": "fixedField",
          "type": {"type": "fixed", "name": "MD5", "size": 16}
        },
        {
          "name": "recordField",
          "type": {"type": "record",
                   "name": "Node",
                   "fields": [{"name": "label", "type": "string"},
                              {"name": "children",
                               "type": {"type": "array",
                                        "items": "Node"}}]}
        }
      ]
    }
    """
    algorithm='sha256'
    expected_hex_fingerprint='cccfd6e3f917cf53b0f90c206342e6703b0d905071f724a1c1f85b731c74058d'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint94(self):
    schema="""
    {
      "type": "record",
      "name": "ipAddr",
      "fields": [{
        "name": "addr",
        "type": [
          {"name": "IPv6", "type": "fixed", "size": 16},
          {"name": "IPv4", "type": "fixed", "size": 4}
        ]
      }]
    }
    """
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='8d961b4e298a1844'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint95(self):
    schema="""
    {
      "type": "record",
      "name": "ipAddr",
      "fields": [{
        "name": "addr",
        "type": [
          {"name": "IPv6", "type": "fixed", "size": 16},
          {"name": "IPv4", "type": "fixed", "size": 4}
        ]
      }]
    }
    """
    algorithm='md5'
    expected_hex_fingerprint='45d85c69b353a99b93d7c4f2fcf0c30d'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint96(self):
    schema="""
    {
      "type": "record",
      "name": "ipAddr",
      "fields": [{
        "name": "addr",
        "type": [
          {"name": "IPv6", "type": "fixed", "size": 16},
          {"name": "IPv4", "type": "fixed", "size": 4}
        ]
      }]
    }
    """
    algorithm='sha256'
    expected_hex_fingerprint='6f6fc8f685a4f07d99734946565d63108806d55a8620febea047cf52cb0ac181'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint97(self):
    schema="""
    {
      "type": "record",
      "name": "TestDoc",
      "doc":  "Doc string",
      "fields": [{"name": "name", "type": "string", "doc": "Doc String"}]
    }
    """
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='0e6660f02bcdc109'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint98(self):
    schema="""
    {
      "type": "record",
      "name": "TestDoc",
      "doc":  "Doc string",
      "fields": [{"name": "name", "type": "string", "doc": "Doc String"}]
    }
    """
    algorithm='md5'
    expected_hex_fingerprint='f2da75f5131f5ab80629538287b8beb2'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint99(self):
    schema="""
    {
      "type": "record",
      "name": "TestDoc",
      "doc":  "Doc string",
      "fields": [{"name": "name", "type": "string", "doc": "Doc String"}]
    }
    """
    algorithm='sha256'
    expected_hex_fingerprint='0b3644f7aa5ca2fc4bad93ca2d3609c12aa9dbda9c15e68b34c120beff08e7b9'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint100(self):
    schema="""
    {"type": "enum", "name": "Test", "symbols": ["A", "B"], "doc": "Doc String"}
    """
    algorithm='CRC-64-AVRO'
    expected_hex_fingerprint='03a2f2c2e27f7a16'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(16)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint101(self):
    schema="""
    {"type": "enum", "name": "Test", "symbols": ["A", "B"], "doc": "Doc String"}
    """
    algorithm='md5'
    expected_hex_fingerprint='d883f2a9b16ed085fcc5e4ca6c8f6ed1'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint102(self):
    schema="""
    {"type": "enum", "name": "Test", "symbols": ["A", "B"], "doc": "Doc String"}
    """
    algorithm='sha256'
    expected_hex_fingerprint='9b51286144f87ce5aebdc61ca834379effa5a41ce6ac0938630ff246297caca8'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint103(self):
    schema='{"type": "int"}'
    algorithm='MD5'  # Java compatible name alias
    expected_hex_fingerprint='ef524ea1b91e73173d938ade36c1db32'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(32)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)

  def testFingerprint104(self):
    schema='{"type": "int"}'
    algorithm='SHA-256'  # Java compatible name alias
    expected_hex_fingerprint='3f2b87a9fe7cc9b13835598c3981cd45e3e355309e5090aa0933d7becb6fba45'
    pre_schema = parse(schema)
    normal_form = ToParsingCanonicalForm(pre_schema)
    fingerprint = Fingerprint(normal_form, algorithm)
    actual_hex_fingerprint = "".join(format(b, "02x") for b in fingerprint).zfill(64)
    self.assertEqual(actual_hex_fingerprint, expected_hex_fingerprint)
