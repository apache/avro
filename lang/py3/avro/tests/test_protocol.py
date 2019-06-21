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
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Test the protocol parsing logic.
"""

import logging
import traceback
import unittest

from avro import protocol


# ------------------------------------------------------------------------------


class ExampleProtocol(object):
  def __init__(
      self,
      protocol_string,
      valid=True,
  ):
    self._protocol_string = protocol_string
    self._valid = valid

  @property
  def protocol_string(self):
    return self._protocol_string

  @property
  def valid(self):
    return self._valid


# ------------------------------------------------------------------------------
# Protocol test cases:

HELLO_WORLD = ExampleProtocol("""
{
  "namespace": "com.acme",
  "protocol": "HelloWorld",
  "types": [
    {
      "name": "Greeting",
      "type": "record",
      "fields": [{"name": "message", "type": "string"}]
    },
    {
      "name": "Curse",
      "type": "error",
      "fields": [{"name": "message", "type": "string"}]
    }
  ],
  "messages": {
    "hello": {
      "request": [{"name": "greeting", "type": "Greeting" }],
      "response": "Greeting",
      "errors": ["Curse"]
    }
  }
}
""")

EXAMPLES = [
  HELLO_WORLD,

  ExampleProtocol(
  """
  {
    "namespace": "org.apache.avro.test",
    "protocol": "Simple",
    "types": [
      {"name": "Kind", "type": "enum", "symbols": ["FOO","BAR","BAZ"]},
      {"name": "MD5", "type": "fixed", "size": 16},
      {"name": "TestRecord", "type": "record",
       "fields": [
         {"name": "name", "type": "string", "order": "ignore"},
         {"name": "kind", "type": "Kind", "order": "descending"},
         {"name": "hash", "type": "MD5"}
       ]
      },
      {"name": "TestError", "type": "error",
       "fields": [
         {"name": "message", "type": "string"}
       ]
      }
    ],
    "messages": {
      "hello": {
        "request": [{"name": "greeting", "type": "string"}],
        "response": "string"
      },
      "echo": {
        "request": [{"name": "record", "type": "TestRecord"}],
        "response": "TestRecord"
      },
      "add": {
        "request": [{"name": "arg1", "type": "int"},
                    {"name": "arg2", "type": "int"}],
        "response": "int"
      },
      "echoBytes": {
        "request": [{"name": "data", "type": "bytes"}],
        "response": "bytes"
      },
      "error": {
        "request": [],
        "response": "null",
        "errors": ["TestError"]
      }
    }
  }
  """),

  ExampleProtocol(
  """
  {
    "namespace": "org.apache.avro.test.namespace",
    "protocol": "TestNamespace",
    "types": [
      {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
      {
        "name": "TestRecord",
        "type": "record",
        "fields": [{"name": "hash", "type": "org.apache.avro.test.util.MD5"}]
      },
      {
        "name": "TestError",
        "namespace": "org.apache.avro.test.errors",
        "type": "error",
        "fields": [{"name": "message", "type": "string"}]
      }
    ],
    "messages": {
      "echo": {
        "request": [{"name": "record", "type": "TestRecord"}],
        "response": "TestRecord"
      },
      "error": {
        "request": [],
        "response": "null",
        "errors": ["org.apache.avro.test.errors.TestError"]
      }
    }
  }
  """),

  ExampleProtocol(
  """
  {
    "namespace": "org.apache.avro.test.namespace",
    "protocol": "TestImplicitNamespace",
    "types": [
      {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
      {
        "name": "ReferencedRecord",
        "type": "record",
        "fields": [{"name": "foo", "type": "string"}]
      },
      {
        "name": "TestRecord",
        "type": "record",
        "fields": [
          {"name": "hash", "type": "org.apache.avro.test.util.MD5"},
          {"name": "unqalified", "type": "ReferencedRecord"}
        ]
      },
      {
        "name": "TestError",
        "type": "error",
        "fields": [{"name": "message", "type": "string"}]
      }
    ],
    "messages": {
      "echo": {
        "request": [
          {"name": "qualified", "type": "org.apache.avro.test.namespace.TestRecord"}
        ],
        "response": "TestRecord"
      },
      "error": {
        "request": [],
        "response": "null",
        "errors": ["org.apache.avro.test.namespace.TestError"]
      }
    }
  }
  """),

  ExampleProtocol(
  """
  {
    "namespace": "org.apache.avro.test.namespace",
    "protocol": "TestNamespaceTwo",
    "types": [
      {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
      {
        "name": "ReferencedRecord",
        "namespace": "org.apache.avro.other.namespace",
        "type": "record",
        "fields": [{"name": "foo", "type": "string"}]
      },
      {
        "name": "TestRecord",
        "type": "record",
        "fields": [
          {"name": "hash", "type": "org.apache.avro.test.util.MD5"},
          {
            "name": "qualified",
            "type": "org.apache.avro.other.namespace.ReferencedRecord"
          }
        ]
      },
      {
        "name": "TestError",
        "type": "error",
        "fields": [{"name": "message", "type": "string"}]
      }
    ],
    "messages": {
      "echo": {
        "request": [
          {
            "name": "qualified",
            "type": "org.apache.avro.test.namespace.TestRecord"
          }
        ],
        "response": "TestRecord"
      },
      "error": {
        "request": [],
        "response": "null",
        "errors": ["org.apache.avro.test.namespace.TestError"]
      }
    }
  }
  """),

  ExampleProtocol(
  """
  {
    "namespace": "org.apache.avro.test.namespace",
    "protocol": "TestValidRepeatedName",
    "types": [
      {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
      {
        "name": "ReferencedRecord",
        "namespace": "org.apache.avro.other.namespace",
        "type": "record",
        "fields": [{"name": "foo", "type": "string"}]
      },
      {
        "name": "ReferencedRecord",
        "type": "record",
        "fields": [{"name": "bar", "type": "double"}]
      },
      {
        "name": "TestError",
        "type": "error",
        "fields": [{"name": "message", "type": "string"}]
      }
    ],
    "messages": {
      "echo": {
        "request": [{"name": "qualified", "type": "ReferencedRecord"}],
        "response": "org.apache.avro.other.namespace.ReferencedRecord"
      },
      "error": {
        "request": [],
        "response": "null",
        "errors": ["org.apache.avro.test.namespace.TestError"]
      }
    }
  }
  """),

  ExampleProtocol(
  """
  {
    "namespace": "org.apache.avro.test.namespace",
    "protocol": "TestInvalidRepeatedName",
    "types": [
      {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
      {
        "name": "ReferencedRecord",
        "type": "record",
        "fields": [{"name": "foo", "type": "string"}]
      },
      {
        "name": "ReferencedRecord",
        "type": "record",
        "fields": [{"name": "bar", "type": "double"}]
      },
      {
        "name": "TestError",
        "type": "error",
        "fields": [{"name": "message", "type": "string"}]
      }
    ],
    "messages": {
      "echo": {
        "request": [{"name": "qualified", "type": "ReferencedRecord"}],
        "response": "org.apache.avro.other.namespace.ReferencedRecord"
      },
      "error": {
        "request": [],
        "response": "null",
        "errors": ["org.apache.avro.test.namespace.TestError"]
      }
    }
  }
  """,
  valid=False),

  ExampleProtocol(
  """
  {
    "namespace": "org.apache.avro.test",
    "protocol": "BulkData",
    "types": [],
    "messages": {
      "read": {
        "request": [],
        "response": "bytes"
      },
      "write": {
        "request": [ {"name": "data", "type": "bytes"} ],
        "response": "null"
      }
    }
  }
  """),

  ExampleProtocol(
  """
  {
    "protocol": "API",
    "namespace": "xyz.api",
    "types": [
      {
        "type": "enum",
        "name": "Symbology",
        "namespace": "xyz.api.product",
        "symbols": [ "OPRA", "CUSIP", "ISIN", "SEDOL" ]
      },
      {
        "type": "record",
        "name": "Symbol",
        "namespace": "xyz.api.product",
        "fields": [ {
          "name": "symbology",
          "type": "xyz.api.product.Symbology"
        }, {
          "name": "symbol",
          "type": "string"
        } ]
      },
      {
        "type": "record",
        "name": "MultiSymbol",
        "namespace": "xyz.api.product",
        "fields": [{
          "name": "symbols",
          "type": {
            "type": "map",
            "values": "xyz.api.product.Symbol"
          }
        }]
      }
    ],
    "messages": {}
  }
  """),
]
# End of EXAMPLES


VALID_EXAMPLES = [e for e in EXAMPLES if e.valid]


# ------------------------------------------------------------------------------


class TestProtocol(unittest.TestCase):

  def testParse(self):
    correct = 0
    for iexample, example in enumerate(EXAMPLES):
      logging.debug(
          'Parsing protocol #%d:\n%s',
          iexample, example.protocol_string)
      try:
        parsed = protocol.Parse(example.protocol_string)
        if example.valid:
          correct += 1
        else:
          self.fail(
              'Invalid protocol was parsed:\n%s' % example.protocol_string)
      except Exception as exn:
        if example.valid:
          self.fail(
              'Valid protocol failed to parse: %s\n%s'
              % (example.protocol_string, traceback.format_exc()))
        else:
          if logging.getLogger().getEffectiveLevel() <= 5:
            logging.debug('Expected error:\n%s', traceback.format_exc())
          else:
            logging.debug('Expected error: %r', exn)
          correct += 1

    self.assertEqual(
      correct,
      len(EXAMPLES),
      'Parse behavior correct on %d out of %d protocols.'
      % (correct, len(EXAMPLES)))

  def testInnerNamespaceSet(self):
    proto = protocol.Parse(HELLO_WORLD.protocol_string)
    self.assertEqual(proto.namespace, 'com.acme')
    greeting_type = proto.type_map['com.acme.Greeting']
    self.assertEqual(greeting_type.namespace, 'com.acme')

  def testInnerNamespaceNotRendered(self):
    proto = protocol.Parse(HELLO_WORLD.protocol_string)
    self.assertEqual('com.acme.Greeting', proto.types[0].fullname)
    self.assertEqual('Greeting', proto.types[0].name)
    # but there shouldn't be 'namespace' rendered to json on the inner type
    self.assertFalse('namespace' in proto.to_json()['types'][0])

  def testValidCastToStringAfterParse(self):
    """
    Test that the string generated by an Avro Protocol object is,
    in fact, a valid Avro protocol.
    """
    num_correct = 0
    for example in VALID_EXAMPLES:
      proto = protocol.Parse(example.protocol_string)
      try:
        protocol.Parse(str(proto))
        logging.debug(
            'Successfully reparsed protocol:\n%s',
            example.protocol_string)
        num_correct += 1
      except:
        logging.debug(
            'Failed to reparse protocol:\n%s',
            example.protocol_string)

    fail_msg = (
      'Cast to string success on %d out of %d protocols'
      % (num_correct, len(VALID_EXAMPLES)))
    self.assertEqual(num_correct, len(VALID_EXAMPLES), fail_msg)

  def testEquivalenceAfterRoundTrip(self):
    """
    1. Given a string, parse it to get Avro protocol "original".
    2. Serialize "original" to a string and parse that string
         to generate Avro protocol "round trip".
    3. Ensure "original" and "round trip" protocols are equivalent.
    """
    num_correct = 0
    for example in VALID_EXAMPLES:
      original_protocol = protocol.Parse(example.protocol_string)
      round_trip_protocol = protocol.Parse(str(original_protocol))

      if original_protocol == round_trip_protocol:
        num_correct += 1
        logging.debug(
            'Successful round-trip for protocol:\n%s',
            example.protocol_string)
      else:
        self.fail(
            'Round-trip failure for protocol:\n%s\nOriginal protocol:\n%s'
            % (example.protocol_string, str(original_protocol)))

    self.assertEqual(
        num_correct,
        len(VALID_EXAMPLES),
        'Round trip success on %d out of %d protocols.'
        % (num_correct, len(VALID_EXAMPLES)))


# ------------------------------------------------------------------------------


if __name__ == '__main__':
  raise Exception('Use run_tests.py')
