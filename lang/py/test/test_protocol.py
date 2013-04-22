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
"""
Test the protocol parsing logic.
"""
import unittest
from avro import protocol

class ExampleProtocol(object):
  def __init__(self, protocol_string, valid, name='', comment=''):
    self._protocol_string = protocol_string
    self._valid = valid
    self._name = name or protocol_string # default to schema_string for name
    self._comment = comment

  # read-only properties
  protocol_string = property(lambda self: self._protocol_string)
  valid = property(lambda self: self._valid)
  name = property(lambda self: self._name)

  # read/write properties
  def set_comment(self, new_comment): self._comment = new_comment
  comment = property(lambda self: self._comment, set_comment)

#
# Example Protocols
#
HELLO_WORLD = ExampleProtocol("""\
{
  "namespace": "com.acme",
  "protocol": "HelloWorld",

  "types": [
    {"name": "Greeting", "type": "record", "fields": [
      {"name": "message", "type": "string"}]},
    {"name": "Curse", "type": "error", "fields": [
      {"name": "message", "type": "string"}]}
  ],

  "messages": {
    "hello": {
      "request": [{"name": "greeting", "type": "Greeting" }],
      "response": "Greeting",
      "errors": ["Curse"]
    }
  }
}
    """, True)
EXAMPLES = [
  HELLO_WORLD,
  ExampleProtocol("""\
{"namespace": "org.apache.avro.test",
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

     {"name": "TestError", "type": "error", "fields": [
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
         "request": [{"name": "arg1", "type": "int"}, {"name": "arg2", "type": "int"}],
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
    """, True),
  ExampleProtocol("""\
{"namespace": "org.apache.avro.test.namespace",
 "protocol": "TestNamespace",

 "types": [
     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
     {"name": "TestRecord", "type": "record",
      "fields": [ {"name": "hash", "type": "org.apache.avro.test.util.MD5"} ]
     },
     {"name": "TestError", "namespace": "org.apache.avro.test.errors",
      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
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
    """, True),
ExampleProtocol("""\
{"namespace": "org.apache.avro.test.namespace",
 "protocol": "TestImplicitNamespace",

 "types": [
     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
     {"name": "ReferencedRecord", "type": "record", 
       "fields": [ {"name": "foo", "type": "string"} ] },
     {"name": "TestRecord", "type": "record",
      "fields": [ {"name": "hash", "type": "org.apache.avro.test.util.MD5"},
                  {"name": "unqalified", "type": "ReferencedRecord"} ]
     },
     {"name": "TestError",
      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
     }
 ],

 "messages": {
     "echo": {
         "request": [{"name": "qualified", 
             "type": "org.apache.avro.test.namespace.TestRecord"}],
         "response": "TestRecord"
     },

     "error": {
         "request": [],
         "response": "null",
         "errors": ["org.apache.avro.test.namespace.TestError"]
     }

 }

}
    """, True),
ExampleProtocol("""\
{"namespace": "org.apache.avro.test.namespace",
 "protocol": "TestNamespaceTwo",

 "types": [
     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
     {"name": "ReferencedRecord", "type": "record", 
       "namespace": "org.apache.avro.other.namespace", 
       "fields": [ {"name": "foo", "type": "string"} ] },
     {"name": "TestRecord", "type": "record",
      "fields": [ {"name": "hash", "type": "org.apache.avro.test.util.MD5"},
                  {"name": "qualified", 
                    "type": "org.apache.avro.other.namespace.ReferencedRecord"} 
                ]
     },
     {"name": "TestError",
      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
     }
 ],

 "messages": {
     "echo": {
         "request": [{"name": "qualified", 
             "type": "org.apache.avro.test.namespace.TestRecord"}],
         "response": "TestRecord"
     },

     "error": {
         "request": [],
         "response": "null",
         "errors": ["org.apache.avro.test.namespace.TestError"]
     }

 }

}
    """, True),
ExampleProtocol("""\
{"namespace": "org.apache.avro.test.namespace",
 "protocol": "TestValidRepeatedName",

 "types": [
     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
     {"name": "ReferencedRecord", "type": "record", 
       "namespace": "org.apache.avro.other.namespace", 
       "fields": [ {"name": "foo", "type": "string"} ] },
     {"name": "ReferencedRecord", "type": "record", 
       "fields": [ {"name": "bar", "type": "double"} ] },
     {"name": "TestError",
      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
     }
 ],

 "messages": {
     "echo": {
         "request": [{"name": "qualified", 
             "type": "ReferencedRecord"}],
         "response": "org.apache.avro.other.namespace.ReferencedRecord"
     },

     "error": {
         "request": [],
         "response": "null",
         "errors": ["org.apache.avro.test.namespace.TestError"]
     }

 }

}
    """, True),
ExampleProtocol("""\
{"namespace": "org.apache.avro.test.namespace",
 "protocol": "TestInvalidRepeatedName",

 "types": [
     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
     {"name": "ReferencedRecord", "type": "record", 
       "fields": [ {"name": "foo", "type": "string"} ] },
     {"name": "ReferencedRecord", "type": "record", 
       "fields": [ {"name": "bar", "type": "double"} ] },
     {"name": "TestError",
      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
     }
 ],

 "messages": {
     "echo": {
         "request": [{"name": "qualified", 
             "type": "ReferencedRecord"}],
         "response": "org.apache.avro.other.namespace.ReferencedRecord"
     },

     "error": {
         "request": [],
         "response": "null",
         "errors": ["org.apache.avro.test.namespace.TestError"]
     }

 }

}
    """, False),
  ExampleProtocol("""\
{"namespace": "org.apache.avro.test",
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
    """, True),
  ExampleProtocol("""\
{
  "protocol" : "API",
  "namespace" : "xyz.api",
  "types" : [ {
    "type" : "enum",
    "name" : "Symbology",
    "namespace" : "xyz.api.product",
    "symbols" : [ "OPRA", "CUSIP", "ISIN", "SEDOL" ]
  }, {
    "type" : "record",
    "name" : "Symbol",
    "namespace" : "xyz.api.product",
    "fields" : [ {
      "name" : "symbology",
      "type" : "xyz.api.product.Symbology"
    }, {
      "name" : "symbol",
      "type" : "string"
    } ]
  }, {
    "type" : "record",
    "name" : "MultiSymbol",
    "namespace" : "xyz.api.product",
    "fields" : [ {
      "name" : "symbols",
      "type" : {
        "type" : "map",
        "values" : "xyz.api.product.Symbol"
      }
    } ]
  } ],
  "messages" : {
  }
}
    """, True),
]

VALID_EXAMPLES = [e for e in EXAMPLES if e.valid]

class TestProtocol(unittest.TestCase):
  def test_parse(self):
    num_correct = 0
    for example in EXAMPLES:
      try:
        protocol.parse(example.protocol_string)
        if example.valid: 
          num_correct += 1
        else:
          self.fail("Parsed invalid protocol: %s" % (example.name,))
      except Exception, e:
        if not example.valid: 
          num_correct += 1
        else:
          self.fail("Coudl not parse valid protocol: %s" % (example.name,))

    fail_msg = "Parse behavior correct on %d out of %d protocols." % \
      (num_correct, len(EXAMPLES))
    self.assertEqual(num_correct, len(EXAMPLES), fail_msg)

  def test_inner_namespace_set(self):
    print ''
    print 'TEST INNER NAMESPACE'
    print '==================='
    print ''
    proto = protocol.parse(HELLO_WORLD.protocol_string)
    self.assertEqual(proto.namespace, "com.acme")
    greeting_type = proto.types_dict['Greeting']
    self.assertEqual(greeting_type.namespace, 'com.acme')

  def test_inner_namespace_not_rendered(self):
    proto = protocol.parse(HELLO_WORLD.protocol_string)
    self.assertEqual('com.acme.Greeting', proto.types[0].fullname)
    self.assertEqual('Greeting', proto.types[0].name)
    # but there shouldn't be 'namespace' rendered to json on the inner type
    self.assertFalse('namespace' in proto.to_json()['types'][0])

  def test_valid_cast_to_string_after_parse(self):
    """
    Test that the string generated by an Avro Protocol object
    is, in fact, a valid Avro protocol.
    """
    print ''
    print 'TEST CAST TO STRING'
    print '==================='
    print ''

    num_correct = 0
    for example in VALID_EXAMPLES:
      protocol_data = protocol.parse(example.protocol_string)
      try:
        try:
          protocol.parse(str(protocol_data))
          debug_msg = "%s: STRING CAST SUCCESS" % example.name
          num_correct += 1
        except:
          debug_msg = "%s: STRING CAST FAILURE" % example.name
      finally:
        print debug_msg

    fail_msg = "Cast to string success on %d out of %d protocols" % \
      (num_correct, len(VALID_EXAMPLES))
    self.assertEqual(num_correct, len(VALID_EXAMPLES), fail_msg)

  def test_equivalence_after_round_trip(self):
    """
    1. Given a string, parse it to get Avro protocol "original".
    2. Serialize "original" to a string and parse that string
         to generate Avro protocol "round trip".
    3. Ensure "original" and "round trip" protocols are equivalent.
    """
    print ''
    print 'TEST ROUND TRIP'
    print '==============='
    print ''

    num_correct = 0
    for example in VALID_EXAMPLES:
      original_protocol = protocol.parse(example.protocol_string)
      round_trip_protocol = protocol.parse(str(original_protocol))

      if original_protocol == round_trip_protocol:
        num_correct += 1
        debug_msg = "%s: ROUND TRIP SUCCESS" % example.name
      else:       
        self.fail("Round trip failure: %s %s %s", (example.name, example.protocol_string, str(original_protocol)))

    fail_msg = "Round trip success on %d out of %d protocols" % \
      (num_correct, len(VALID_EXAMPLES))
    self.assertEqual(num_correct, len(VALID_EXAMPLES), fail_msg)

if __name__ == '__main__':
  unittest.main()
