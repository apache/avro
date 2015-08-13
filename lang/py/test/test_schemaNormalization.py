"""
Test the schemaNormalization
"""
import unittest
import json
import ctypes
import set_avro_test_path

from avro import schema
from avro import schemaNormalization

def print_test_name(test_name):
  print ''
  print test_name
  print '=' * len(test_name)
  print ''

class ExampleSchema(object):
  def __init__(self, schema_string, valid, name='', comment=''):
    self._schema_string = schema_string
    self._valid = valid
    self._name = name or schema_string # default to schema_string for name
    self.comment = comment

  @property
  def schema_string(self):
    return self._schema_string

  @property
  def valid(self):
    return self._valid

  @property
  def name(self):
    return self._name

#
# Example Schemas
#

def make_primitive_examples():
  examples = []
  for type in schema.PRIMITIVE_TYPES:
    examples.append(ExampleSchema('"%s"' % type, True))
    examples.append(ExampleSchema('{"type": "%s"}' % type, True))
  return examples

PRIMITIVE_EXAMPLES = [
  ExampleSchema('"True"', False),
  ExampleSchema('True', False),
  ExampleSchema('{"no_type": "test"}', False),
  ExampleSchema('{"type": "panther"}', False),
] + make_primitive_examples()

FIXED_EXAMPLES = [
  ExampleSchema('{"type": "fixed", "name": "Test", "size": 1}', True),
  ExampleSchema("""\
    {"type": "fixed",
     "name": "MyFixed",
     "namespace": "org.apache.hadoop.avro",
     "size": 1}
    """, True),
  ExampleSchema("""\
    {"type": "fixed",
     "name": "Missing size"}
    """, False),
  ExampleSchema("""\
    {"type": "fixed",
     "size": 314}
    """, False),
]

ENUM_EXAMPLES = [
  ExampleSchema('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', True),
  ExampleSchema("""\
    {"type": "enum",
     "name": "Status",
     "symbols": "Normal Caution Critical"}
    """, False),
  ExampleSchema("""\
    {"type": "enum",
     "name": [ 0, 1, 1, 2, 3, 5, 8 ],
     "symbols": ["Golden", "Mean"]}
    """, False),
  ExampleSchema("""\
    {"type": "enum",
     "symbols" : ["I", "will", "fail", "no", "name"]}
    """, False),
  ExampleSchema("""\
    {"type": "enum",
     "name": "Test"
     "symbols" : ["AA", "AA"]}
    """, False),
]

ARRAY_EXAMPLES = [
  ExampleSchema('{"type": "array", "items": "long"}', True),
  ExampleSchema("""\
    {"type": "array",
     "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}
    """, True),
]

MAP_EXAMPLES = [
  ExampleSchema('{"type": "map", "values": "long"}', True),
  ExampleSchema("""\
    {"type": "map",
     "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}
    """, True),
]

UNION_EXAMPLES = [
  ExampleSchema('["string", "null", "long"]', True),
  ExampleSchema('["null", "null"]', False),
  ExampleSchema('["long", "long"]', False),
  ExampleSchema("""\
    [{"type": "array", "items": "long"}
     {"type": "array", "items": "string"}]
    """, False),
]

RECORD_EXAMPLES = [
  ExampleSchema("""\
    {"type": "record",
     "name": "Test",
     "fields": [{"name": "f",
                 "type": "long"}]}
    """, True),
  ExampleSchema("""\
    {"type": "error",
     "name": "Test",
     "fields": [{"name": "f",
                 "type": "long"}]}
    """, True),
  ExampleSchema("""\
    {"type": "record",
     "name": "Node",
     "fields": [{"name": "label", "type": "string"},
                {"name": "children",
                 "type": {"type": "array", "items": "Node"}}]}
    """, True),
  ExampleSchema("""\
    {"type": "record",
     "name": "Lisp",
     "fields": [{"name": "value",
                 "type": ["null", "string",
                          {"type": "record",
                           "name": "Cons",
                           "fields": [{"name": "car", "type": "Lisp"},
                                      {"name": "cdr", "type": "Lisp"}]}]}]}
    """, True),
  ExampleSchema("""\
    {"type": "record",
     "name": "HandshakeRequest",
     "namespace": "org.apache.avro.ipc",
     "fields": [{"name": "clientHash",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "clientProtocol", "type": ["null", "string"]},
                {"name": "serverHash", "type": "MD5"},
                {"name": "meta", 
                 "type": ["null", {"type": "map", "values": "bytes"}]}]}
    """, True),
  ExampleSchema("""\
    {"type": "record",
     "name": "HandshakeResponse",
     "namespace": "org.apache.avro.ipc",
     "fields": [{"name": "match",
                 "type": {"type": "enum",
                          "name": "HandshakeMatch",
                          "symbols": ["BOTH", "CLIENT", "NONE"]}},
                {"name": "serverProtocol", "type": ["null", "string"]},
                {"name": "serverHash",
                 "type": ["null",
                          {"name": "MD5", "size": 16, "type": "fixed"}]},
                {"name": "meta",
                 "type": ["null", {"type": "map", "values": "bytes"}]}]}
    """, True),
  ExampleSchema("""\
    {"type": "record",
     "name": "Interop",
     "namespace": "org.apache.avro",
     "fields": [{"name": "intField", "type": "int"},
                {"name": "longField", "type": "long"},
                {"name": "stringField", "type": "string"},
                {"name": "boolField", "type": "boolean"},
                {"name": "floatField", "type": "float"},
                {"name": "doubleField", "type": "double"},
                {"name": "bytesField", "type": "bytes"},
                {"name": "nullField", "type": "null"},
                {"name": "arrayField",
                 "type": {"type": "array", "items": "double"}},
                {"name": "mapField",
                 "type": {"type": "map",
                          "values": {"name": "Foo",
                                     "type": "record",
                                     "fields": [{"name": "label",
                                                 "type": "string"}]}}},
                {"name": "unionField",
                 "type": ["boolean",
                          "double",
                          {"type": "array", "items": "bytes"}]},
                {"name": "enumField",
                 "type": {"type": "enum",
                          "name": "Kind",
                          "symbols": ["A", "B", "C"]}},
                {"name": "fixedField",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "recordField",
                 "type": {"type": "record",
                          "name": "Node",
                          "fields": [{"name": "label", "type": "string"},
                                     {"name": "children",
                                      "type": {"type": "array",
                                               "items": "Node"}}]}}]}
    """, True),
  ExampleSchema("""\
    {"type": "record",
     "name": "ipAddr",
     "fields": [{"name": "addr", 
                 "type": [{"name": "IPv6", "type": "fixed", "size": 16},
                          {"name": "IPv4", "type": "fixed", "size": 4}]}]}
    """, True),
  ExampleSchema("""\
    {"type": "record",
     "name": "Address",
     "fields": [{"type": "string"},
                {"type": "string", "name": "City"}]}
    """, False),
  ExampleSchema("""\
    {"type": "record",
     "name": "Event",
     "fields": [{"name": "Sponsor"},
                {"name": "City", "type": "string"}]}
    """, False),
  ExampleSchema("""\
    {"type": "record",
     "fields": "His vision, from the constantly passing bars,"
     "name", "Rainer"}
    """, False),
  ExampleSchema("""\
    {"name": ["Tom", "Jerry"],
     "type": "record",
     "fields": [{"name": "name", "type": "string"}]}
    """, False),
]

DOC_EXAMPLES = [
  ExampleSchema("""\
    {"type": "record",
     "name": "TestDoc",
     "doc":  "Doc string",
     "fields": [{"name": "name", "type": "string", 
                 "doc" : "Doc String"}]}
    """, True),
  ExampleSchema("""\
    {"type": "enum", "name": "Test", "symbols": ["A", "B"],
     "doc": "Doc String"}
    """, True),
]

OTHER_PROP_EXAMPLES = [
  ExampleSchema("""\
    {"type": "record",
     "name": "TestRecord",
     "cp_string": "string",
     "cp_int": 1,
     "cp_array": [ 1, 2, 3, 4],
     "fields": [ {"name": "f1", "type": "string", "cp_object": {"a":1,"b":2} },
                 {"name": "f2", "type": "long", "cp_null": null} ]}
    """, True),
  ExampleSchema("""\
     {"type": "map", "values": "long", "cp_boolean": true}
    """, True),
  ExampleSchema("""\
    {"type": "enum",
     "name": "TestEnum",
     "symbols": [ "one", "two", "three" ],
     "cp_float" : 1.0 }
    """,True),
  ExampleSchema("""\
    {"type": "long",
     "date": "true"}
    """, True)
]

EXAMPLES = PRIMITIVE_EXAMPLES
EXAMPLES += FIXED_EXAMPLES
EXAMPLES += ENUM_EXAMPLES
EXAMPLES += ARRAY_EXAMPLES
EXAMPLES += MAP_EXAMPLES
EXAMPLES += UNION_EXAMPLES
EXAMPLES += RECORD_EXAMPLES
EXAMPLES += DOC_EXAMPLES

VALID_EXAMPLES = [e for e in EXAMPLES if e.valid]

KNOWN_RESULTS = [
{ ##000
'INPUT':'"null"'
,'canonical':'"null"'
,'fingerprint': 7195948357588979594

},{ ##001
'INPUT':"""{"type":"null"}"""
,'canonical':'"null"'
,'fingerprint': 7195948357588979594

},{ ##002
'INPUT':'"boolean"'
,'canonical':'"boolean"'
,'fingerprint': -6970731678124411036

},{ ##003
'INPUT':"""{"type":"boolean"}"""
,'canonical':'"boolean"'
,'fingerprint': -6970731678124411036

},{ ##004
'INPUT':'"int"'
,'canonical':'"int"'
,'fingerprint': 8247732601305521295

},{ ##005
'INPUT':"""{"type":"int"}"""
,'canonical':'"int"'
,'fingerprint': 8247732601305521295

},{ ##006
'INPUT':'"long"'
,'canonical':'"long"'
,'fingerprint': -3434872931120570953

},{ ##007
'INPUT':"""{"type":"long"}"""
,'canonical':'"long"'
,'fingerprint': -3434872931120570953

},{ ##008
'INPUT':'"float"'
,'canonical':'"float"'
,'fingerprint': 5583340709985441680

},{ ##009
'INPUT':"""{"type":"float"}"""
,'canonical':'"float"'
,'fingerprint': 5583340709985441680

},{ ##010
'INPUT':'"double"'
,'canonical':'"double"'
,'fingerprint': -8181574048448539266

},{ ##011
'INPUT':"""{"type":"double"}"""
,'canonical':'"double"'
,'fingerprint': -8181574048448539266

},{ ##012
'INPUT':'"bytes"'
,'canonical':'"bytes"'
,'fingerprint': 5746618253357095269

},{ ##013
'INPUT':"""{"type":"bytes"}"""
,'canonical':'"bytes"'
,'fingerprint': 5746618253357095269

},{ ##014
'INPUT':'"string"'
,'canonical':'"string"'
,'fingerprint': -8142146995180207161

},{ ##015
'INPUT':"""{"type":"string"}"""
,'canonical':'"string"'
,'fingerprint': -8142146995180207161

},{ ##016
'INPUT':"""[  ]"""
,'canonical':"""[]"""
,'fingerprint': -1241056759729112623

},{ ##017
'INPUT':"""[ "int"  ]"""
,'canonical':"""["int"]"""
,'fingerprint': -5232228896498058493

},{ ##018
'INPUT':"""[ "int" , {"type":"boolean"} ]"""
,'canonical':"""["int","boolean"]"""
,'fingerprint': 5392556393470105090

},{ ##019
'INPUT':"""{"fields":[], "type":"record", "name":"foo"}"""
,'canonical':"""{"name":"foo","type":"record","fields":[]}"""
,'fingerprint': -4824392279771201922

},{ ##020
'INPUT':"""{"fields":[], "type":"record", "name":"foo", "namespace":"x.y"}"""
,'canonical':"""{"name":"x.y.foo","type":"record","fields":[]}"""
,'fingerprint': 5916914534497305771

},{ ##021
'INPUT':"""{"fields":[], "type":"record", "name":"a.b.foo", "namespace":"x.y"}"""
,'canonical':"""{"name":"a.b.foo","type":"record","fields":[]}"""
,'fingerprint': -4616218487480524110

},{ ##022
'INPUT':"""{"fields":[], "type":"record", "name":"foo", "doc":"Useful info"}"""
,'canonical':"""{"name":"foo","type":"record","fields":[]}"""
,'fingerprint': -4824392279771201922

},{ ##023
'INPUT':"""{"fields":[], "type":"record", "name":"foo", "aliases":["foo","bar"]}"""
,'canonical':"""{"name":"foo","type":"record","fields":[]}"""
,'fingerprint': -4824392279771201922

},{ ##024
'INPUT':"""{"fields":[], "type":"record", "name":"foo", "doc":"foo", "aliases":["foo","bar"]}"""
,'canonical':"""{"name":"foo","type":"record","fields":[]}"""
,'fingerprint': -4824392279771201922

},{ ##025
'INPUT':"""{"fields":[{"type":{"type":"boolean"}, "name":"f1"}], "type":"record", "name":"foo"}"""
,'canonical':"""{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"}]}"""
,'fingerprint': 7843277075252814651

},{ ##026
'INPUT':"""{ "fields":[{"type":"boolean", "aliases":[], "name":"f1", "default":true},
{"order":"descending","name":"f2","doc":"Hello","type":"int"}],"type":"record", "name":"foo"}"""
,'canonical':"""{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"},{"name":"f2","type":"int"}]}"""
,'fingerprint': -4860222112080293046

},{ ##027
'INPUT':"""{"type":"enum", "name":"foo", "symbols":["A1"]}"""
,'canonical':"""{"name":"foo","type":"enum","symbols":["A1"]}"""
,'fingerprint': -6342190197741309591

},{ ##028
'INPUT':"""{"namespace":"x.y.z", "type":"enum", "name":"foo", "doc":"foo bar", "symbols":["A1", "A2"]}"""
,'canonical':"""{"name":"x.y.z.foo","type":"enum","symbols":["A1","A2"]}"""
,'fingerprint': -4448647247586288245

},{ ##029
'INPUT':"""{"name":"foo","type":"fixed","size":15}"""
,'canonical':"""{"name":"foo","type":"fixed","size":15}"""
,'fingerprint': 1756455273707447556

},{ ##030
'INPUT':"""{"namespace":"x.y.z", "type":"fixed", "name":"foo", "doc":"foo bar", "size":32}"""
,'canonical':"""{"name":"x.y.z.foo","type":"fixed","size":32}"""
,'fingerprint': -3064184465700546786

},{ ##031
'INPUT':"""{ "items":{"type":"null"}, "type":"array"}"""
,'canonical':"""{"type":"array","items":"null"}"""
,'fingerprint': -589620603366471059

},{ ##032
'INPUT':"""{ "values":"string", "type":"map"}"""
,'canonical':"""{"type":"map","values":"string"}"""
,'fingerprint': -8732877298790414990

},{ ##033
'INPUT': """{"name":"PigValue","type":"record",
   "fields":[{"name":"value", "type":["null", "int", "long", "PigValue"]}]}"""
,'canonical':"""{"name":"PigValue","type":"record","fields":[{"name":"value","type":["null","int","long","PigValue"]}]}"""
,'fingerprint': -1759257747318642341
}
]

# TODO(hammer): refactor into harness for examples
# TODO(hammer): pretty-print detailed output
# TODO(hammer): make verbose flag
# TODO(hammer): show strack trace to user
# TODO(hammer): use logging module?
class TestSchemaNormalizaton(unittest.TestCase):

  def test_1_init(self):

    
    for example in VALID_EXAMPLES:
      SN = schemaNormalization.SchemaNormalization()  
      schema_obj = schema.parse(example._schema_string)
      print "original:"+str(schema_obj)
      canonical_schema_str = SN.toParsingForm(schema_obj)
      print "canonical:"+canonical_schema_str
      schema_obj2 = schema.parse(canonical_schema_str)


  def test_2_recurs(self):
    for example in VALID_EXAMPLES:
      SN = schemaNormalization.SchemaNormalization()
      
      schema_obj = schema.parse(example._schema_string)
      print "original :               "+str(schema_obj)
      
      canonical_schema_str = SN.toParsingForm(schema_obj)
      schema_obj2 = schema.parse(canonical_schema_str)
      print "canonical:               "+str(schema_obj2)

      
      canonical_schema_str2 = SN.toParsingForm(schema_obj2)
      schema_obj3 = schema.parse(canonical_schema_str2)
      print "canonical from canonical:"+str(schema_obj3)

      self.assertEqual(schema_obj2.to_json(),schema_obj3.to_json())

  def test_3_fingerprint64(self):
    for example in VALID_EXAMPLES:
      SN = schemaNormalization.SchemaNormalization()
        
      schema_obj = schema.parse(example._schema_string)
      print "original :               "+str(schema_obj)
      print "fingerprint64:%d"%(SN.parsingFingerprint64(schema_obj))


  def test_3_fingerprint64(self):
    for example in VALID_EXAMPLES:
      SN = schemaNormalization.SchemaNormalization()
        
      schema_obj = schema.parse(example._schema_string)
      print "original :               "+str(schema_obj)
      fg1 = SN.parsingFingerprint64(schema_obj)
      fg2 = SN.parsingFingerprint("CRC-64-AVRO",schema_obj)
      md5 = SN.parsingFingerprint("MD5",schema_obj)
      sha256 = SN.parsingFingerprint("sha-256",schema_obj)
      print "fingerprint64:%s"%(fg1)
      print "fingerprint CRC-64-AVRO :%s"%(fg2)
      self.assertEqual(fg1,fg2)
      
      print "fingerprint MD5 :%s"%(md5)
      print "fingerprint SHA256 :%s"%(sha256)


  def test_4_known_results(self):


    print "---"*200
    for result in KNOWN_RESULTS:
      print result
      sch = result['INPUT']
      SN = schemaNormalization.SchemaNormalization()
      jsonobj =   json.loads(sch)
      print jsonobj
      canonical = SN._build(jsonobj)
      print canonical
      
      fg64 = ctypes.c_int64(long(SN.fingerprint64(canonical),16)).value
      print fg64
      
      self.assertEqual(str(canonical),result['canonical'])
      
      self.assertEqual(fg64,result['fingerprint'])
      
      
if __name__ == '__main__':
  unittest.main()
