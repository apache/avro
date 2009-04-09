#Licensed to the Apache Software Foundation (ASF) under one
#or more contributor license agreements.  See the NOTICE file
#distributed with this work for additional information
#regarding copyright ownership.  The ASF licenses this file
#to you under the Apache License, Version 2.0 (the
#"License"); you may not use this file except in compliance
#with the License.  You may obtain a copy of the License at
#
#http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

""" Contains the Schema classes.
A schema may be one of:
  An record, mapping field names to field value data;
  An array of values, all of the same schema;
  A map containing key/value pairs, each of a declared schema;
  A union of other schemas;
  A unicode string;
  A sequence of bytes;
  A 32-bit signed int;
  A 64-bit signed long;
  A 32-bit floating-point float;
  A 64-bit floating-point double; or
  A boolean."""

import cStringIO
import odict
import avro.jsonparser as jsonparser

#The schema types
STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL, ARRAY, MAP, UNION, RECORD = range(12)

class Schema(object):
  """Base class for all Schema classes."""

  def __init__(self, type):
    self.__type = type

  def gettype(self):
    return self.__type

class _StringSchema(Schema):
  def __init__(self):
    Schema.__init__(self, STRING)

  def str(self, names):
    return "\"string\""

class _BytesSchema(Schema):
  def __init__(self):
    Schema.__init__(self, BYTES)

  def str(self, names):
    return "\"bytes\""

class _IntSchema(Schema):
  def __init__(self):
    Schema.__init__(self, INT)

  def str(self, names):
    return "\"int\""

class _LongSchema(Schema):
  def __init__(self):
    Schema.__init__(self, LONG)

  def str(self, names):
    return "\"long\""

class _FloatSchema(Schema):
  def __init__(self):
    Schema.__init__(self, FLOAT)

  def str(self, names):
    return "\"float\""

class _DoubleSchema(Schema):
  def __init__(self):
    Schema.__init__(self, DOUBLE)

  def str(self, names):
    return "\"double\""

class _BooleanSchema(Schema):
  def __init__(self):
    Schema.__init__(self, BOOLEAN)

  def str(self, names):
    return "\"boolean\""

class _NullSchema(Schema):
  def __init__(self):
    Schema.__init__(self, NULL)

  def str(self, names):
    return "\"null\""

class _RecordSchema(Schema):
  def __init__(self, fields, name=None, namespace=None, iserror=False):
    Schema.__init__(self, RECORD)
    self.__name = name
    self.__namespace = namespace
    self.__fields = fields
    self.__iserror = iserror

  def getname(self):
    return self.__name

  def getnamespace(self):
    return self.__namespace

  def getfields(self):
    return self.__fields

  def iserror(self):
    return self.__iserror

  def str(self, names):
    if names.get(self.__name) is self:
      return "\""+self.__name+"\""
    elif self.__name is not None:
      names[self.__name] = self
    str = cStringIO.StringIO()
    str.write("{\"type\": \"")
    if self.iserror():
      str.write("error")
    else:
      str.write("record")
    str.write("\", ")
    if self.__name is not None:
      str.write("\"name\": \""+self.__name+"\", ")
    #if self.__namespace is not None:
      #str.write("\"namespace\": \""+self.__namespace+"\", ")
    str.write("\"fields\": {")
    count=0
    for k,v in self.__fields:
      str.write("\"")
      str.write(k)
      str.write("\": ")
      str.write(v.str(names))
      count+=1
      if count < len(self.__fields):
        str.write(",")
    str.write("}}")
    return str.getvalue()

class _ArraySchema(Schema):
  def __init__(self, elemtype):
    Schema.__init__(self, ARRAY)
    self.__elemtype = elemtype

  def getelementtype(self):
    return self.__elemtype

  def str(self, names):
    str = cStringIO.StringIO()
    str.write("{\"type\": \"array\", \"items\": ")
    str.write(self.__elemtype.str(names))
    str.write("}")
    return str.getvalue()

class _MapSchema(Schema):
  def __init__(self, keytype, valuetype):
    Schema.__init__(self, MAP)
    self.__ktype = keytype
    self.__vtype = valuetype

  def getkeytype(self):
    return self.__ktype

  def getvaluetype(self):
    return self.__vtype


  def str(self, names):
    str = cStringIO.StringIO()
    str.write("{\"type\": \"map\", \"keys\":  ")
    str.write(self.__ktype.str(names))
    str.write(", \"values\": ");
    str.write(self.__vtype.str(names));
    str.write("}")
    return str.getvalue()

class _UnionSchema(Schema):
  def __init__(self, elemtypes):
    Schema.__init__(self, UNION)
    self.__elemtypes = elemtypes

  def getelementtypes(self):
    return self.__elemtypes

  def str(self, names):
    str = cStringIO.StringIO()
    str.write("[")
    count=0
    for elemtype in self.__elemtypes:
      str.write(elemtype.str(names))
      count+=1
      if count < len(self.__elemtypes):
        str.write(",")
    str.write("]")
    return str.getvalue()

_PRIMITIVES = {'string':_StringSchema(),
        'bytes':_BytesSchema(),
        'int':_IntSchema(),
        'long':_LongSchema(),
        'float':_FloatSchema(),
        'double':_DoubleSchema(),
        'boolean':_BooleanSchema(),
        'null':_NullSchema()}    

class _Names(dict):
  def __init__(self, names=_PRIMITIVES):
    self.update(names)

  def __setitem__(self, key, val):
    if dict.get(self, key) is not None:
      raise SchemaParseException("Can't redefine: "+ key.__str__())
    dict.__setitem__(self, key, val)

class AvroException(Exception):
  pass

class SchemaParseException(AvroException):
  pass

def _parse(obj, names):
  if isinstance(obj, unicode):
    schema = names.get(obj)
    if schema is not None:
      return schema
    else:
      raise SchemaParseException("Undefined name: "+obj.__str__())
  elif isinstance(obj, dict):
    type = obj.get("type")
    if type is None:
      raise SchemaParseException("No type: "+obj.__str__())
    if type == "record" or type == "error":
      name = obj.get("name")
      namespace = obj.get("namespace")
      fields = list()
      schema = _RecordSchema(fields, name, namespace, type == "error")
      if name is not None:
        names[name] = schema
      for k,v in obj.get("fields").items():
        fields.append((k, _parse(v, names)))
      return schema
    elif type == "array":
      return _ArraySchema(_parse(obj.get("items"), names))
    elif type == "map":
      return _MapSchema(_parse(obj.get("keys"), names), 
                       _parse(obj.get("values"), names))
    else:
      raise SchemaParseException("Type not yet supported: "+type.__str__())
  elif isinstance(obj, list):
    elemtypes = list()
    for elemtype in obj:
      elemtypes.append(_parse(elemtype, names))
    return _UnionSchema(elemtypes)
  else:
    raise SchemaParseException("Schema not yet supported:" + obj.__str__())

def stringval(schm):
  """Returns the string representation of the schema instance."""
  return schm.str(_Names())

def parse(json_string):
  """Constructs the Schema from the json text."""
  dict = jsonparser.parse(json_string)
  return _parse(dict, _Names())
