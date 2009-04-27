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
import simplejson

#The schema types
STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL, ARRAY, MAP, UNION, RECORD = range(12)

class Schema(object):
  """Base class for all Schema classes."""

  def __init__(self, type):
    self.__type = type

  def gettype(self):
    return self.__type

  def __eq__(self, other, seen=None):
    if self is other:
      return True
    return isinstance(other, Schema) and self.__type == other.__type

  def __hash__(self, seen=None):
    return self.__type.__hash__()

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
    str.write("\"fields\": [")
    count=0
    for k,v in self.__fields:
      str.write("{\"name\": \"")
      str.write(k)
      str.write("\", \"type\": ")
      str.write(v.str(names))
      str.write("}")
      count+=1
      if count < len(self.__fields):
        str.write(",")
    str.write("]}")
    return str.getvalue()

  def __eq__(self, other, seen={}):
    if self is other or seen.get(id(self)) is other:
      return True
    if isinstance(other, _RecordSchema):
      size = len(self.__fields)
      if len(other.__fields) != size:
        return False
      seen[id(self)] = other
      for i in range(0, size):
        if not self.__fields[i][1].__eq__(other.__fields[i][1], seen):
          return False
      return True
    else:
      return False

  def __hash__(self, seen=set()):
    if seen.__contains__(id(self)):
      return 0
    seen.add(id(self))
    hash = self.gettype().__hash__() 
    for field, fieldschm in self.__fields:
      hash = hash + fieldschm.__hash__(seen)
    return hash

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

  def __eq__(self, other, seen={}):
    if self is other or seen.get(id(self)) is other:
      return True
    seen[id(self)]= other
    return (isinstance(other, _ArraySchema) and 
            self.__elemtype.__eq__(other.__elemtype, seen))

  def __hash__(self, seen=set()):
    if seen.__contains__(id(self)):
      return 0
    seen.add(id(self))
    return self.gettype().__hash__() + self.__elemtype.__hash__(seen)

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

  def __eq__(self, other, seen={}):
    if self is other or seen.get(id(self)) is other:
      return True
    seen[id(self)]= other
    return (isinstance(other, _MapSchema) and 
            self.__ktype.__eq__(other.__ktype, seen) and 
            self.__vtype.__eq__(other.__vtype), seen)

  def __hash__(self, seen=set()):
    if seen.__contains__(id(self)):
      return 0
    seen.add(id(self))
    return (self.gettype().__hash__() + 
            self.__ktype.__hash__(seen) +
            self.__vtype.__hash__(seen))

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

  def __eq__(self, other, seen={}):
    if self is other or seen.get(id(self)) is other:
      return True
    seen[id(self)]= other
    if isinstance(other, _UnionSchema):
      size = len(self.__elemtypes)
      if len(other.__elemtypes) != size:
        return False
      for i in range(0, size):
        if not self.__elemtypes[i].__eq__(other.__elemtypes[i], seen):
          return False
      return True
    else:
      return False

  def __hash__(self, seen=set()):
    if seen.__contains__(id(self)):
      return 0
    seen.add(id(self))
    hash = self.gettype().__hash__() 
    for elem in self.__elemtypes:
      hash = hash + elem.__hash__(seen)
    return hash

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
    self.__defaults = names

  def get(self, key):
    val = dict.get(self, key)
    if val is None:
      val = self.__defaults.get(key)
    return val

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
      fieldsnode = obj.get("fields")
      if fieldsnode is None:
        raise SchemaParseException("Record has no fields: "+obj.__str__())
      for field in fieldsnode:
        fieldname = field.get("name")
        if fieldname is None:
          raise SchemaParseException("No field name: "+field.__str__())
        fieldtype = field.get("type")
        if fieldtype is None:
          raise SchemaParseException("No field type: "+field.__str__())
        fields.append((fieldname, _parse(fieldtype, names)))
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
  dict = simplejson.loads(json_string)
  return _parse(dict, _Names())
