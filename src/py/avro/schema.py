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
Contains the Schema classes.

A schema may be one of:
  An record, mapping field names to field value data;
  An enum, containing one of a small set of symbols;
  An array of values, all of the same schema;
  A map containing string/value pairs, each of a declared schema;
  A union of other schemas;
  A fixed sized binary object;
  A unicode string;
  A sequence of bytes;
  A 32-bit signed int;
  A 64-bit signed long;
  A 32-bit floating-point float;
  A 64-bit floating-point double;
  A boolean; or
  Null.
"""

import cStringIO
# Use simplejson or Python 2.6 json, prefer simplejson.
try:
  import simplejson as json
except ImportError:
  import json

import odict

# The schema types
STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL, \
ARRAY, MAP, UNION, FIXED, RECORD, ENUM = range(14)

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
    return '"string"'

class _BytesSchema(Schema):
  def __init__(self):
    Schema.__init__(self, BYTES)

  def str(self, names):
    return '"bytes"'

class _IntSchema(Schema):
  def __init__(self):
    Schema.__init__(self, INT)

  def str(self, names):
    return '"int"'

class _LongSchema(Schema):
  def __init__(self):
    Schema.__init__(self, LONG)

  def str(self, names):
    return '"long"'

class _FloatSchema(Schema):
  def __init__(self):
    Schema.__init__(self, FLOAT)

  def str(self, names):
    return '"float"'

class _DoubleSchema(Schema):
  def __init__(self):
    Schema.__init__(self, DOUBLE)

  def str(self, names):
    return '"double"'

class _BooleanSchema(Schema):
  def __init__(self):
    Schema.__init__(self, BOOLEAN)

  def str(self, names):
    return '"boolean"'

class _NullSchema(Schema):
  def __init__(self):
    Schema.__init__(self, NULL)

  def str(self, names):
    return '"null"'

class NamedSchema(Schema):
  """Named Schemas include Record, Enum, and Fixed."""
  def __init__(self, type, name, space):
    Schema.__init__(self, type)
    self.__name = name
    self.__space = space

  def getname(self):
    return self.__name

  def getspace(self):
    return self.__space

  def equalnames(self, other):
    if other is None:
      return False
    if (self.__name == other.__name and self.__space == other.__space):
      return True
    return False

  def namestring(self):
    str = cStringIO.StringIO()
    str.write('"name": "' + self.__name + '", ')
    if self.__space is not None:
      str.write('"namespace": "' + self.__space + '", ')
    return str.getvalue()

  def __hash__(self, seen=None):
    hash = self.gettype().__hash__()
    hash += self.__name.__hash__()
    if self.__space is not None:
      hash += self.__space.__hash__()
    return hash

class Field(object):
  def __init__(self, name, schema, defaultvalue=None):
    self.__name = name
    self.__schema = schema
    self.__defaultvalue = defaultvalue

  def getname(self):
    return self.__name

  def getschema(self):
    return self.__schema

  def getdefaultvalue(self):
    return self.__defaultvalue

  def __eq__(self, other, seen={}):
    return (self.__name == other.__name and
            self.__schema.__eq__(other.__schema, seen) and 
            self.__defaultvalue == other.__defaultvalue)

class _RecordSchema(NamedSchema):
  def __init__(self, fields, name=None, space=None, iserror=False):
    NamedSchema.__init__(self, RECORD, name, space)
    self.__fields = fields
    self.__iserror = iserror

  def getfields(self):
    return self.__fields

  def iserror(self):
    return self.__iserror

  def str(self, names):
    if names.get(self.getname()) is self:
      return '"%s"' % self.getname()
    elif self.getname() is not None:
      names[self.getname()] = self
    str = cStringIO.StringIO()
    str.write('{"type": "')
    if self.iserror():
      str.write("error")
    else:
      str.write("record")
    str.write('", ')
    str.write(self.namestring())
    str.write('"fields": [')
    count = 0
    for field in self.__fields.values():
      str.write('{"name": "')
      str.write(field.getname())
      str.write('", "type": ')
      str.write(field.getschema().str(names))
      if field.getdefaultvalue() is not None:
        str.write(', "default": ')
        str.write(repr(field.getdefaultvalue()))
      str.write('}')
      count += 1
      if count < len(self.__fields):
        str.write(',')
    str.write(']}')
    return str.getvalue()

  def __eq__(self, other, seen={}):
    if self is other or seen.get(id(self)) is other:
      return True
    if isinstance(other, _RecordSchema) and self.equalnames(other):
      size = len(self.__fields)
      if len(other.__fields) != size:
        return False
      seen[id(self)] = other
      for field in self.__fields.values():
        if not field.__eq__(other.__fields.get(field.getname()), seen):
          return False
      return True
    else:
      return False

  def __hash__(self, seen=set()):
    if seen.__contains__(id(self)):
      return 0
    seen.add(id(self))
    hash = NamedSchema.__hash__(self, seen)
    for field in self.__fields.values():
      hash = hash + field.getschema().__hash__(seen)
    return hash

class _ArraySchema(Schema):
  def __init__(self, elemtype):
    Schema.__init__(self, ARRAY)
    self.__elemtype = elemtype

  def getelementtype(self):
    return self.__elemtype

  def str(self, names):
    str = cStringIO.StringIO()
    str.write('{"type": "array", "items": ')
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
  def __init__(self, valuetype):
    Schema.__init__(self, MAP)
    self.__vtype = valuetype

  def getvaluetype(self):
    return self.__vtype

  def str(self, names):
    str = cStringIO.StringIO()
    str.write('{"type": "map", "values": ')
    str.write(self.__vtype.str(names));
    str.write("}")
    return str.getvalue()

  def __eq__(self, other, seen={}):
    if self is other or seen.get(id(self)) is other:
      return True
    seen[id(self)] = other
    return (isinstance(other, _MapSchema) and
            self.__vtype.__eq__(other.__vtype), seen)

  def __hash__(self, seen=set()):
    if seen.__contains__(id(self)):
      return 0
    seen.add(id(self))
    return (self.gettype().__hash__() +
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
    count = 0
    for elemtype in self.__elemtypes:
      str.write(elemtype.str(names))
      count += 1
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

class _EnumSchema(NamedSchema):
  def __init__(self, name, space, symbols):
    NamedSchema.__init__(self, ENUM, name, space)
    self.__symbols = symbols
    self.__ordinals = dict()
    for i, symbol in enumerate(symbols):
      self.__ordinals[symbol] = i

  def getenumsymbols(self):
    return self.__symbols

  def getenumordinal(self, symbol):
    return self.__ordinals.get(symbol)

  def str(self, names):
    if names.get(self.getname()) is self:
      return '"%s"' % self.getname()
    elif self.getname() is not None:
      names[self.getname()] = self
    str = cStringIO.StringIO()
    str.write('{"type": "enum", ')
    str.write(self.namestring())
    str.write('"symbols": [')
    count = 0
    for symbol in self.__symbols:
      str.write('"%s"' % symbol)
      count += 1
      if count < len(self.__symbols):
        str.write(',')
    str.write(']}')
    return str.getvalue()

  def __eq__(self, other, seen={}):
    if self is other or seen.get(id(self)) is other:
      return True
    if isinstance(other, _EnumSchema) and self.equalnames(other):
      size = len(self.__symbols)
      if len(other.__symbols) != size:
        return False
      seen[id(self)] = other
      for i in range(0, size):
        if not self.__symbols[i].__eq__(other.__symbols[i]):
          return False
      return True
    else:
      return False

  def __hash__(self, seen=set()):
    if seen.__contains__(id(self)):
      return 0
    seen.add(id(self))
    hash = NamedSchema.__hash__(self, seen)
    for symbol in self.__symbols:
      hash += symbol.__hash__()
    return hash

class _FixedSchema(NamedSchema):
  def __init__(self, name, space, size):
    NamedSchema.__init__(self, FIXED, name, space)
    self.__size = size

  def getsize(self):
    return self.__size

  def str(self, names):
    if names.get(self.getname()) is self:
      return '"%s"' % self.getname()
    elif self.getname() is not None:
      names[self.getname()] = self
    str = cStringIO.StringIO()
    str.write('{"type": "fixed", ')
    str.write(self.namestring())
    str.write('"size": ' + repr(self.__size) + '}')
    return str.getvalue()

  def __eq__(self, other, seen=None):
    if self is other:
      return True
    if (isinstance(other, _FixedSchema) and self.equalnames(other) 
        and self.__size == other.__size):
      return True
    return False

  def __hash__(self, seen=None):
    return NamedSchema.__hash__(self, seen) + self.__size.__hash__()

_PRIMITIVES = {'string':_StringSchema(),
        'bytes':_BytesSchema(),
        'int':_IntSchema(),
        'long':_LongSchema(),
        'float':_FloatSchema(),
        'double':_DoubleSchema(),
        'boolean':_BooleanSchema(),
        'null':_NullSchema()}    

class _Names(odict.OrderedDict):
  def __init__(self, names=_PRIMITIVES):
    odict.OrderedDict.__init__(self)
    self.__defaults = names

  def get(self, key):
    val = odict.OrderedDict.get(self, key)
    if val is None:
      val = self.__defaults.get(key)
    return val

  def __setitem__(self, key, val):
    if odict.OrderedDict.get(self, key) is not None:
      raise SchemaParseException("Can't redefine: " + key.__str__())
    odict.OrderedDict.__setitem__(self, key, val)

class AvroException(Exception):
  pass

class SchemaParseException(AvroException):
  pass

def _parse(obj, names):
  if isinstance(obj, basestring):
    schema = names.get(obj)
    if schema is not None:
      return schema
    else:
      raise SchemaParseException("Undefined name: " + obj.__str__())
  elif isinstance(obj, dict):
    type = obj.get("type")
    if type is None:
      raise SchemaParseException("No type: " + obj.__str__())
    if (type == "record" or type == "error" or 
        type == "enum" or type == "fixed"):
      name = obj.get("name")
      space = obj.get("namespace")
      if name is None:
        raise SchemaParseException("No name in schema: " + obj.__str__())
      if type == "record" or type == "error":
        fields = odict.OrderedDict()
        schema = _RecordSchema(fields, name, space, type == "error")
        names[name] = schema
        fieldsnode = obj.get("fields")
        if fieldsnode is None:
          raise SchemaParseException("Record has no fields: " + obj.__str__())
        for field in fieldsnode:
          fieldname = field.get("name")
          if fieldname is None:
            raise SchemaParseException("No field name: " + field.__str__())
          fieldtype = field.get("type")
          if fieldtype is None:
            raise SchemaParseException("No field type: " + field.__str__())
          defaultval = field.get("default")
          fields[fieldname] = Field(fieldname, _parse(fieldtype, names), 
                                    defaultval)
        return schema
      elif type == "enum":
        symbolsnode = obj.get("symbols")
        if symbolsnode == None or not isinstance(symbolsnode, list):
          raise SchemaParseException("Enum has no symbols: " + obj.__str__())
        symbols = list()
        for symbol in symbolsnode:
          symbols.append(symbol)
        schema = _EnumSchema(name, space, symbols)
        names[name] = schema
        return schema
      elif type == "fixed":
        schema = _FixedSchema(name, space, obj.get("size"))
        names[name] = schema
        return schema
    elif type == "array":
      return _ArraySchema(_parse(obj.get("items"), names))
    elif type == "map":
      return _MapSchema(_parse(obj.get("values"), names))
    else:
      raise SchemaParseException("Type not yet supported: " + type.__str__())
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
  dict = json.loads(json_string)
  return _parse(dict, _Names())
