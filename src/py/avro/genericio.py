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

"""Generic representation for data. 
Represent Schema data with generic python types.

Uses the following mapping:
  * Schema records are implemented as dict.
  * Schema arrays are implemented as list.
  * Schema maps are implemented as dict.
  * Schema strings are implemented as unicode.
  * Schema bytes are implemented as str.
  * Schema ints are implemented as int.
  * Schema longs are implemented as long.
  * Schema floats are implemented as float.
  * Schema doubles are implemented as float.
  * Schema booleans are implemented as bool. 
"""

import avro.schema as schema
import avro.io as io

def _validatearray(schm, object):
  if not isinstance(object, list):
    return False
  for elem in object:
    if not validate(schm.getelementtype(), elem):
      return False
  return True

def _validatemap(schm, object):
  if not isinstance(object, dict):
    return False
  for k,v in object.items():
    if not validate(schm.getvaluetype(), v):
      return False
  return True

def _validaterecord(schm, object):
  if not isinstance(object, dict):
    return False
  for field in schm.getfields().values():
    if not validate(field.getschema(), object.get(field.getname())):
      return False
  return True

def _validateunion(schm, object):
  for elemtype in schm.getelementtypes():
    if validate(elemtype, object):
      return True
  return False

_validatefn = {
     schema.NULL : lambda schm, object: object is None,
     schema.BOOLEAN : lambda schm, object: isinstance(object, bool),
     schema.STRING : lambda schm, object: isinstance(object, basestring),
     schema.FLOAT : lambda schm, object: isinstance(object, float),
     schema.DOUBLE : lambda schm, object: isinstance(object, float),
     schema.BYTES : lambda schm, object: isinstance(object, str),
     schema.INT : lambda schm, object: ((isinstance(object, long) or 
                                         isinstance(object, int)) and 
                              io._INT_MIN_VALUE <= object <= io._INT_MAX_VALUE),
     schema.LONG : lambda schm, object: ((isinstance(object, long) or 
                                          isinstance(object, int)) and 
                            io._LONG_MIN_VALUE <= object <= io._LONG_MAX_VALUE),
     schema.ENUM : lambda schm, object:
                                schm.getenumsymbols().__contains__(object),
     schema.FIXED : lambda schm, object:
                                (isinstance(object, str) and 
                                 len(object) == schm.getsize()),
     schema.ARRAY : _validatearray,
     schema.MAP : _validatemap,
     schema.RECORD : _validaterecord,
     schema.UNION : _validateunion
     }

def validate(schm, object):
  """Returns True if a python datum matches a schema."""
  fn = _validatefn.get(schm.gettype())
  if fn is not None:
    return fn(schm, object)
  else:
    return False

class DatumReader(io.DatumReaderBase):
  """DatumReader for generic python objects."""

  def __init__(self, actual=None, expected=None):
    self.setschema(actual)
    self.__expected = expected
    self.__readfn = {
     schema.BOOLEAN : lambda actual, expected, decoder: decoder.readboolean(),
     schema.STRING : lambda actual, expected, decoder: decoder.readutf8(),
     schema.INT : lambda actual, expected, decoder: decoder.readint(),
     schema.LONG : lambda actual, expected, decoder: decoder.readlong(),
     schema.FLOAT : lambda actual, expected, decoder: decoder.readfloat(),
     schema.DOUBLE : lambda actual, expected, decoder: decoder.readdouble(),
     schema.BYTES : lambda actual, expected, decoder: decoder.readbytes(),
     schema.FIXED : self.readfixed,
     schema.ARRAY : self.readarray,
     schema.MAP : self.readmap,
     schema.RECORD : self.readrecord,
     schema.ENUM : self.readenum
     }
    self.__skipfn = {
     schema.BOOLEAN : lambda schm, decoder: decoder.skipboolean(),
     schema.STRING : lambda schm, decoder: decoder.skiputf8(),
     schema.INT : lambda schm, decoder: decoder.skipint(),
     schema.LONG : lambda schm, decoder: decoder.skiplong(),
     schema.FLOAT : lambda schm, decoder: decoder.skipfloat(),
     schema.DOUBLE : lambda schm, decoder: decoder.skipdouble(),
     schema.BYTES : lambda schm, decoder: decoder.skipbytes(),
     schema.FIXED : self.skipfixed,
     schema.ARRAY : self.skiparray,
     schema.MAP : self.skipmap,
     schema.RECORD : self.skiprecord,
     schema.ENUM : self.skipenum,
     schema.UNION : self.skipunion
     }

  def setschema(self, schm):
    self.__actual = schm

  def read(self, decoder):
    if self.__expected is None:
      self.__expected = self.__actual
    return self.readdata(self.__actual, self.__expected, decoder)

  def readdata(self, actual, expected, decoder):
    if actual.gettype() == schema.UNION:
      actual = actual.getelementtypes()[int(decoder.readlong())]
    if expected.gettype() == schema.UNION:
      expected = self._resolve(actual, expected)
    if actual.gettype() == schema.NULL:
      return None
    fn = self.__readfn.get(actual.gettype())
    if fn is not None:
      return fn(actual, expected, decoder)
    else:
      raise schema.AvroException("Unknown type: "+schema.stringval(actual));

  def skipdata(self, schm, decoder):
    fn = self.__skipfn.get(schm.gettype())
    if fn is not None:
      return fn(schm, decoder)
    else:
      raise schema.AvroException("Unknown type: "+schema.stringval(schm));

  def readfixed(self, actual, expected, decoder):
    self.__checkname(actual, expected)
    if actual.getsize() != expected.getsize():
      self.__raisematchException(actual, expected)
    return decoder.read(actual.getsize())

  def skipfixed(self, schm):
    return decoder.skip(actual.getsize())

  def readmap(self, actual, expected, decoder):
    if (actual.getvaluetype().gettype() != 
          expected.getvaluetype().gettype()):
      self.__raisematchException(actual, expected)
    result = dict()
    size = decoder.readlong()
    if size != 0:
      for i in range(0, size):
        key = decoder.readutf8()
        result[key] = self.readdata(actual.getvaluetype(), 
                                    expected.getvaluetype(), decoder)
      decoder.readlong()
    return result

  def skipmap(self, schm, decoder):
    size = decoder.readlong()
    if size != 0:
      for i in range(0, size):
        decoder.skiputf8()
        self.skipdata(schm.getvaluetype(), decoder)
      decoder.skiplong()

  def readarray(self, actual, expected, decoder):
    if (actual.getelementtype().gettype() != 
          expected.getelementtype().gettype()):
      self.__raisematchException(actual, expected)
    result = list()
    size = decoder.readlong()
    if size != 0:
      for i in range(0, size):
        result.append(self.readdata(actual.getelementtype(), 
                                    expected.getelementtype(), decoder))
      decoder.readlong()
    return result

  def skiparray(self, schm, decoder):
    size = decoder.readlong()
    if size != 0:
      for i in range(0, size):
        self.skipdata(schm.getelementtype(), decoder)
      decoder.skiplong()

  def createrecord(self, schm):
    return dict()

  def addfield(self, record, name, value):
     record[name] = value

  def readrecord(self, actual, expected, decoder):
    self.__checkname(actual, expected)
    expectedfields = expected.getfields()
    record = self.createrecord(actual)
    size = 0 
    for fieldname, field in actual.getfields().items():
      if expected == actual:
        expectedfield = field
      else:
        expectedfield = expectedfields.get(fieldname)
      if expectedfield is None:
        self.skipdata(field.getschema(), decoder)
        continue
      self.addfield(record, fieldname, self.readdata(field.getschema(), 
                                        expectedfield.getschema(), decoder))
      size += 1
    if len(expectedfields) > size:  # not all fields set
      actualfields = actual.getfields()
      for fieldname, field in expectedfields.items():
        if not actualfields.has_key(fieldname):
          defval = field.getdefaultvalue()
          if defval is not None:
            self.addfield(record, fieldname, 
                      self._defaultfieldvalue(field.getschema(), defval))
    return record

  def skiprecord(self, schm, decoder):
    for field in schm.getfields().values():
      self.skipdata(field.getschema(), decoder)

  def readenum(self, actual, expected, decoder):
    self.__checkname(actual, expected)
    index = decoder.readint()
    return actual.getenumsymbols()[index]

  def skipenum(self, schm, decoder):
    decoder.skipint()

  def skipunion(self, schm, decoder):
    index = int(decoder.readlong())
    return self.skipdata(schm.getelementtypes()[index], decoder)

  def _resolve(self, actual, expected):
    # scan for exact match
    for branch in expected.getelementtypes():
      if branch.gettype() == actual.gettype():
        return branch
    #scan for match via numeric promotion
    for branch in expected.getelementtypes():
      actualtype = actual.gettype()
      expectedtype = branch.gettype()
      if actualtype == schema.INT:
        if (expectedtype == schema.LONG or expectedtype == schema.FLOAT 
            or expectedtype == schema.DOUBLE):
          return branch
      elif actualtype == schema.LONG:
        if (expectedtype == schema.FLOAT or expectedtype == schema.DOUBLE):
          return branch
      elif actualtype == schema.FLOAT:
        if (expectedtype == schema.DOUBLE):
          return branch
    self.__raisematchException(actual, expected)

  def __checkname(self, actual, expected):
    if actual.getname() != expected.getname():
      self.__raisematchException(actual, expected)

  def __raisematchException(self, actual, expected):
    raise schema.AvroException("Expected "+schema.stringval(expected)+
                        ", found "+schema.stringval(actual))

  def _defaultfieldvalue(self, schm, defaultnode):
    if schm.gettype() == schema.RECORD:
      record = self.createrecord(schm)
      for field in schm.getfields().values():
        v = defaultnode.get(field.getname())
        if v is None:
          v = field.getdefaultvalue()
        if v is not None:
          record[field.getname()] = self._defaultfieldvalue(
                                                  field.getschema(), v)
      return record
    elif schm.gettype() == schema.ENUM:
      return defaultnode
    elif schm.gettype() == schema.ARRAY:
      array = list()
      for node in defaultnode:
        array.append(self._defaultfieldvalue(schm.getelementtype(), node))
      return array
    elif schm.gettype() == schema.MAP:
      map = dict()
      for k,v in defaultnode.items():
        map[k] = self._defaultfieldvalue(schm.getvaluetype(), v)
      return map
    elif schm.gettype() == schema.UNION:
      return self._defaultfieldvalue(schm.getelementtypes()[0], defaultnode)
    elif schm.gettype() == schema.FIXED:
      return defaultnode
    elif schm.gettype() == schema.STRING:
      return defaultnode
    elif schm.gettype() == schema.BYTES:
      return defaultnode
    elif schm.gettype() == schema.INT:
      return int(defaultnode)
    elif schm.gettype() == schema.LONG:
      return long(defaultnode)
    elif schm.gettype() == schema.FLOAT:
      return float(defaultnode)
    elif schm.gettype() == schema.DOUBLE:
      return float(defaultnode)
    elif schm.gettype() == schema.BOOLEAN:
      return bool(defaultnode)
    elif schm.gettype() == schema.NULL:
      return None
    else:
      raise schema.AvroException("Unknown type: "+schema.stringval(actual))

class DatumWriter(io.DatumWriterBase):
  """DatumWriter for generic python objects."""

  def __init__(self, schm=None):
    self.setschema(schm)
    self.__writefn = {
     schema.BOOLEAN : lambda schm, datum, encoder: 
                  encoder.writeboolean(datum),
     schema.STRING : lambda schm, datum, encoder: 
                  encoder.writeutf8(datum),
     schema.INT : lambda schm, datum, encoder: 
                  encoder.writeint(datum),
     schema.LONG : lambda schm, datum, encoder: 
                  encoder.writelong(datum),
     schema.FLOAT : lambda schm, datum, encoder: 
                  encoder.writefloat(datum),
     schema.DOUBLE : lambda schm, datum, encoder: 
                  encoder.writedouble(datum),
     schema.BYTES : lambda schm, datum, encoder: 
                  encoder.writebytes(datum),
     schema.FIXED : lambda schm, datum, encoder: 
                  encoder.write(datum),
     schema.ARRAY : self.writearray,
     schema.MAP : self.writemap,
     schema.RECORD : self.writerecord,
     schema.ENUM : self.writeenum,
     schema.UNION : self.writeunion
     }

  def setschema(self, schm):
    self.__schm = schm

  def write(self, datum, encoder):
    self.writedata(self.__schm, datum, encoder)

  def writedata(self, schm, datum, encoder):
    if schm.gettype() == schema.NULL:
      if datum is None:
        return
      raise io.AvroTypeException(schm, datum)
    fn = self.__writefn.get(schm.gettype())
    if fn is not None:
      fn(schm, datum, encoder)
    else:
      raise io.AvroTypeException(schm, datum)

  def writemap(self, schm, datum, encoder):
    if not isinstance(datum, dict):
      raise io.AvroTypeException(schm, datum)
    if len(datum) > 0:
      encoder.writelong(len(datum))
      for k,v in datum.items():
        encoder.writeutf8(k)
        self.writedata(schm.getvaluetype(), v, encoder)
    encoder.writelong(0)

  def writearray(self, schm, datum, encoder):
    if not isinstance(datum, list):
      raise io.AvroTypeException(schm, datum)
    if len(datum) > 0:
      encoder.writelong(len(datum))
      for item in datum:
        self.writedata(schm.getelementtype(), item, encoder)
    encoder.writelong(0)

  def writerecord(self, schm, datum, encoder):
    if not isinstance(datum, dict):
      raise io.AvroTypeException(schm, datum)
    for field in schm.getfields().values():
      self.writedata(field.getschema(), datum.get(field.getname()), encoder)

  def writeunion(self, schm, datum, encoder):
    index = self.resolveunion(schm, datum)
    encoder.writelong(index)
    self.writedata(schm.getelementtypes()[index], datum, encoder)

  def writeenum(self, schm, datum, encoder):
    index = schm.getenumordinal(datum)
    encoder.writeint(index)

  def resolveunion(self, schm, datum):
    index = 0
    for elemtype in schm.getelementtypes():
      if validate(elemtype, datum):
        return index
      index+=1
    raise io.AvroTypeException(schm, datum)