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
  for field,fieldschema in schm.getfields():
    if not validate(fieldschema, object.get(field)):
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
     schema.STRING : lambda schm, object: isinstance(object, unicode),
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

  def __init__(self, schm=None):
    self.setschema(schm)
    self.__readfn = {
     schema.BOOLEAN : lambda schm, decoder: decoder.readboolean(),
     schema.STRING : lambda schm, decoder: decoder.readutf8(),
     schema.INT : lambda schm, decoder: decoder.readint(),
     schema.LONG : lambda schm, decoder: decoder.readlong(),
     schema.FLOAT : lambda schm, decoder: decoder.readfloat(),
     schema.DOUBLE : lambda schm, decoder: decoder.readdouble(),
     schema.BYTES : lambda schm, decoder: decoder.readbytes(),
     schema.FIXED : lambda schm, decoder: 
                            (decoder.read(schm.getsize())),
     schema.ARRAY : self.readarray,
     schema.MAP : self.readmap,
     schema.RECORD : self.readrecord,
     schema.ENUM : self.readenum,
     schema.UNION : self.readunion
     }

  def setschema(self, schm):
    self.__schm = schm

  def read(self, decoder):
    return self.readdata(self.__schm, decoder)
    
  def readdata(self, schm, decoder):
    if schm.gettype() == schema.NULL:
      return None
    fn = self.__readfn.get(schm.gettype())
    if fn is not None:
      return fn(schm, decoder)
    else:
      raise AvroException("Unknown type: "+schema.stringval(schm));

  def readmap(self, schm, decoder):
    result = dict()
    size = decoder.readlong()
    if size != 0:
      for i in range(0, size):
        key = decoder.readutf8()
        result[key] = self.readdata(schm.getvaluetype(), decoder)
      decoder.readlong()
    return result

  def readarray(self, schm, decoder):
    result = list()
    size = decoder.readlong()
    if size != 0:
      for i in range(0, size):
        result.append(self.readdata(schm.getelementtype(), decoder))
      decoder.readlong()
    return result

  def readrecord(self, schm, decoder):
    result = dict() 
    for field,fieldschema in schm.getfields():
      result[field] = self.readdata(fieldschema, decoder)
    return result

  def readenum(self, schm, decoder):
    index = decoder.readint()
    return schm.getenumsymbols()[index]

  def readunion(self, schm, decoder):
    index = int(decoder.readlong())
    return self.readdata(schm.getelementtypes()[index], decoder)

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
    for field,fieldschema in schm.getfields():
      self.writedata(fieldschema, datum.get(field), encoder)

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