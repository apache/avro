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

"""Define Record schema and protocol classes at runtime. This can be used 
to invoke the methods on Protocol proxy directly."""

import avro.schema as schema
import avro.io as io
import avro.genericio as genericio

#TODO pkgname should not be passed, instead classes should be constructed 
#based on schema namespace

def _validatearray(schm, pkgname, object):
  if not isinstance(object, list):
    return False
  for elem in object:
    if not validate(schm.getelementtype(), pkgname, elem):
      return False
  return True

def _validatemap(schm, pkgname, object):
  if not isinstance(object, dict):
    return False
  for k,v in object.items():
    if not validate(schm.getvaluetype(), pkgname, v):
      return False
  return True

def _validaterecord(schm, pkgname, object):
  if not isinstance(object, gettype(schm, pkgname)):
    return False
  for field in schm.getfields().values():
    data = object.__getattribute__(field.getname())
    if not validate(field.getschema(), pkgname, data):
      return False
  return True

def _validateunion(schm, pkgname, object):
  for elemtype in schm.getelementtypes():
    if validate(elemtype, pkgname, object):
      return True
  return False

_validatefn = {
     schema.NULL : lambda schm, pkgname, object: object is None,
     schema.BOOLEAN : lambda schm, pkgname, object: isinstance(object, bool),
     schema.STRING : lambda schm, pkgname, object: isinstance(object, unicode),
     schema.FLOAT : lambda schm, pkgname, object: isinstance(object, float),
     schema.DOUBLE : lambda schm, pkgname, object: isinstance(object, float),
     schema.BYTES : lambda schm, pkgname, object: isinstance(object, str),
     schema.INT : lambda schm, pkgname, object: ((isinstance(object, long) or 
                                         isinstance(object, int)) and 
                              io._INT_MIN_VALUE <= object <= io._INT_MAX_VALUE),
     schema.LONG : lambda schm, pkgname, object: ((isinstance(object, long) or 
                                          isinstance(object, int)) and 
                            io._LONG_MIN_VALUE <= object <= io._LONG_MAX_VALUE),
     schema.ENUM : lambda schm, pkgname, object:
                                schm.getenumsymbols().__contains__(object),
     schema.FIXED : lambda schm, pkgname, object:
                                (isinstance(object, str) and 
                                 len(object) == schm.getsize()),
     schema.ARRAY : _validatearray,
     schema.MAP : _validatemap,
     schema.RECORD : _validaterecord,
     schema.UNION : _validateunion
     }

def validate(schm, pkgname, object):
  """Returns True if a python datum matches a schema."""
  fn = _validatefn.get(schm.gettype())
  if fn is not None:
    return fn(schm, pkgname, object)
  else:
    return False

def gettype(recordschm, pkgname, base=object):
  """Returns the type with classname as recordschm name and pkg name as pkgname. 
  If type does not exist creates a new type."""
  clazzname = pkgname + recordschm.getname()
  clazz = globals().get(clazzname)
  if clazz is None:
    clazz = type(str(clazzname),(base,),{})
    for field in recordschm.getfields().values():
      setattr(clazz, field.getname(), None)
    globals()[clazzname] = clazz
  return clazz

class ReflectDatumReader(genericio.DatumReader):
  """DatumReader for arbitrary python classes."""

  def __init__(self, pkgname, schm=None):
    genericio.DatumReader.__init__(self, schm)
    self.__pkgname = pkgname

  def addfield(self, record, name, value):
    setattr(record, name, value)

  def createrecord(self, schm):
    type = gettype(schm, self.__pkgname)
    return type()

class ReflectDatumWriter(genericio.DatumWriter):
  """DatumWriter for arbitrary python classes."""

  def __init__(self, pkgname, schm=None):
    genericio.DatumWriter.__init__(self, schm)
    self.__pkgname = pkgname

  def writerecord(self, schm, datum, encoder):
    for field in schm.getfields().values():
      self.writedata(field.getschema(), getattr(datum, field.getname()),
                      encoder)

  def resolveunion(self, schm, datum):
    index = 0
    for elemtype in schm.getelementtypes():
      if validate(elemtype, self.__pkgname, datum):
        return index
      index+=1
    raise io.AvroTypeException(schm, datum)
