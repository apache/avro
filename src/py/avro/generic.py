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
import avro.ipc as ipc

def validate(schm, object):
  """Returns True if a python datum matches a schema."""

  if schm.gettype() == schema.NULL:
    return object is None
  elif schm.gettype() == schema.STRING:
    return isinstance(object, unicode)
  elif schm.gettype() == schema.INT:
    if ((isinstance(object, long) or isinstance(object, int))
         and io._INT_MIN_VALUE <= object <= io._INT_MAX_VALUE):
      return True
  elif schm.gettype() == schema.LONG:
    if ((isinstance(object, long) or isinstance(object, int))
         and io._LONG_MIN_VALUE <= object <= io._LONG_MAX_VALUE):
      return True
  elif schm.gettype() == schema.FLOAT:
    return isinstance(object, float)
  elif schm.gettype() == schema.DOUBLE:
    return isinstance(object, float)
  elif schm.gettype() == schema.BYTES:
    return isinstance(object, str)
  elif schm.gettype() == schema.BOOLEAN:
    return isinstance(object, bool)
  elif schm.gettype() == schema.ARRAY:
    if not isinstance(object, list):
      return False
    for elem in object:
      if not validate(schm.getelementtype(), elem):
        return False
    return True
  elif schm.gettype() == schema.MAP:
    if not isinstance(object, dict):
      return False
    for k,v in object.items():
      if not (validate(schm.getkeytype(), k) and 
              validate(schm.getvaluetype(), v)):
        return False
    return True
  elif schm.gettype() == schema.RECORD:
    if not isinstance(object, dict):
      return False
    for field,fieldschema in schm.getfields():
      if not validate(fieldschema, object.get(field)):
        return False
    return True
  elif schm.gettype() == schema.UNION:
    for elemtype in schm.getelementtypes():
      if validate(elemtype, object):
        return True
    return False
  else:
    return False

class DatumReader(io.DatumReaderBase):
  """DatumReader for generic python objects."""

  def __init__(self, schm=None):
    self.setschema(schm)
    self.__readfn = {
     schema.BOOLEAN : lambda schm, valuereader: valuereader.readboolean(),
     schema.STRING : lambda schm, valuereader: valuereader.readutf8(),
     schema.INT : lambda schm, valuereader: valuereader.readint(),
     schema.LONG : lambda schm, valuereader: valuereader.readlong(),
     schema.FLOAT : lambda schm, valuereader: valuereader.readfloat(),
     schema.DOUBLE : lambda schm, valuereader: valuereader.readdouble(),
     schema.BYTES : lambda schm, valuereader: valuereader.readbytes(),
     schema.ARRAY : self.readarray,
     schema.MAP : self.readmap,
     schema.RECORD : self.readrecord,
     schema.UNION : self.readunion
     }

  def setschema(self, schm):
    self.__schm = schm

  def read(self, valuereader):
    return self.readdata(self.__schm, valuereader)
    
  def readdata(self, schm, valuereader):
    if schm.gettype() == schema.NULL:
      return None
    fn = self.__readfn.get(schm.gettype())
    if fn is not None:
      return fn(schm, valuereader)
    else:
      raise AvroException("Unknown type: "+schema.stringval(schm));

  def readmap(self, schm, valuereader):
    result = dict()
    size = valuereader.readlong()
    if size != 0:
      for i in range(0, size):
        key = self.readdata(schm.getkeytype(), valuereader)
        result[key] = self.readdata(schm.getvaluetype(), valuereader)
      valuereader.readlong()
    return result

  def readarray(self, schm, valuereader):
    result = list()
    size = valuereader.readlong()
    if size != 0:
      for i in range(0, size):
        result.append(self.readdata(schm.getelementtype(), valuereader))
      valuereader.readlong()
    return result

  def readrecord(self, schm, valuereader):
    result = dict() 
    for field,fieldschema in schm.getfields():
      result[field] = self.readdata(fieldschema, valuereader)
    return result

  def readunion(self, schm, valuereader):
    index = int(valuereader.readlong())
    return self.readdata(schm.getelementtypes()[index], valuereader)

class DatumWriter(io.DatumWriterBase):
  """DatumWriter for generic python objects."""

  def __init__(self, schm=None):
    self.setschema(schm)
    self.__writefn = {
     schema.BOOLEAN : lambda schm, datum, valuereader: 
                  valuereader.writeboolean(datum),
     schema.STRING : lambda schm, datum, valuereader: 
                  valuereader.writeutf8(datum),
     schema.INT : lambda schm, datum, valuereader: 
                  valuereader.writeint(datum),
     schema.LONG : lambda schm, datum, valuereader: 
                  valuereader.writelong(datum),
     schema.FLOAT : lambda schm, datum, valuereader: 
                  valuereader.writefloat(datum),
     schema.DOUBLE : lambda schm, datum, valuereader: 
                  valuereader.writedouble(datum),
     schema.BYTES : lambda schm, datum, valuereader: 
                  valuereader.writebytes(datum),
     schema.ARRAY : self.writearray,
     schema.MAP : self.writemap,
     schema.RECORD : self.writerecord,
     schema.UNION : self.writeunion
     }

  def setschema(self, schm):
    self.__schm = schm

  def write(self, datum, valuewriter):
    self.writedata(self.__schm, datum, valuewriter)

  def writedata(self, schm, datum, valuewriter):
    if schm.gettype() == schema.NULL:
      if datum is None:
        return
      raise io.AvroTypeException(schm, datum)
    fn = self.__writefn.get(schm.gettype())
    if fn is not None:
      fn(schm, datum, valuewriter)
    else:
      raise io.AvroTypeException(schm, datum)

  def writemap(self, schm, datum, valuewriter):
    if not isinstance(datum, dict):
      raise io.AvroTypeException(schm, datum)
    if len(datum) > 0:
      valuewriter.writelong(len(datum))
      for k,v in datum.items():
        self.writedata(schm.getkeytype(), k, valuewriter)
        self.writedata(schm.getvaluetype(), v, valuewriter)
    valuewriter.writelong(0)

  def writearray(self, schm, datum, valuewriter):
    if not isinstance(datum, list):
      raise io.AvroTypeException(schm, datum)
    if len(datum) > 0:
      valuewriter.writelong(len(datum))
      for item in datum:
        self.writedata(schm.getelementtype(), item, valuewriter)
    valuewriter.writelong(0)

  def writerecord(self, schm, datum, valuewriter):
    if not isinstance(datum, dict):
      raise io.AvroTypeException(schm, datum)
    for field,fieldschema in schm.getfields():
      self.writedata(fieldschema, datum.get(field), valuewriter)

  def writeunion(self, schm, datum, valuewriter):
    index = self.resolveunion(schm, datum)
    valuewriter.writelong(index)
    self.writedata(schm.getelementtypes()[index], datum, valuewriter)

  def resolveunion(self, schm, datum):
    index = 0
    for elemtype in schm.getelementtypes():
      if validate(elemtype, datum):
        return index
      index+=1
    raise io.AvroTypeException(schm, datum)

class Requestor(ipc.RequestorBase):
  """Requestor implementation for generic python data."""

  def getdatumwriter(self, schm):
    return DatumWriter(schm)

  def getdatumreader(self, schm):
    return DatumReader(schm)

  def writerequest(self, schm, req, vwriter):
    self.getdatumwriter(schm).write(req, vwriter)

  def readresponse(self, schm, vreader):
    return self.getdatumreader(schm).read(vreader)

  def readerror(self, schm, vreader):
    return ipc.AvroRemoteException(self.getdatumreader(schm).read(vreader))

class Responder(ipc.ResponderBase):
  """Responder implementation for generic python data."""

  def getdatumwriter(self, schm):
    return DatumWriter(schm)

  def getdatumreader(self, schm):
    return DatumReader(schm)

  def readrequest(self, schm, vreader):
    return self.getdatumreader(schm).read(vreader)

  def writeresponse(self, schm, response, vwriter):
    self.getdatumwriter(schm).write(response, vwriter)

  def writeerror(self, schm, error, vwriter):
    self.getdatumwriter(schm).write(error.getvalue(), vwriter)
