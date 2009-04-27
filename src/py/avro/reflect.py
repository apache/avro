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
import avro.ipc as ipc
import avro.generic as generic

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
  if not isinstance(object, gettype(schm.getname(), pkgname)):
    return False
  for field,fieldschema in schm.getfields():
    data = object.__getattribute__(field)
    if not validate(fieldschema, pkgname, data):
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

def gettype(name, pkgname, base=object):
  """Returns the type with classname as name and pkg name as pkgname. 
  If type does not exist creates a new type."""
  clazzname = pkgname + name
  clazz = globals().get(clazzname)
  if clazz is None:
    clazz = type(str(clazzname),(base,),{})
    globals()[clazzname] = clazz
  return clazz

class ReflectDatumReader(generic.DatumReader):
  """DatumReader for arbitrary python classes."""

  def __init__(self, pkgname, schm=None):
    generic.DatumReader.__init__(self, schm)
    self.__pkgname = pkgname

  def readrecord(self, schm, valuereader):
    type = gettype(schm.getname(), self.__pkgname)
    result = type()
    for field,fieldschema in schm.getfields():
      setattr(result, field, self.readdata(fieldschema, valuereader))
    return result

class ReflectDatumWriter(generic.DatumWriter):
  """DatumWriter for arbitrary python classes."""

  def __init__(self, pkgname, schm=None):
    generic.DatumWriter.__init__(self, schm)
    self.__pkgname = pkgname

  def writerecord(self, schm, datum, valuewriter):
    for field,fieldschema in schm.getfields():
      self.writedata(fieldschema, getattr(datum, field), valuewriter)

  def resolveunion(self, schm, datum):
    index = 0
    for elemtype in schm.getelementtypes():
      if validate(elemtype, self.__pkgname, datum):
        return index
      index+=1
    raise io.AvroTypeException(schm, datum)

class _Proxy(object):

  class _MethodInvoker(object):

    def __init__(self, requestor, methodname):
      self.requestor = requestor
      self.methodname = methodname

    def __call__(self, *args):
      return self.requestor.call(self.methodname, args)

  def __init__(self, requestor):
    self.requestor = requestor
    self.invokers = dict()
    msgs = self.requestor.getlocal().getmessages()
    for methodname, method in msgs.items():
      self.invokers[methodname] = self._MethodInvoker(
                                                self.requestor, methodname)

  def __getattribute__(self, attr):
    attrhandle = object.__getattribute__(self, "invokers").get(attr)
    if attrhandle is None:
      attrhandle = object.__getattribute__(self, attr)
    return attrhandle

def getclient(protocol, transceiver):
  """Create a proxy instance whose methods invoke RPCs."""
  requestor = ReflectRequestor(protocol, transceiver)
  return _Proxy(requestor)

class ReflectRequestor(generic.Requestor):

  def __init__(self, localproto, transceiver):
    ipc.RequestorBase.__init__(self, localproto, transceiver)
    self.__pkgname = localproto.getnamespace() + "."

  def getdatumwriter(self, schm):
    return ReflectDatumWriter(self.__pkgname, schm)

  def getdatumreader(self, schm):
    return ReflectDatumReader(self.__pkgname, schm)

  def writerequest(self, schm, req, vwriter):
    index = 0
    for arg in req:
      argschm = schm.getfields()[index][1]
      generic.Requestor.writerequest(self, argschm, arg, vwriter)

  def readerror(self, schm, vreader):
    return self.getdatumreader(schm).read(vreader)

class ReflectResponder(generic.Responder):

  def __init__(self, localproto, impl):
    generic.Responder.__init__(self, localproto)
    self.__pkgname = localproto.getnamespace() + "."
    self.__impl = impl

  def getdatumwriter(self, schm):
    return ReflectDatumWriter(self.__pkgname, schm)

  def getdatumreader(self, schm):
    return ReflectDatumReader(self.__pkgname, schm)

  def readrequest(self, schm, vreader):
    req = list()
    for field, fieldschm in schm.getfields():
      req.append(generic.Responder.readrequest(self, fieldschm, vreader))
    return req

  def writeerror(self, schm, error, vwriter):
    self.getdatumwriter(schm).write(error, vwriter)

  def invoke(self, msg, req):
    method = self.__impl.__getattribute__(msg.getname())
    if method is None:
      raise AttributeError("No method with name "+ method)
    resp = method(*req)
    return resp

