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

""" Uses reflectio to write and read data objects. Provides support for remote 
method invocation via protocol's proxy instance."""

import avro.schema as schema
import avro.ipc as ipc
import avro.genericipc as genericipc
import avro.reflectio as reflectio

#TODO pkgname should not be passed, instead classes should be constructed 
#based on schema namespace

class _Proxy(object):

  class _MethodInvoker(object):

    def __init__(self, requestor, methodname):
      self.requestor = requestor
      self.methodname = methodname

    def __call__(self, *args):
      return self.requestor.request(self.methodname, args)

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

class ReflectRequestor(genericipc.Requestor):

  def __init__(self, localproto, transceiver):
    ipc.RequestorBase.__init__(self, localproto, transceiver)
    self.__pkgname = localproto.getnamespace() + "."

  def getdatumwriter(self, schm):
    return reflectio.ReflectDatumWriter(self.__pkgname, schm)

  def getdatumreader(self, schm):
    return reflectio.ReflectDatumReader(self.__pkgname, schm)

  def writerequest(self, schm, req, encoder):
    for arg in req:
      argschm = schm.getfields().values()[0].getschema()
      genericipc.Requestor.writerequest(self, argschm, arg, encoder)

  def readerror(self, schm, decoder):
    return self.getdatumreader(schm).read(decoder)

class ReflectResponder(genericipc.Responder):

  def __init__(self, localproto, impl):
    genericipc.Responder.__init__(self, localproto)
    self.__pkgname = localproto.getnamespace() + "."
    self.__impl = impl

  def getdatumwriter(self, schm):
    return reflectio.ReflectDatumWriter(self.__pkgname, schm)

  def getdatumreader(self, schm):
    return reflectio.ReflectDatumReader(self.__pkgname, schm)

  def readrequest(self, schm, decoder):
    req = list()
    for field in schm.getfields().values():
      req.append(genericipc.Responder.readrequest(self, field.getschema(), 
                                               decoder))
    return req

  def writeerror(self, schm, error, encoder):
    self.getdatumwriter(schm).write(error, encoder)

  def invoke(self, msg, req):
    method = self.__impl.__getattribute__(msg.getname())
    if method is None:
      raise AttributeError("No method with name "+ method)
    resp = method(*req)
    return resp

