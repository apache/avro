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

import socket
import avro.schema as schema
import avro.reflect as reflect
import avro.ipc as ipc
import testipc, testio, testioreflect

TestRecord = reflect.gettype("TestRecord", testioreflect._PKGNAME)
TestError = reflect.gettype("TestError", testioreflect._PKGNAME, 
                            ipc.AvroRemoteException)

class TestImpl(object):

  def hello(self, req):
    return unicode('goodbye')

  def echo(self, req):
    return req

  def echoBytes(self, req):
    return req

  def error(self):
    error = TestError()
    error.message = unicode('an error')
    raise error

class TestProtocol(testipc.TestProtocol):

  def checkstartserver(self):
    addr = ('localhost', 0)
    responder = reflect.ReflectResponder(testipc.PROTOCOL, TestImpl())
    self.server = ipc.SocketServer(responder, addr)
    sock = socket.socket()
    sock.connect(self.server.getaddress())
    client = ipc.SocketTransceiver(sock)
    self.proxy = reflect.getclient(testipc.PROTOCOL, client)

  def checkhello(self):
    resp = self.proxy.hello(unicode("bob"))
    self.assertEquals('goodbye', resp)

  def checkecho(self):
    record = TestRecord()
    record.name = unicode('foo')
    echoed = self.proxy.echo(record)
    self.assertEquals(record.name, echoed.name)

  def checkechobytes(self):
    rand = testio.RandomData(schema._BytesSchema())
    data = rand.next()
    echoed = self.proxy.echoBytes(data)
    self.assertEquals(data, echoed)

  def checkerror(self):
    error = None
    try:
      self.proxy.error()
    except TestError, e:
      error = e
    self.assertNotEquals(error, None)
    self.assertEquals("an error", error.message)

  def checkshutdown(self):
    self.server.close()