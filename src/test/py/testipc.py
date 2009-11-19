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

import unittest, socket, struct
import testio
import avro.ipc as ipc
import avro.genericipc as genericipc
import avro.protocol as protocol
import avro.schema as schema

PROTOCOL = protocol.parse(open("src/test/schemata/simple.avpr").read())

class TestProtocol(unittest.TestCase):

  class TestResponder(genericipc.Responder):

    def __init__(self):
      ipc.ResponderBase.__init__(self, PROTOCOL)

    def invoke(self, msg, req):
      if msg.getname() == 'hello':
        print "hello:", req.get("greeting")
        return unicode('goodbye')
      elif msg.getname() == 'echo':
        rec = req.get("record")
        print "echo:", rec
        return rec
      elif msg.getname() == 'echoBytes':
        data = req.get("data")
        print "echoBytes:", data
        return data
      elif msg.getname() == 'error':
        error = dict()
        error["message"] = unicode('an error')
        raise ipc.AvroRemoteException(error)
      else:
        raise schema.AvroException("unexpected message:",msg.getname());

  def testipc(self):
    self.server = None
    self.requestor = None
    try:
      self.checkstartserver()
      self.checkhello()
      self.checkecho()
      self.checkechobytes()
      self.checkerror()
    finally:
      self.checkshutdown()

  def checkstartserver(self):
    addr = ('localhost', 0)
    self.server = ipc.SocketServer(self.TestResponder(), addr)
    sock = socket.socket()
    sock.connect(self.server.getaddress())
    client = ipc.SocketTransceiver(sock)
    self.requestor = genericipc.Requestor(PROTOCOL, client)

  def checkhello(self):
    params = dict()
    params['greeting'] = unicode('bob')
    resp = self.requestor.request('hello', params)
    self.assertEquals('goodbye',resp)

  def checkecho(self):
    record = dict()
    record['name'] = unicode('foo')
    record['kind'] = 'BAR'
    record['hash'] = struct.pack('16s','0123456789012345')
    params = dict()
    params['record'] = record
    echoed = self.requestor.request('echo', params)
    self.assertEquals(record,echoed)

  def checkechobytes(self):
    params = dict()
    rand = testio.RandomData(schema._BytesSchema())
    data = rand.next()
    params['data'] = data
    echoed = self.requestor.request('echoBytes', params)
    self.assertEquals(data,echoed)

  def checkerror(self):
    error = None
    try:
      self.requestor.request("error", dict())
    except ipc.AvroRemoteException, e:
      error = e
    self.assertNotEquals(error, None)
    self.assertEquals("an error", error.getvalue().get("message"))

  def checkshutdown(self):
    if self.server is not None:
      try:
        self.server.close()
      except Exception, e:
        print "Exception while closing socket", e
