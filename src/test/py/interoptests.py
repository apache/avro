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

import unittest, os, sys, socket
import avro.schema as schema
import avro.io as io
import avro.ipc as ipc
import avro.genericio as genericio
import avro.reflectio as reflectio
import avro.reflectipc as reflectipc
import avro.datafile as datafile
import testio, testipc, testioreflect, testipcreflect

_BLOCKINGFILE_DIR = "build/test/blocking-data-files/"
_DATAFILE_DIR = "build/test/data-files/"
_SERVER_PORTS_DIR = testio._DIR + "server-ports/"

class TestGeneratedFiles(unittest.TestCase):

  def __init__(self, methodName, validator=genericio.validate, 
               datumreader=genericio.DatumReader):
    unittest.TestCase.__init__(self, methodName)
    self.__validator = validator
    self.__datumreader = datumreader

  def testreadfiles(self):
    origschm = schema.parse(open("src/test/schemata/interop.avsc").read())
    for file in os.listdir(_DATAFILE_DIR):
      print "Validating:", file.__str__()
      dr = datafile.DataFileReader(open(_DATAFILE_DIR+file, "rb"), 
                             self.__datumreader())
      count = int(dr.getmeta("count"))
      decodedSchm = schema.parse(dr.getmeta("schema"))
      self.assertEquals(origschm, decodedSchm)
      for i in range(0,count):
        datum = dr.next()
        self.assertTrue(self.__validator(origschm, datum))
    # validate reading of blocking arrays, blocking maps
    for file in os.listdir(_BLOCKINGFILE_DIR):
      print "Validating:", file.__str__()
      reader = open(_BLOCKINGFILE_DIR+file, "rb")
      decoder = io.Decoder(reader)
      dreader = self.__datumreader()
      dreader.setschema(origschm)
      count = int(decoder.readlong()) #metadata:the count of objects in the file
      blockcount = decoder.readlong()
      for i in range(0,count):
        while blockcount == 0:
          blockcount = decoder.readlong()
        blockcount -= 1
        datum = dreader.read(decoder)
        self.assertTrue(self.__validator(origschm, datum))

class TestReflectGeneratedFiles(TestGeneratedFiles):

  def __init__(self, methodName):
    TestGeneratedFiles.__init__(self, methodName, 
                                       testioreflect.dyvalidator, 
                                       testioreflect.ReflectDReader)

def _interopclient():
  for file in os.listdir(_SERVER_PORTS_DIR):
    port = open(_SERVER_PORTS_DIR+file).read()
    print "Validating python client to", file, "-", port
    sock = socket.socket()
    sock.connect(("localhost", int(port)))
    client = ipc.SocketTransceiver(sock)
    testproto = testipcreflect.TestProtocol("testipc")
    testproto.proxy = reflectipc.getclient(testipc.PROTOCOL, client)
    testproto.checkhello()
    testproto.checkecho()
    testproto.checkechobytes()
    testproto.checkerror()
    print "Done! Validation python client to", file, "-", port

def _interopserver():
  addr = ('localhost', 0)
  responder = reflectipc.ReflectResponder(testipc.PROTOCOL, 
                                       testipcreflect.TestImpl())
  server = ipc.SocketServer(responder, addr)
  file = open(_SERVER_PORTS_DIR+"py-port", "w")
  file.write(str(server.getaddress()[1]))

if __name__ == '__main__':
  usage = "Usage: testipcrefect.py <client/server>"
  if len(sys.argv) != 2:
    print usage
    exit
  if sys.argv[1] == "client":
    _interopclient()
  elif sys.argv[1] == "server":
    _interopserver()
  else:
    print usage
