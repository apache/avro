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

"""Support for inter-process calls."""

import socket, struct, errno, struct, cStringIO, threading
import avro.schema as schema
import avro.protocol as protocol
import avro.io as io

class TransceiverBase(object):
  """Base class for transmitters and receivers of raw binary messages."""

  def transceive(self, request):
    self.writebuffers(request)
    return self.readbuffers()

  def writebuffers(self, buf):
    pass

  def readbuffers(self):
    pass

  def close(self):
    pass

class AvroRemoteException(Exception):
  def __init__(self, ob=None):
    self.__value = ob
    msg = None
    if self.__value is not None:
      msg = self.__value.__str__()
    Exception.__init__(self, msg)

  def getvalue(self):
    return self.__value

class ConnectionClosedException(Exception):
  pass

class RequestorBase(object):
  """Base class for the client side of a protocol interaction."""

  def __init__(self, localproto, transceiver):
    self.__localproto = localproto
    self.__transceiver = transceiver
    self.__remoteproto = self.handshake()

  def getlocal(self):
    return self.__localproto

  def getremote(self):
    return self.__remoteproto

  def gettransceiver(self):
    return self.__transceiver

  def handshake(self):
    buf = cStringIO.StringIO()
    vwriter = io.ValueWriter(buf)
    vwriter.writelong(protocol.VERSION)
    vwriter.writeutf8(unicode(self.__localproto.__str__(), 'utf8'))
    response = self.__transceiver.transceive(buf.getvalue())
    vreader = io.ValueReader(cStringIO.StringIO(response))
    remote = vreader.readutf8()
    return protocol.parse(remote)

  def call(self, msgname, req):
    """Writes a request message and reads a response or error message."""
    m = self.__localproto.getmessages().get(msgname)
    if m is None:
      raise schema.AvroException("Not a local message: "+msgname.__str__())
    remotemsg = self.__remoteproto.getmessages().get(msgname)
    if remotemsg is None:
      raise schema.AvroException("Not a remote message: "+msgname.__str__())
    writer = cStringIO.StringIO()
    vwriter = io.ValueWriter(writer)
    vwriter.writeutf8(m.getname())
    
    self.writerequest(m.getrequest(), req, vwriter)
    response = self.__transceiver.transceive(writer.getvalue())
    vreader = io.ValueReader(cStringIO.StringIO(response))
    self.__remoteproto.getmessages().get(msgname)
    if not vreader.readboolean():
      return self.readresponse(remotemsg.getresponse(), vreader)
    else:
      raise self.readerror(remotemsg.geterrors(), vreader)

  def writerequest(self, schm, req, vwriter):
    """Writes a request message."""
    pass

  def readresponse(self, schm, vreader):
    """Reads a response message."""
    pass

  def readerror(self, schm, vreader):
    """Reads an error message."""
    pass

class ResponderBase(object):
  """Base class for the server side of a protocol interaction."""

  def __init__(self, localproto):
    self.__localproto = localproto

  def getlocal(self):
    return self.__localproto

  def handshake(self, server):
    """Returns the remote protocol."""
    vreader = io.ValueReader(cStringIO.StringIO(server.readbuffers()))
    out = cStringIO.StringIO()
    vwriter = io.ValueWriter(out)
    version = vreader.readlong()
    if version != protocol.VERSION:
      raise schema.AvroException("Incompatible request version: "
                                  +version.__str__())
    proto = vreader.readutf8()
    remote = protocol.parse(proto)
    vwriter.writeutf8(unicode(self.__localproto.__str__(), 'utf8'))
    server.writebuffers(out.getvalue())
    
    return remote

  def call(self, remoteproto, input):
    buf = cStringIO.StringIO()
    vwriter = io.ValueWriter(buf)
    try:
      reader = cStringIO.StringIO(input)
      vreader = io.ValueReader(reader)
      msgname = vreader.readutf8()
      m = remoteproto.getmessages().get(msgname)
      if m is None:
        raise schema.AvroException("No such remote message: "+msgname.__str__())
      
      req = self.readrequest(m.getrequest(), vreader)
      m = self.__localproto.getmessages().get(msgname)
      if m is None:
        raise schema.AvroException("No such local message: "+msgname.__str__())
      error = None
      try:
        response = self.invoke(m, req)
      except AvroRemoteException, e:
        error = e
      vwriter.writeboolean(error is not None)
      if error is None:
        self.writeresponse(m.getresponse(), response, vwriter)
      else:
        self.writeerror(m.geterrors(), error, vwriter)
    except schema.AvroException, e:
      error = AvroRemoteException(unicode(e.__str__()))
      buf = cStringIO.StringIO()
      vwriter = io.ValueWriter(buf)
      vwriter.writeboolean(True)
      self.writeerror(protocol._SYSTEM_ERRORS, error, vwriter)
    
    return buf.getvalue()

  def invoke(self, msg, req):
    pass

  def readrequest(self, schm, vreader):
    """Reads a request message."""
    pass

  def writeresponse(self, schm, response, vwriter):
    """Writes a response message."""
    pass

  def writeerror(self, schm, error, vwriter):
    """Writes an error message."""
    pass

_STRUCT_INT = struct.Struct('!I')
_BUFFER_SIZE = 8192
class SocketTransceiver(TransceiverBase):
  """A simple socket-based Transceiver implementation."""

  def __init__(self, sock):
    self.__sock = sock

  def readbuffers(self):
    msg = cStringIO.StringIO()
    while True:
      buffer = cStringIO.StringIO()
      size = self.__readlength()
      if size == 0:
        return msg.getvalue()
      while buffer.tell() < size:
        chunk = self.__sock.recv(size-buffer.tell())
        if chunk == '':
          raise ConnectionClosedException("socket read 0 bytes")
        buffer.write(chunk)
      msg.write(buffer.getvalue())

  def writebuffers(self, msg):
    totalsize = len(msg)
    totalsent = 0
    while totalsize-totalsent > 0:
      if totalsize-totalsent > _BUFFER_SIZE:
        batchsize = _BUFFER_SIZE
      else:
        batchsize = totalsize-totalsent
      self.__writebuffer(msg[totalsent:(totalsent+batchsize)])
      totalsent = totalsent + batchsize
    self.__writelength(0) #null terminate

  def __writebuffer(self, msg):
    size = len(msg)
    self.__writelength(size)
    totalsent = 0
    while totalsent < size:
      sent = self.__sock.send(msg[totalsent:])
      if sent == 0:
        raise ConnectionClosedException("socket sent 0 bytes")
      totalsent = totalsent + sent

  def __writelength(self, len):
    sent = self.__sock.sendall(_STRUCT_INT.pack(len))
    if sent == 0:
      raise ConnectionClosedException("socket sent 0 bytes")

  def __readlength(self):
    read = self.__sock.recv(4)
    if read == '':
      raise ConnectionClosedException("socket read 0 bytes")
    return _STRUCT_INT.unpack(read)[0]

  def close(self):
    self.__sock.shutdown(socket.SHUT_RDWR)
    self.__sock.close()

class SocketServer(threading.Thread):
  """A simple socket-based server implementation."""

  def __init__(self, responder, addr):
    self.__responder = responder
    self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.__sock.bind(addr)
    self.__sock.listen(5)
    self.__address = self.__sock.getsockname()
    print "Socket server starting on ",self.__address
    threading.Thread.__init__(self, name="SocketServer on " + addr.__str__())
    #self.setDaemon(True)
    self.start()

  def getaddress(self):
    return self.__address

  def run(self):
    try:
      while True:
        conn, add = self.__sock.accept()
        print 'Connected by', add
        self.Connection(conn, add, self.__responder)
    except socket.error, e:
      eno, msg = e.args
      if eno == errno.EINVAL:
        print "Socket server closed on ", self.__address
        return
      else:
        raise e

  def close(self):
    self.__sock.shutdown(socket.SHUT_RDWR)
    self.__sock.close()

  class Connection(SocketTransceiver):

    def __init__(self, sock, add, responder):
      self.__responder = responder
      SocketTransceiver.__init__(self, sock)
      self.__thread = threading.Thread(name="Connection-"+add.__str__(), 
                             target=self.__run)
      self.__thread.setDaemon(True)
      self.__thread.start()

    def __run(self):
      try:
        remoteproto = self.__responder.handshake(self)
        while True:
          buf = self.readbuffers()
          buf = self.__responder.call(remoteproto, buf)
          self.writebuffers(buf)
      except ConnectionClosedException, e:
        print "Closed:", self.__thread.getName()
        return
      finally:
        self.close()
