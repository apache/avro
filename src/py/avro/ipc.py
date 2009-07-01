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

import socket, struct, errno, struct, cStringIO, threading, weakref, os
import avro.schema as schema
import avro.protocol as protocol
import avro.io as io
import avro.genericio as genericio
import avro.reflectio as reflectio

class TransceiverBase(object):
  """Base class for transmitters and receivers of raw binary messages."""

  def getremotename(self):
    pass

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

_PKGNAME = "org.apache.avro.ipc."
_HANDSHAKE_FILE_DIR = os.path.dirname(__file__).__str__() + os.path.sep
_HANDSHAKE_REQUEST_SCHEMA = schema.parse(
                                  open(_HANDSHAKE_FILE_DIR +
                                       "HandshakeRequest.avsc").read())
_HANDSHAKE_RESPONSE_SCHEMA = schema.parse(
                                  open(_HANDSHAKE_FILE_DIR +
                                       "HandshakeResponse.avsc").read())
_HANDSHAKE_REQUESTOR_WRITER = reflectio.ReflectDatumWriter(_PKGNAME, 
                                                    _HANDSHAKE_REQUEST_SCHEMA)
_HANDSHAKE_REQUESTOR_READER = reflectio.ReflectDatumReader(_PKGNAME, 
                                                    _HANDSHAKE_RESPONSE_SCHEMA)
_HANDSHAKE_RESPONDER_WRITER = reflectio.ReflectDatumWriter(_PKGNAME, 
                                                    _HANDSHAKE_RESPONSE_SCHEMA)
_HANDSHAKE_RESPONDER_READER = reflectio.ReflectDatumReader(_PKGNAME, 
                                                    _HANDSHAKE_REQUEST_SCHEMA)
_HandshakeRequest = reflectio.gettype(_HANDSHAKE_REQUEST_SCHEMA, _PKGNAME)
_HandshakeResponse = reflectio.gettype(_HANDSHAKE_RESPONSE_SCHEMA, _PKGNAME)
_HANDSHAKE_MATCH_BOTH = "BOTH"
_HANDSHAKE_MATCH_CLIENT = "CLIENT"
_HANDSHAKE_MATCH_NONE = "NONE"
_REMOTE_HASHES = dict()
_REMOTE_PROTOCOLS = dict()

_META_SCHEMA = schema.parse("{\"type\": \"map\", \"values\": \"bytes\"}")
_META_WRITER = genericio.DatumWriter(_META_SCHEMA)
_META_READER = genericio.DatumReader(_META_SCHEMA)

class RequestorBase(object):
  """Base class for the client side of a protocol interaction."""

  def __init__(self, localproto, transceiver):
    self.__localproto = localproto
    self.__transceiver = transceiver
    self.__established = False
    self.__sendlocaltext = False
    self.__remoteproto = None

  def getlocal(self):
    return self.__localproto

  def getremote(self):
    return self.__remoteproto

  def gettransceiver(self):
    return self.__transceiver

  def request(self, msgname, req):
    """Writes a request message and reads a response or error message."""
    processed = False
    while not self.__established or not processed:
      processed = True
      buf = cStringIO.StringIO()
      encoder = io.Encoder(buf)
      if not self.__established:
        self.__writehandshake(encoder)
      requestmeta = dict()
      _META_WRITER.write(requestmeta, encoder)
      m = self.__localproto.getmessages().get(msgname)
      if m is None:
        raise schema.AvroException("Not a local message: "+msgname.__str__())
      encoder.writeutf8(m.getname())
      self.writerequest(m.getrequest(), req, encoder)
      response = self.__transceiver.transceive(buf.getvalue())
      decoder = io.Decoder(cStringIO.StringIO(response))
      if not self.__established:
        self.__readhandshake(decoder)
    responsemeta = _META_READER.read(decoder)
    m = self.getremote().getmessages().get(msgname)
    if m is None:
      raise schema.AvroException("Not a remote message: "+msgname.__str__())
    if not decoder.readboolean():
      return self.readresponse(m.getresponse(), decoder)
    else:
      raise self.readerror(m.geterrors(), decoder)

  def __writehandshake(self, encoder):
    localhash = self.__localproto.getMD5()
    remotename = self.__transceiver.getremotename()
    remotehash = _REMOTE_HASHES.get(remotename)
    self.__remoteproto = _REMOTE_PROTOCOLS.get(remotehash)
    if remotehash is None:
      remotehash = localhash
      self.__remoteproto = self.__localproto
    handshake = _HandshakeRequest()
    handshake.clientHash = localhash
    handshake.serverHash = remotehash
    if self.__sendlocaltext:
      handshake.clientProtocol = unicode(self.__localproto.__str__(), 'utf8')
    _HANDSHAKE_REQUESTOR_WRITER.write(handshake, encoder)

  def __readhandshake(self, decoder):
    handshake = _HANDSHAKE_REQUESTOR_READER.read(decoder)
    print ("Handshake.match of protocol:" + 
           self.__localproto.getname().__str__()+" with:"+ 
           self.__transceiver.getremotename().__str__() + " is " +
           handshake.match.__str__())
    if handshake.match == _HANDSHAKE_MATCH_BOTH:
      self.__established = True
    elif handshake.match == _HANDSHAKE_MATCH_CLIENT:
      self.__setremote(handshake)
      self.__established = True
    elif handshake.match == _HANDSHAKE_MATCH_NONE:
      self.__setremote(handshake)
      self.__sendlocaltext = True
    else:
      raise schema.AvroException("Unexpected match: "+handshake.match.__str__())

  def __setremote(self, handshake):
    self.__remoteproto = protocol.parse(handshake.serverProtocol.__str__())
    remotehash = handshake.serverHash
    _REMOTE_HASHES[self.__transceiver.getremotename()] = remotehash
    if not _REMOTE_PROTOCOLS.has_key(remotehash):
      _REMOTE_PROTOCOLS[remotehash] = self.__remoteproto

  def writerequest(self, schm, req, encoder):
    """Writes a request message."""
    pass

  def readresponse(self, schm, decoder):
    """Reads a response message."""
    pass

  def readerror(self, schm, decoder):
    """Reads an error message."""
    pass

class ResponderBase(object):
  """Base class for the server side of a protocol interaction."""

  def __init__(self, localproto):
    self.__localproto = localproto
    self.__remotes = weakref.WeakKeyDictionary()
    self.__protocols = dict()
    self.__localhash = self.__localproto.getMD5()
    self.__protocols[self.__localhash] = self.__localproto

  def getlocal(self):
    return self.__localproto

  def respond(self, transceiver):
    """Called by a server to deserialize a request, compute and serialize
   * a response or error."""
    transreq = transceiver.readbuffers()
    reader = cStringIO.StringIO(transreq)
    decoder = io.Decoder(reader)
    buf = cStringIO.StringIO()
    encoder = io.Encoder(buf)
    error = None
    responsemeta = dict()
    
    try:
      remoteproto = self.__handshake(transceiver, decoder, encoder)
      if remoteproto is None:  #handshake failed
        return buf.getvalue()
      
      #read request using remote protocol specification
      requestmeta = _META_READER.read(decoder)
      msgname = decoder.readutf8()
      m = remoteproto.getmessages().get(msgname)
      if m is None:
        raise schema.AvroException("No such remote message: "+msgname.__str__())
      req = self.readrequest(m.getrequest(), decoder)
      
      #read response using local protocol specification
      m = self.__localproto.getmessages().get(msgname)
      if m is None:
        raise schema.AvroException("No such local message: "+msgname.__str__())
      try:
        response = self.invoke(m, req)
      except AvroRemoteException, e:
        error = e
      except Exception, e:
        error = AvroRemoteException(unicode(e.__str__()))
      _META_WRITER.write(responsemeta, encoder)
      encoder.writeboolean(error is not None)
      if error is None:
        self.writeresponse(m.getresponse(), response, encoder)
      else:
        self.writeerror(m.geterrors(), error, encoder)
    except schema.AvroException, e:
      error = AvroRemoteException(unicode(e.__str__()))
      buf = cStringIO.StringIO()
      encoder = io.Encoder(buf)
      _META_WRITER.write(responsemeta, encoder)
      encoder.writeboolean(True)
      self.writeerror(protocol._SYSTEM_ERRORS, error, encoder)
      
    return buf.getvalue()


  def __handshake(self, transceiver, decoder, encoder):
    remoteproto = self.__remotes.get(transceiver)
    if remoteproto != None:
      return remoteproto #already established
    request = _HANDSHAKE_RESPONDER_READER.read(decoder)
    remoteproto = self.__protocols.get(request.clientHash)
    if remoteproto is None and request.clientProtocol is not None:
      remoteproto = protocol.parse(request.clientProtocol)
      self.__protocols[request.clientHash] = remoteproto
    if remoteproto is not None:
      self.__remotes[transceiver] = remoteproto
    response = _HandshakeResponse()
    
    if self.__localhash == request.serverHash:
      if remoteproto is None:
        response.match = _HANDSHAKE_MATCH_NONE
      else:
        response.match = _HANDSHAKE_MATCH_BOTH
    else:
      if remoteproto is None:
        response.match = _HANDSHAKE_MATCH_NONE
      else:
        response.match = _HANDSHAKE_MATCH_CLIENT
    if response.match != _HANDSHAKE_MATCH_BOTH:
      response.serverProtocol = unicode(self.__localproto.__str__(), "utf8")
      response.serverHash = self.__localhash
    _HANDSHAKE_RESPONDER_WRITER.write(response, encoder)
    return remoteproto

  def invoke(self, msg, req):
    pass

  def readrequest(self, schm, decoder):
    """Reads a request message."""
    pass

  def writeresponse(self, schm, response, encoder):
    """Writes a response message."""
    pass

  def writeerror(self, schm, error, encoder):
    """Writes an error message."""
    pass

_STRUCT_INT = struct.Struct('!I')
_BUFFER_SIZE = 8192
class SocketTransceiver(TransceiverBase):
  """A simple socket-based Transceiver implementation."""

  def __init__(self, sock):
    self.__sock = sock

  def getremotename(self):
    return self.__sock.getsockname()

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
        try:
          while True:
            self.writebuffers(self.__responder.respond(self))
        except ConnectionClosedException, e:
          print "Closed:", self.__thread.getName()
          return
        finally:
          self.close()
      except Exception, ex:
        print "Unexpected error", ex
