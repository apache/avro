#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
try:
  from cStringIO import StringIO
except ImportError:
  from StringIO import StringIO
from avro import ipc
from avro import io

from zope.interface import implements

from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from twisted.internet.defer import maybeDeferred, Deferred
from twisted.web.iweb import IBodyProducer
from twisted.web import resource, server
from twisted.internet.protocol import Protocol

class TwistedRequestor(ipc.BaseRequestor):
  """A Twisted-compatible requestor. Returns a Deferred that will fire with the
     returning value, instead of blocking until the request completes."""
  def _process_handshake(self, call_response, message_name, request_datum):
    # process the handshake and call response
    buffer_decoder = io.BinaryDecoder(StringIO(call_response))
    call_response_exists = self.read_handshake_response(buffer_decoder)
    if call_response_exists:
      return self.read_call_response(message_name, buffer_decoder)
    else:
      return self.request(message_name, request_datum)

  def issue_request(self, call_request, message_name, request_datum):
    d = self.transceiver.transceive(call_request)
    d.addCallback(self._process_handshake, message_name, request_datum)
    return d

class RequestStreamingProducer(object):
  """A streaming producer for issuing requests with the Twisted.web Agent."""
  implements(IBodyProducer)

  paused = False
  stopped = False
  started = False

  def __init__(self, message):
    self._message = message
    self._length = len(message)
    # We need a buffer length header for every buffer and an additional
    # zero-length buffer as the message terminator
    self._length += (self._length / ipc.BUFFER_SIZE + 2) \
      * ipc.BUFFER_HEADER_LENGTH
    self._total_bytes_sent = 0
    self._deferred = Deferred()

  # read-only properties
  message = property(lambda self: self._message)
  length = property(lambda self: self._length)
  consumer = property(lambda self: self._consumer)
  deferred = property(lambda self: self._deferred)

  def _get_total_bytes_sent(self):
    return self._total_bytes_sent

  def _set_total_bytes_sent(self, bytes_sent):
    self._total_bytes_sent = bytes_sent

  total_bytes_sent = property(_get_total_bytes_sent, _set_total_bytes_sent)

  def startProducing(self, consumer):
    if self.started:
      return

    self.started = True
    self._consumer = consumer
    # Keep writing data to the consumer until we're finished,
    # paused (pauseProducing()) or stopped (stopProducing())
    while self.length - self.total_bytes_sent > 0 and \
      not self.paused and not self.stopped:
      self.write()
    # self.write will fire this deferred once it has written
    # the entire message to the consumer
    return self.deferred

  def resumeProducing(self):
    self.paused = False
    self.write(self)

  def pauseProducing(self):
    self.paused = True

  def stopProducing(self):
    self.stopped = True

  def write(self):
    if self.length - self.total_bytes_sent > ipc.BUFFER_SIZE:
      buffer_length = ipc.BUFFER_SIZE
    else:
      buffer_length = self.length - self.total_bytes_sent
    self.write_buffer(self.message[self.total_bytes_sent:
                              (self.total_bytes_sent + buffer_length)])
    self.total_bytes_sent += buffer_length
    # Make sure we wrote the entire message
    if self.total_bytes_sent == self.length and not self.stopped:
      self.stopProducing()
      # A message is always terminated by a zero-length buffer.
      self.write_buffer_length(0)
      self.deferred.callback(None)

  def write_buffer(self, chunk):
    buffer_length = len(chunk)
    self.write_buffer_length(buffer_length)
    self.consumer.write(chunk)

  def write_buffer_length(self, n):
    self.consumer.write(ipc.BIG_ENDIAN_INT_STRUCT.pack(n))

class AvroProtocol(Protocol):

  recvd = ''
  done = False

  def __init__(self, finished):
    self.finished = finished
    self.message = []

  def dataReceived(self, data):
    self.recvd = self.recvd + data
    while len(self.recvd) >= ipc.BUFFER_HEADER_LENGTH:
      buffer_length ,= ipc.BIG_ENDIAN_INT_STRUCT.unpack(
        self.recvd[:ipc.BUFFER_HEADER_LENGTH])
      if buffer_length == 0:
        response = ''.join(self.message)
        self.done = True
        self.finished.callback(response)
        break
      if len(self.recvd) < buffer_length + ipc.BUFFER_HEADER_LENGTH:
        break
      buffer = self.recvd[ipc.BUFFER_HEADER_LENGTH:buffer_length + ipc.BUFFER_HEADER_LENGTH]
      self.recvd = self.recvd[buffer_length + ipc.BUFFER_HEADER_LENGTH:]
      self.message.append(buffer)

  def connectionLost(self, reason):
    if not self.done:
      self.finished.errback(ipc.ConnectionClosedException("Reader read 0 bytes."))

class TwistedHTTPTransceiver(object):
  """This transceiver uses the Agent class present in Twisted.web >= 9.0
     for issuing requests to the remote endpoint."""
  def __init__(self, host, port, remote_name=None, reactor=None):
    self.url = "http://%s:%d/" % (host, port)

    if remote_name is None:
      # There's no easy way to get this peer's remote address
      # in Twisted so I use a random UUID to identify ourselves
      import uuid
      self.remote_name = uuid.uuid4()

    if reactor is None:
      from twisted.internet import reactor
    self.agent = Agent(reactor)

  def read_framed_message(self, response):
    finished = Deferred()
    response.deliverBody(AvroProtocol(finished))
    return finished

  def transceive(self, request):
    req_method = 'POST'
    req_headers = {
      'Content-Type': ['avro/binary'],
      'Accept-Encoding': ['identity'],
    }

    body_producer = RequestStreamingProducer(request)
    d = self.agent.request(
      req_method,
      self.url,
      headers=Headers(req_headers),
      bodyProducer=body_producer)
    return d.addCallback(self.read_framed_message)

class AvroResponderResource(resource.Resource):
  """This Twisted.web resource can be placed anywhere in a URL hierarchy
     to provide an Avro endpoint. Different Avro protocols can be served
     by the same web server as long as they are in different resources in
     a URL hierarchy."""
  isLeaf = True

  def __init__(self, responder):
    resource.Resource.__init__(self)
    self.responder = responder

  def cb_render_POST(self, resp_body, request):
    request.setResponseCode(200)
    request.setHeader('Content-Type', 'avro/binary')
    resp_writer = ipc.FramedWriter(request)
    resp_writer.write_framed_message(resp_body)
    request.finish()

  def render_POST(self, request):
    # Unfortunately, Twisted.web doesn't support incoming
    # streamed input yet, the whole payload must be kept in-memory
    request.content.seek(0, 0)
    call_request_reader = ipc.FramedReader(request.content)
    call_request = call_request_reader.read_framed_message()
    d = maybeDeferred(self.responder.respond, call_request)
    d.addCallback(self.cb_render_POST, request)
    return server.NOT_DONE_YET
