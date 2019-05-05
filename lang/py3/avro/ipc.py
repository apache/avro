#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""RPC/IPC support."""

import abc
import http.client
import http.server
import io
import logging
import os
import socketserver
from typing import IO, Dict, Optional, Type

from avro import io as avro_io
from avro import protocol, schema

logger = logging.getLogger(__name__)

RequestDatumType = Dict[str, str]
# ------------------------------------------------------------------------------
# Constants

def LoadResource(name: str) -> str:
  dir_path = os.path.dirname(__file__)
  rsrc_path = os.path.join(dir_path, name)
  with open(rsrc_path, 'r') as f:
    return f.read()


# Handshake schema is pulled in during build
HANDSHAKE_REQUEST_SCHEMA_JSON = LoadResource('HandshakeRequest.avsc')
HANDSHAKE_RESPONSE_SCHEMA_JSON = LoadResource('HandshakeResponse.avsc')

HANDSHAKE_REQUEST_SCHEMA = schema.Parse(HANDSHAKE_REQUEST_SCHEMA_JSON)
HANDSHAKE_RESPONSE_SCHEMA = schema.Parse(HANDSHAKE_RESPONSE_SCHEMA_JSON)

HANDSHAKE_REQUESTOR_WRITER = avro_io.DatumWriter(HANDSHAKE_REQUEST_SCHEMA)
HANDSHAKE_REQUESTOR_READER = avro_io.DatumReader(HANDSHAKE_RESPONSE_SCHEMA)
HANDSHAKE_RESPONDER_WRITER = avro_io.DatumWriter(HANDSHAKE_RESPONSE_SCHEMA)
HANDSHAKE_RESPONDER_READER = avro_io.DatumReader(HANDSHAKE_REQUEST_SCHEMA)

META_SCHEMA = schema.Parse('{"type": "map", "values": "bytes"}')
META_WRITER = avro_io.DatumWriter(META_SCHEMA)
META_READER = avro_io.DatumReader(META_SCHEMA)

SYSTEM_ERROR_SCHEMA = schema.Parse('["string"]')

AVRO_RPC_MIME = 'avro/binary'

# protocol cache

# Decoder/encoder for a 32 bits big-endian integer.
UINT32_BE = avro_io.STRUCT_INT

# Default size of the buffers use to frame messages:
BUFFER_SIZE = 8192


# ------------------------------------------------------------------------------
# Exceptions


class AvroRemoteException(schema.AvroException):
  """
  Raised when an error message is sent by an Avro requestor or responder.
  """
  def __init__(self, fail_msg: Optional[str]=None) -> None:
    schema.AvroException.__init__(self, fail_msg)

class ConnectionClosedException(schema.AvroException):
  pass


# ------------------------------------------------------------------------------
# Base IPC Classes (Requestor/Responder)


class BaseRequestor(object, metaclass=abc.ABCMeta):
  """Base class for the client side of a protocol interaction."""

  def __init__(self, local_protocol: protocol.Protocol, transceiver: Transceiver) -> None:
    """Initializes a new requestor object.

    Args:
      local_protocol: Avro Protocol describing the messages sent and received.
      transceiver: Transceiver instance to channel messages through.
    """
    self._local_protocol = local_protocol
    self._transceiver = transceiver
    self._remote_protocol = None
    self._remote_hash = None
    self._send_protocol = None

  @property
  def local_protocol(self) -> protocol.Protocol:
    """Returns: the Avro Protocol describing the messages sent and received."""
    return self._local_protocol

  @property
  def transceiver(self) -> Transceiver:
    """Returns: the underlying channel used by this requestor."""
    return self._transceiver

  @abc.abstractmethod
  def _IssueRequest(self, call_request: bytes, message_name: str, request_datum: RequestDatumType) -> None:
    """TODO: Document this method.

    Args:
      call_request: ???
      message_name: Name of the message.
      request_datum: ???
    Returns:
      ???
    """

  def Request(self, message_name: str, request_datum: RequestDatumType) -> None:
    """Writes a request message and reads a response or error message.

    Args:
      message_name: Name of the IPC method.
      request_datum: IPC request.
    Returns:
      The IPC response.
    """
    # build handshake and call request
    buffer_writer = io.BytesIO()
    buffer_encoder = avro_io.BinaryEncoder(buffer_writer)
    self._WriteHandshakeRequest(buffer_encoder)
    self._WriteCallRequest(message_name, request_datum, buffer_encoder)

    # send the handshake and call request; block until call response
    call_request = buffer_writer.getvalue()
    return self._IssueRequest(call_request, message_name, request_datum)

  def _WriteHandshakeRequest(self, encoder: avro_io.BinaryEncoder) -> None:
    """Emits the handshake request.

    Args:
      encoder: Encoder to write the handshake request into.
    """
    local_hash = self._local_protocol.md5

    if self._remote_hash is None:
      self._remote_hash = local_hash
      self._remote_protocol = self._local_protocol

    request_datum = {
      'clientHash': local_hash,
      'serverHash': self._remote_hash,
    }
    if self._send_protocol:
      request_datum['clientProtocol'] = str(self._local_protocol)

    logger.info('Sending handshake request: %s', request_datum)
    HANDSHAKE_REQUESTOR_WRITER.write(request_datum, encoder)

  def _WriteCallRequest(self, message_name: str, request_datum: RequestDatumType, encoder: avro_io.BinaryEncoder) -> None:
    """
    The format of a call request is:
      * request metadata, a map with values of type bytes
      * the message name, an Avro string, followed by
      * the message parameters. Parameters are serialized according to
        the message's request declaration.
    """
    # request metadata (not yet implemented)
    request_metadata = {} # type: RequestDatumType
    META_WRITER.write(request_metadata, encoder)

    # Identify message to send:
    message = self.local_protocol.message_map.get(message_name)
    if message is None:
      raise schema.AvroException('Unknown message: %s' % message_name)
    encoder.write_utf8(message.name)

    # message parameters
    self._WriteRequest(message.request, request_datum, encoder)

  def _WriteRequest(self, request_schema: schema.Schema, request_datum: RequestDatumType, encoder: avro_io.BinaryEncoder) -> None:
    logger.info('writing request: %s', request_datum)
    datum_writer = avro_io.DatumWriter(request_schema)
    datum_writer.write(request_datum, encoder)

  def _ReadHandshakeResponse(self, decoder: avro_io.BinaryDecoder) -> bool:
    """Reads and processes the handshake response message.

    Args:
      decoder: Decoder to read messages from.
    Returns:
      call-response exists (boolean) ???
    Raises:
      schema.AvroException on ???
    """
    handshake_response = HANDSHAKE_REQUESTOR_READER.read(decoder)
    logger.info('Processing handshake response: %s', handshake_response)
    match = handshake_response['match']
    if match == 'BOTH':
      # Both client and server protocol hashes match:
      self._send_protocol = False
      return True

    elif match == 'CLIENT':
      # Client's side hash mismatch:
      self._remote_protocol = \
          protocol.Parse(handshake_response['serverProtocol'])
      self._remote_hash = handshake_response['serverHash']
      self._send_protocol = False
      return True

    elif match == 'NONE':
      # Neither client nor server match:
      self._remote_protocol = \
          protocol.Parse(handshake_response['serverProtocol'])
      self._remote_hash = handshake_response['serverHash']
      self._send_protocol = True
      return False
    else:
      raise schema.AvroException('handshake_response.match=%r' % match)

  def _ReadCallResponse(self, message_name: str, decoder: avro_io.BinaryDecoder) -> None:
    """Reads and processes a method call response.

    The format of a call response is:
      - response metadata, a map with values of type bytes
      - a one-byte error flag boolean, followed by either:
        - if the error flag is false,
          the message response, serialized per the message's response schema.
        - if the error flag is true,
          the error, serialized per the message's error union schema.

    Args:
      message_name:
      decoder:
    Returns:
      ???
    Raises:
      schema.AvroException on ???
    """
    # response metadata
    response_metadata = META_READER.read(decoder)

    # remote response schema
    remote_message_schema = self._remote_protocol.message_map.get(message_name)
    if remote_message_schema is None:
      raise schema.AvroException('Unknown remote message: %s' % message_name)

    # local response schema
    local_message_schema = self._local_protocol.message_map.get(message_name)
    if local_message_schema is None:
      raise schema.AvroException('Unknown local message: %s' % message_name)

    # error flag
    if not decoder.read_boolean():
      writer_schema = remote_message_schema.response
      reader_schema = local_message_schema.response
      return self._ReadResponse(writer_schema, reader_schema, decoder)
    else:
      writer_schema = remote_message_schema.errors
      reader_schema = local_message_schema.errors
      raise self._ReadError(writer_schema, reader_schema, decoder)

  def _ReadResponse(self, writer_schema: schema.Schema, reader_schema: schema.Schema, decoder: avro_io.BinaryDecoder) -> schema.AvroType:
    datum_reader = avro_io.DatumReader(writer_schema, reader_schema)
    result = datum_reader.read(decoder)
    return result

  def _ReadError(self, writer_schema: schema.Schema, reader_schema: schema.Schema, decoder: avro_io.BinaryDecoder) -> AvroRemoteException:
    datum_reader = avro_io.DatumReader(writer_schema, reader_schema)
    return AvroRemoteException(datum_reader.read(decoder))


class Requestor(BaseRequestor):
  """Concrete requestor implementation."""

  def _IssueRequest(self, call_request: bytes, message_name: str, request_datum: RequestDatumType) -> None:
    call_response = self.transceiver.Transceive(call_request)

    # process the handshake and call response
    buffer_decoder = avro_io.BinaryDecoder(io.BytesIO(call_response))
    call_response_exists = self._ReadHandshakeResponse(buffer_decoder)
    if call_response_exists:
      return self._ReadCallResponse(message_name, buffer_decoder)
    else:
      return self.Request(message_name, request_datum)


# ------------------------------------------------------------------------------


class Responder(object, metaclass=abc.ABCMeta):
  """Base class for the server side of a protocol interaction."""

  def __init__(self, local_protocol: protocol.Protocol) -> None:
    self._local_protocol = local_protocol
    self._local_hash = self._local_protocol.md5
    self._protocol_cache = { self._local_hash: self._local_protocol }

  @property
  def local_protocol(self) -> protocol.Protocol:
    return self._local_protocol

  # utility functions to manipulate protocol cache
  def get_protocol_cache(self, hash: str) -> Optional[protocol.Protocol]:
    return self._protocol_cache.get(hash)

  def set_protocol_cache(self, hash: str, protocol: protocol.Protocol) -> None:
    self._protocol_cache[hash] = protocol

  def Respond(self, call_request: bytes) -> bytes:
    """Entry point to process one procedure call.

    Args:
      call_request: Serialized procedure call request.
    Returns:
      Serialized procedure call response.
    Raises:
      ???
    """
    buffer_reader = io.BytesIO(call_request)
    buffer_decoder = avro_io.BinaryDecoder(buffer_reader)
    buffer_writer = io.BytesIO()
    buffer_encoder = avro_io.BinaryEncoder(buffer_writer)
    error = None
    response_metadata = {}

    try:
      remote_protocol = self._ProcessHandshake(buffer_decoder, buffer_encoder)
      # handshake failure
      if remote_protocol is None:
        return buffer_writer.getvalue()

      # read request using remote protocol
      request_metadata = META_READER.read(buffer_decoder)
      remote_message_name = buffer_decoder.read_utf8()

      # get remote and local request schemas so we can do
      # schema resolution (one fine day)
      remote_message = remote_protocol.message_map.get(remote_message_name)
      if remote_message is None:
        fail_msg = 'Unknown remote message: %s' % remote_message_name
        raise schema.AvroException(fail_msg)
      local_message = self.local_protocol.message_map.get(remote_message_name)
      if local_message is None:
        fail_msg = 'Unknown local message: %s' % remote_message_name
        raise schema.AvroException(fail_msg)
      writer_schema = remote_message.request
      reader_schema = local_message.request
      request = self._ReadRequest(writer_schema, reader_schema, buffer_decoder)
      logger.info('Processing request: %r', request)

      # perform server logic
      try:
        response = self.Invoke(local_message, request)
      except AvroRemoteException as exn:
        error = exn
      except Exception as exn:
        error = AvroRemoteException(str(exn))

      # write response using local protocol
      META_WRITER.write(response_metadata, buffer_encoder)
      buffer_encoder.write_boolean(error is not None)
      if error is None:
        writer_schema = local_message.response
        self._WriteResponse(writer_schema, response, buffer_encoder)
      else:
        writer_schema = local_message.errors
        self._WriteError(writer_schema, error, buffer_encoder)
    except schema.AvroException as exn:
      error = AvroRemoteException(str(exn))
      buffer_encoder = avro_io.BinaryEncoder(io.StringIO())
      META_WRITER.write(response_metadata, buffer_encoder)
      buffer_encoder.write_boolean(True)
      self._WriteError(SYSTEM_ERROR_SCHEMA, error, buffer_encoder)
    return buffer_writer.getvalue()

  def _ProcessHandshake(self, decoder: avro_io.BinaryDecoder, encoder: avro_io.BinaryEncoder) -> protocol.Protocol:
    """Processes an RPC handshake.

    Args:
      decoder: Where to read from.
      encoder: Where to write to.
    Returns:
      The requested Protocol.
    """
    handshake_request = HANDSHAKE_RESPONDER_READER.read(decoder)
    logger.info('Processing handshake request: %s', handshake_request)

    # determine the remote protocol
    client_hash = handshake_request.get('clientHash')
    client_protocol = handshake_request.get('clientProtocol')
    remote_protocol = self.get_protocol_cache(client_hash)
    if remote_protocol is None and client_protocol is not None:
      remote_protocol = protocol.Parse(client_protocol)
      self.set_protocol_cache(client_hash, remote_protocol)

    # evaluate remote's guess of the local protocol
    server_hash = handshake_request.get('serverHash')

    handshake_response = {}
    if self._local_hash == server_hash:
      if remote_protocol is None:
        handshake_response['match'] = 'NONE'
      else:
        handshake_response['match'] = 'BOTH'
    else:
      if remote_protocol is None:
        handshake_response['match'] = 'NONE'
      else:
        handshake_response['match'] = 'CLIENT'

    if handshake_response['match'] != 'BOTH':
      handshake_response['serverProtocol'] = str(self.local_protocol)
      handshake_response['serverHash'] = self._local_hash

    logger.info('Handshake response: %s', handshake_response)
    HANDSHAKE_RESPONDER_WRITER.write(handshake_response, encoder)
    return remote_protocol

  @abc.abstractmethod
  def Invoke(self, local_message: str, request: None) -> None:
    """Processes one procedure call.

    Args:
      local_message: Avro message specification.
      request: Call request.
    Returns:
      Call response.
    Raises:
      ???
    """

  def _ReadRequest(self, writer_schema: schema.Schema, reader_schema: schema.Schema, decoder: avro_io.BinaryDecoder) -> schema.AvroType:
    datum_reader = avro_io.DatumReader(writer_schema, reader_schema)
    return datum_reader.read(decoder)

  def _WriteResponse(self, writer_schema: schema.Schema, response_datum: schema.AvroType, encoder: avro_io.BinaryEncoder) -> None:
    datum_writer = avro_io.DatumWriter(writer_schema)
    datum_writer.write(response_datum, encoder)

  def _WriteError(self, writer_schema: schema.Schema, error_exception: BaseException, encoder: avro_io.BinaryEncoder) -> None:
    datum_writer = avro_io.DatumWriter(writer_schema)
    datum_writer.write(str(error_exception), encoder)


# ------------------------------------------------------------------------------
# Framed message


class FramedReader(object):
  """Wrapper around a file-like object to read framed data."""

  def __init__(self, reader: IO) -> None:
    self._reader = reader

  def Read(self) -> bytes:
    """Reads one message from the configured reader.

    Returns:
      The message, as bytes.
    """
    message = io.BytesIO()
    # Read and append frames until we encounter a 0-size frame:
    while self._ReadFrame(message) > 0: pass
    return message.getvalue()

  def _ReadFrame(self, message: IO) -> int:
    """Reads and appends one frame into the given message bytes.

    Args:
      message: Message to append the frame to.
    Returns:
      Size of the frame that was read.
      The empty frame (size 0) indicates the end of a message.
    """
    frame_size = self._ReadInt32()
    remaining = frame_size
    while remaining > 0:
      data_bytes = self._reader.read(remaining)
      if len(data_bytes) == 0:
        raise ConnectionClosedException(
            'FramedReader: expecting %d more bytes in frame of size %d, got 0.'
            % (remaining, frame_size))
      message.write(data_bytes)
      remaining -= len(data_bytes)
    return frame_size

  def _ReadInt32(self) -> int:
    encoded = self._reader.read(UINT32_BE.size)
    if len(encoded) != UINT32_BE.size:
      raise ConnectionClosedException('Invalid header: %r' % encoded)
    return UINT32_BE.unpack(encoded)[0]


class FramedWriter(object):
  """Wrapper around a file-like object to write framed data."""

  def __init__(self, writer: IO) -> None:
    self._writer = writer

  def Write(self, message: bytes) -> None:
    """Writes a message.

    Message is chunked into sequences of frames terminated by an empty frame.

    Args:
      message: Message to write, as bytes.
    """
    while len(message) > 0:
      chunk_size = max(BUFFER_SIZE, len(message))
      chunk = message[:chunk_size]
      self._WriteBuffer(chunk)
      message = message[chunk_size:]

    # A message is always terminated by a zero-length buffer.
    self._WriteUnsignedInt32(0)

  def _WriteBuffer(self, chunk: bytes) -> None:
    self._WriteUnsignedInt32(len(chunk))
    self._writer.write(chunk)

  def _WriteUnsignedInt32(self, uint32: int) -> None:
    self._writer.write(UINT32_BE.pack(uint32))


# ------------------------------------------------------------------------------
# Transceiver (send/receive channel)


class Transceiver(object, metaclass=abc.ABCMeta):
  @abc.abstractproperty
  def remote_name(self) -> str:
    pass

  @abc.abstractmethod
  def ReadMessage(self) -> bytes:
    """Reads a single message from the channel.

    Blocks until a message can be read.

    Returns:
      The message read from the channel.
    """
    pass

  @abc.abstractmethod
  def WriteMessage(self, message: bytes) -> None:
    """Writes a message into the channel.

    Blocks until the message has been written.

    Args:
      message: Message to write.
    """
    pass

  def Transceive(self, request: bytes) -> bytes:
    """Processes a single request-reply interaction.

    Synchronous request-reply interaction.

    Args:
      request: Request message.
    Returns:
      The reply message.
    """
    self.WriteMessage(request)
    result = self.ReadMessage()
    return result

  def Close(self) -> None:
    """Closes this transceiver."""
    pass


class HTTPTransceiver(Transceiver):
  """HTTP-based transceiver implementation."""

  def __init__(self, host: str, port: int, req_resource: str='/', ssl: bool=False) -> None:
    """Initializes a new HTTP transceiver.

    Args:
      host: Name or IP address of the remote host to interact with.
      port: Port the remote server is listening on.
      req_resource: Optional HTTP resource path to use, '/' by default.
    """
    self._req_resource = req_resource
    if ssl:
        self._conn = http.client.HTTPSConnection(host, port)
    else:
        self._conn = http.client.HTTPConnection(host, port)
    self._conn.connect()
    self._remote_name = self._conn.sock.getsockname()

  @property
  def remote_name(self) -> str:
    return self._remote_name

  def ReadMessage(self) -> bytes:
    response = self._conn.getresponse()
    response_reader = FramedReader(response)
    framed_message = response_reader.Read()
    response.read()    # ensure we're ready for subsequent requests
    return framed_message

  def WriteMessage(self, message: bytes) -> None:
    req_method = 'POST'
    req_headers = {'Content-Type': AVRO_RPC_MIME}

    bio = io.BytesIO()
    req_body_buffer = FramedWriter(bio)
    req_body_buffer.Write(message)
    req_body = bio.getvalue()

    self._conn.request(req_method, self._req_resource, req_body, req_headers)

  def Close(self) -> None:
    self._conn.close()
    self._conn = None


# ------------------------------------------------------------------------------
# Server Implementations


def _MakeHandlerClass(responder: Responder) -> Type[http.server.BaseHTTPRequestHandler]:
  class AvroHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self) -> None:
      reader = FramedReader(self.rfile)
      call_request = reader.Read()
      logger.info('Serialized request: %r', call_request)
      call_response = responder.Respond(call_request)
      logger.info('Serialized response: %r', call_response)

      self.send_response(200)
      self.send_header('Content-type', AVRO_RPC_MIME)
      self.end_headers()

      framed_writer = FramedWriter(self.wfile)
      framed_writer.Write(call_response)
      self.wfile.flush()
      logger.info('Response sent')

  return AvroHTTPRequestHandler


class MultiThreadedHTTPServer(
    socketserver.ThreadingMixIn,
    http.server.HTTPServer,
):
  """Multi-threaded HTTP server."""


class AvroIpcHttpServer(MultiThreadedHTTPServer):
  """Avro IPC server implemented on top of an HTTP server."""

  def __init__(self, interface: str, port: int, responder: Responder) -> None:
    """Initializes a new Avro IPC server.

    Args:
      interface: Interface the server listens on, eg. 'localhost' or '0.0.0.0'.
      port: TCP port the server listens on, eg. 8000.
      responder: Responder implementation to handle RPCs.
    """
    super(AvroIpcHttpServer, self).__init__(
        server_address=(interface, port),
        RequestHandlerClass=_MakeHandlerClass(responder),
    )


if __name__ == '__main__':
  raise Exception('Not a standalone module')
