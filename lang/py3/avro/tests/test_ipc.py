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
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
There are currently no IPC tests within python, in part because there are no
servers yet available.
"""

import logging
import threading
import time
import unittest

from avro import ipc
from avro import protocol
from avro import schema


def NowMS():
  return int(time.time() * 1000)


ECHO_PROTOCOL_JSON = """
{
  "protocol" : "Echo",
  "namespace" : "org.apache.avro.ipc.echo",
  "types" : [ {
    "type" : "record",
    "name" : "Ping",
    "fields" : [ {
      "name" : "timestamp",
      "type" : "long",
      "default" : -1
    }, {
      "name" : "text",
      "type" : "string",
      "default" : ""
    } ]
  }, {
    "type" : "record",
    "name" : "Pong",
    "fields" : [ {
      "name" : "timestamp",
      "type" : "long",
      "default" : -1
    }, {
      "name" : "ping",
      "type" : "Ping"
    } ]
  } ],
  "messages" : {
    "ping" : {
      "request" : [ {
        "name" : "ping",
        "type" : "Ping"
      } ],
      "response" : "Pong"
    }
  }
}
"""


ECHO_PROTOCOL = protocol.Parse(ECHO_PROTOCOL_JSON)


class EchoResponder(ipc.Responder):
  def __init__(self):
    super(EchoResponder, self).__init__(
        local_protocol=ECHO_PROTOCOL,
    )

  def Invoke(self, message, request):
    logging.info('Message: %s', message)
    logging.info('Request: %s', request)
    ping = request['ping']
    return {'timestamp': NowMS(), 'ping': ping}


class TestIPC(unittest.TestCase):

  def __init__(self, *args, **kwargs):
    super(TestIPC, self).__init__(*args, **kwargs)
    # Reference to an Echo RPC over HTTP server:
    self._server = None

  def StartEchoServer(self):
    self._server = ipc.AvroIpcHttpServer(
        interface='localhost',
        port=0,
        responder=EchoResponder(),
    )

    def ServerThread():
      self._server.serve_forever()

    self._server_thread = threading.Thread(target=ServerThread)
    self._server_thread.start()

    logging.info(
        'Echo RPC Server listening on %s:%s',
        *self._server.server_address)
    logging.info('RPC socket: %s', self._server.socket)

  def StopEchoServer(self):
    assert (self._server is not None)
    self._server.shutdown()
    self._server_thread.join()
    self._server.server_close()
    self._server = None

  def testEchoService(self):
    """Tests client-side of the Echo service."""
    self.StartEchoServer()
    try:
      (server_host, server_port) = self._server.server_address

      transceiver = ipc.HTTPTransceiver(host=server_host, port=server_port)
      requestor = ipc.Requestor(
          local_protocol=ECHO_PROTOCOL,
          transceiver=transceiver,
      )
      response = requestor.Request(
          message_name='ping',
          request_datum={'ping': {'timestamp': 31415, 'text': 'hello ping'}},
      )
      logging.info('Received echo response: %s', response)

      response = requestor.Request(
          message_name='ping',
          request_datum={'ping': {'timestamp': 123456, 'text': 'hello again'}},
      )
      logging.info('Received echo response: %s', response)

      transceiver.Close()

    finally:
      self.StopEchoServer()


if __name__ == '__main__':
  raise Exception('Use run_tests.py')
