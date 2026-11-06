#!/usr/bin/env python3

##
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
Tests for avro.ipc. There are no IPC servers available to test against, so
requestors and responders talk to each other through an in-memory transceiver.
"""

import io
import unittest

import avro.errors
import avro.ipc
import avro.protocol

ECHO_PROTOCOL = avro.protocol.parse(
    """{
    "protocol": "Echo",
    "namespace": "org.apache.avro.test",
    "messages": {
        "echo": {"request": [{"name": "text", "type": "string"}], "response": "string"}
    }
}"""
)


class LoopbackTransceiver:
    """
    Pass each request straight to a responder.

    Requestors cache remote protocols in module globals keyed by remote_name,
    so give each test its own name to keep one test's handshake out of another.
    """

    def __init__(self, responder, remote_name):
        self.responder = responder
        self.remote_name = remote_name

    def transceive(self, request):
        return self.responder.respond(request)


class EchoResponder(avro.ipc.Responder):
    def invoke(self, local_message, request):
        return request["text"]


class UnreadableRequestResponder(EchoResponder):
    def read_request(self, writers_schema, readers_schema, decoder):
        raise avro.errors.AvroException("Cannot read request.")


class WrongResponseTypeResponder(EchoResponder):
    def invoke(self, local_message, request):
        return 42


class TestIPC(unittest.TestCase):
    def test_server_with_path(self):
        client_with_custom_path = avro.ipc.HTTPTransceiver("apache.org", 80, "/service/article")
        self.assertEqual("/service/article", client_with_custom_path.req_resource)

        client_with_default_path = avro.ipc.HTTPTransceiver("apache.org", 80)
        self.assertEqual("/", client_with_default_path.req_resource)

    def test_round_trip(self):
        """The first request completes the handshake and gets its response."""
        transceiver = LoopbackTransceiver(EchoResponder(ECHO_PROTOCOL), "test_round_trip")
        requestor = avro.ipc.Requestor(ECHO_PROTOCOL, transceiver)
        self.assertEqual("hello", requestor.request("echo", {"text": "hello"}))

    def test_system_error_reaches_requestor(self):
        """An error raised outside invoke() is sent to the requestor."""
        transceiver = LoopbackTransceiver(UnreadableRequestResponder(ECHO_PROTOCOL), "test_system_error_reaches_requestor")
        requestor = avro.ipc.Requestor(ECHO_PROTOCOL, transceiver)
        with self.assertRaisesRegex(avro.errors.AvroRemoteException, "Cannot read request."):
            requestor.request("echo", {"text": "hello"})

    def test_invalid_response_reaches_requestor(self):
        """A response that does not match its schema is sent to the requestor as an error."""
        transceiver = LoopbackTransceiver(WrongResponseTypeResponder(ECHO_PROTOCOL), "test_invalid_response_reaches_requestor")
        requestor = avro.ipc.Requestor(ECHO_PROTOCOL, transceiver)
        with self.assertRaisesRegex(avro.errors.AvroRemoteException, "not an example of the schema"):
            requestor.request("echo", {"text": "hello"})

    def test_empty_reader(self):
        response_reader = avro.ipc.FramedReader(io.BytesIO(b"Bad Response"))
        with self.assertRaises(avro.errors.ConnectionClosedException):
            response_reader.read_framed_message()


if __name__ == "__main__":  # pragma: no coverage
    unittest.main()
