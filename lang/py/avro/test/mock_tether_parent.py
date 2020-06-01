#!/usr/bin/env python

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

from __future__ import absolute_import, division, print_function

import socket
import sys

import avro.tether.tether_task
import avro.tether.util
from avro import ipc, protocol

try:
    import BaseHTTPServer as http_server  # type: ignore
except ImportError:
    import http.server as http_server  # type: ignore


SERVER_ADDRESS = ('localhost', avro.tether.util.find_port())


class MockParentResponder(ipc.Responder):
    """
    The responder for the mocked parent
    """

    def __init__(self):
        ipc.Responder.__init__(self, avro.tether.tether_task.outputProtocol)

    def invoke(self, message, request):
        if message.name == 'configure':
            print("MockParentResponder: Received 'configure': inputPort={0}".format(request["port"]))

        elif message.name == 'status':
            print("MockParentResponder: Received 'status': message={0}".format(request["message"]))
        elif message.name == 'fail':
            print("MockParentResponder: Received 'fail': message={0}".format(request["message"]))
        else:
            print("MockParentResponder: Received {0}".format(message.name))

        # flush the output so it shows up in the parent process
        sys.stdout.flush()

        return None


class MockParentHandler(http_server.BaseHTTPRequestHandler):
    """Create a handler for the parent.
    """

    def do_POST(self):
        self.responder = MockParentResponder()
        call_request_reader = ipc.FramedReader(self.rfile)
        call_request = call_request_reader.read_framed_message()
        resp_body = self.responder.respond(call_request)
        self.send_response(200)
        self.send_header('Content-Type', 'avro/binary')
        self.end_headers()
        resp_writer = ipc.FramedWriter(self.wfile)
        resp_writer.write_framed_message(resp_body)


if __name__ == '__main__':
    if (len(sys.argv) <= 1):
        raise ValueError("Usage: mock_tether_parent command")

    cmd = sys.argv[1].lower()
    if (sys.argv[1] == 'start_server'):
        if (len(sys.argv) == 3):
            port = int(sys.argv[2])
        else:
            raise ValueError("Usage: mock_tether_parent start_server port")

        SERVER_ADDRESS = (SERVER_ADDRESS[0], port)
        print("mock_tether_parent: Launching Server on Port: {0}".format(SERVER_ADDRESS[1]))

        # flush the output so it shows up in the parent process
        sys.stdout.flush()
        parent_server = http_server.HTTPServer(SERVER_ADDRESS, MockParentHandler)
        parent_server.allow_reuse_address = True
        parent_server.serve_forever()
