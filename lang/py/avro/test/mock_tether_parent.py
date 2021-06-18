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

import http.server
import sys
from typing import Mapping

import avro.errors
import avro.ipc
import avro.protocol
import avro.tether.tether_task
import avro.tether.util

SERVER_ADDRESS = ("localhost", avro.tether.util.find_port())


class MockParentResponder(avro.ipc.Responder):
    """
    The responder for the mocked parent
    """

    def __init__(self) -> None:
        super().__init__(avro.tether.tether_task.outputProtocol)

    def invoke(self, message: avro.protocol.Message, request: Mapping[str, str]) -> None:
        response = f"MockParentResponder: Received '{message.name}'"
        responses = {
            "configure": f"{response}': inputPort={request.get('port')}",
            "status": f"{response}: message={request.get('message')}",
            "fail": f"{response}: message={request.get('message')}",
        }
        print(responses.get(message.name, response))
        sys.stdout.flush()  # flush the output so it shows up in the parent process


class MockParentHandler(http.server.BaseHTTPRequestHandler):
    """Create a handler for the parent."""

    def do_POST(self) -> None:
        self.responder = MockParentResponder()
        call_request_reader = avro.ipc.FramedReader(self.rfile)
        call_request = call_request_reader.read_framed_message()
        resp_body = self.responder.respond(call_request)
        self.send_response(200)
        self.send_header("Content-Type", "avro/binary")
        self.end_headers()
        resp_writer = avro.ipc.FramedWriter(self.wfile)
        resp_writer.write_framed_message(resp_body)


def main() -> None:
    global SERVER_ADDRESS

    if len(sys.argv) != 3 or sys.argv[1].lower() != "start_server":
        raise avro.errors.UsageError("Usage: mock_tether_parent start_server port")

    try:
        port = int(sys.argv[2])
    except ValueError as e:
        raise avro.errors.UsageError("Usage: mock_tether_parent start_server port") from e

    SERVER_ADDRESS = (SERVER_ADDRESS[0], port)
    print(f"mock_tether_parent: Launching Server on Port: {SERVER_ADDRESS[1]}")

    # flush the output so it shows up in the parent process
    sys.stdout.flush()
    parent_server = http.server.HTTPServer(SERVER_ADDRESS, MockParentHandler)
    parent_server.allow_reuse_address = True
    parent_server.serve_forever()


if __name__ == "__main__":
    main()
