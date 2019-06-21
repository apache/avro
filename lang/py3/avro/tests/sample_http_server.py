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
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from avro import ipc
from avro import protocol

MAIL_PROTOCOL_JSON = """\
{"namespace": "example.proto",
 "protocol": "Mail",

 "types": [
     {"name": "Message", "type": "record",
      "fields": [
          {"name": "to",   "type": "string"},
          {"name": "from", "type": "string"},
          {"name": "body", "type": "string"}
      ]
     }
 ],

 "messages": {
     "send": {
         "request": [{"name": "message", "type": "Message"}],
         "response": "string"
     },
     "replay": {
         "request": [],
         "response": "string"
     }
 }
}
"""
MAIL_PROTOCOL = protocol.Parse(MAIL_PROTOCOL_JSON)
SERVER_ADDRESS = ('localhost', 9090)

class MailResponder(ipc.Responder):
  def __init__(self):
    ipc.Responder.__init__(self, MAIL_PROTOCOL)

  def invoke(self, message, request):
    if message.name == 'send':
      request_content = request['message']
      response = "Sent message to %(to)s from %(from)s with body %(body)s" % \
                 request_content
      return response
    elif message.name == 'replay':
      return 'replay'

class MailHandler(BaseHTTPRequestHandler):
  def do_POST(self):
    self.responder = MailResponder()
    call_request_reader = ipc.FramedReader(self.rfile)
    call_request = call_request_reader.read_framed_message()
    resp_body = self.responder.respond(call_request)
    self.send_response(200)
    self.send_header('Content-Type', 'avro/binary')
    self.end_headers()
    resp_writer = ipc.FramedWriter(self.wfile)
    resp_writer.write_framed_message(resp_body)

if __name__ == '__main__':
  mail_server = HTTPServer(SERVER_ADDRESS, MailHandler)
  mail_server.allow_reuse_address = True
  mail_server.serve_forever()
