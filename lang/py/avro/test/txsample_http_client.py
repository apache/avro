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
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import, division, print_function

import sys

from avro import protocol, txipc
from twisted.internet import defer, reactor
from twisted.python.util import println

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
MAIL_PROTOCOL = protocol.parse(MAIL_PROTOCOL_JSON)
SERVER_HOST = 'localhost'
SERVER_PORT = 9090


class UsageError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def make_requestor(server_host, server_port, protocol):
    client = txipc.TwistedHTTPTransceiver(SERVER_HOST, SERVER_PORT)
    return txipc.TwistedRequestor(protocol, client)


if __name__ == '__main__':
    if len(sys.argv) not in [4, 5]:
        raise UsageError("Usage: <to> <from> <body> [<count>]")

    # client code - attach to the server and send a message
    # fill in the Message record
    message = dict()
    message['to'] = sys.argv[1]
    message['from'] = sys.argv[2]
    message['body'] = sys.argv[3]

    try:
        num_messages = int(sys.argv[4])
    except IndexError:
        num_messages = 1

    # build the parameters for the request
    params = {}
    params['message'] = message

    requests = []
    # send the requests and print the result
    for msg_count in range(num_messages):
        requestor = make_requestor(SERVER_HOST, SERVER_PORT, MAIL_PROTOCOL)
        d = requestor.request('send', params)
        d.addCallback(lambda result: println("Result: " + result))
        requests.append(d)
    results = defer.gatherResults(requests)

    def replay_cb(result):
        print("Replay Result: " + result)
        reactor.stop()

    def replay(_):
        # try out a replay message
        requestor = make_requestor(SERVER_HOST, SERVER_PORT, MAIL_PROTOCOL)
        d = requestor.request('replay', dict())
        d.addCallback(replay_cb)

    results.addCallback(replay)
    reactor.run()
