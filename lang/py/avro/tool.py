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

"""
Command-line tool

NOTE: The API for the command-line tool is experimental.
"""

from __future__ import absolute_import, division, print_function

import os.path
import sys
import threading
import warnings

import avro.io
from avro import datafile, ipc, protocol

try:
    import BaseHTTPServer as http_server  # type: ignore
except ImportError:
    import http.server as http_server  # type: ignore

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse  # type: ignore


class GenericResponder(ipc.Responder):
    def __init__(self, proto, msg, datum):
        proto_json = open(proto, 'rb').read()
        ipc.Responder.__init__(self, protocol.parse(proto_json))
        self.msg = msg
        self.datum = datum

    def invoke(self, message, request):
        if message.name == self.msg:
            print("Message: %s Datum: %s" % (message.name, self.datum), file=sys.stderr)
            # server will shut down after processing a single Avro request
            global server_should_shutdown
            server_should_shutdown = True
            return self.datum


class GenericHandler(http_server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.responder = responder
        call_request_reader = ipc.FramedReader(self.rfile)
        call_request = call_request_reader.read_framed_message()
        resp_body = self.responder.respond(call_request)
        self.send_response(200)
        self.send_header('Content-Type', 'avro/binary')
        self.end_headers()
        resp_writer = ipc.FramedWriter(self.wfile)
        resp_writer.write_framed_message(resp_body)
        if server_should_shutdown:
            print("Shutting down server.", file=sys.stderr)
            quitter = threading.Thread(target=self.server.shutdown)
            quitter.daemon = True
            quitter.start()


def run_server(uri, proto, msg, datum):
    url_obj = urlparse(uri)
    server_addr = (url_obj.hostname, url_obj.port)
    global responder
    global server_should_shutdown
    server_should_shutdown = False
    responder = GenericResponder(proto, msg, datum)
    server = http_server.HTTPServer(server_addr, GenericHandler)
    print("Port: %s" % server.server_port)
    sys.stdout.flush()
    server.allow_reuse_address = True
    print("Starting server.", file=sys.stderr)
    server.serve_forever()


def send_message(uri, proto, msg, datum):
    url_obj = urlparse(uri)
    client = ipc.HTTPTransceiver(url_obj.hostname, url_obj.port)
    proto_json = open(proto, 'rb').read()
    requestor = ipc.Requestor(protocol.parse(proto_json), client)
    print(requestor.request(msg, datum))

##
# TODO: Replace this with fileinput()


def file_or_stdin(f):
    return sys.stdin if f == '-' else open(f, 'rb')


def main(args=sys.argv):
    if len(args) == 1:
        print("Usage: %s [dump|rpcreceive|rpcsend]" % args[0])
        return 1

    if args[1] == "dump":
        if len(args) != 3:
            print("Usage: %s dump input_file" % args[0])
            return 1
        for d in datafile.DataFileReader(file_or_stdin(args[2]), avro.io.DatumReader()):
            print(repr(d))
    elif args[1] == "rpcreceive":
        usage_str = "Usage: %s rpcreceive uri protocol_file " % args[0]
        usage_str += "message_name (-data d | -file f)"
        if len(args) not in [5, 7]:
            print(usage_str)
            return 1
        uri, proto, msg = args[2:5]
        datum = None
        if len(args) > 5:
            if args[5] == "-file":
                reader = open(args[6], 'rb')
                datum_reader = avro.io.DatumReader()
                dfr = datafile.DataFileReader(reader, datum_reader)
                datum = next(dfr)
            elif args[5] == "-data":
                print("JSON Decoder not yet implemented.")
                return 1
            else:
                print(usage_str)
                return 1
        run_server(uri, proto, msg, datum)
    elif args[1] == "rpcsend":
        usage_str = "Usage: %s rpcsend uri protocol_file " % args[0]
        usage_str += "message_name (-data d | -file f)"
        if len(args) not in [5, 7]:
            print(usage_str)
            return 1
        uri, proto, msg = args[2:5]
        datum = None
        if len(args) > 5:
            if args[5] == "-file":
                reader = open(args[6], 'rb')
                datum_reader = avro.io.DatumReader()
                dfr = datafile.DataFileReader(reader, datum_reader)
                datum = next(dfr)
            elif args[5] == "-data":
                print("JSON Decoder not yet implemented.")
                return 1
            else:
                print(usage_str)
                return 1
        send_message(uri, proto, msg, datum)
    return 0


if __name__ == "__main__":
    if os.path.dirname(avro.io.__file__) in sys.path:
        warnings.warn("Invoking avro/tool.py directly is likely to lead to a name collision "
                      "with the python io module. Try doing `python -m avro.tool` instead.")

    sys.exit(main(sys.argv))
