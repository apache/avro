#! /usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
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
import sys
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import urlparse
from avro import io
from avro import datafile
from avro import protocol
from avro import ipc

class GenericResponder(ipc.Responder):
  def __init__(self, proto, msg, datum):
    proto_json = file(proto, 'r').read()
    ipc.Responder.__init__(self, protocol.parse(proto_json))
    self.msg = msg
    self.datum = datum

  def invoke(self, message, request):
    if message.name == self.msg:
      print >> sys.stderr, "Message: %s Datum: %s" % (message.name, self.datum)
      # server will shut down after processing a single Avro request
      global server_should_shutdown
      server_should_shutdown = True
      return self.datum

class GenericHandler(BaseHTTPRequestHandler):
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
      print >> sys.stderr, "Shutting down server."
      self.server.force_stop()

class StoppableHTTPServer(HTTPServer):
  """HTTPServer.shutdown added in Python 2.6. FML."""
  stopped = False
  allow_reuse_address = True
  def __init__(self, *args, **kw):
    HTTPServer.__init__(self, *args, **kw)
    self.allow_reuse_address = True

  def serve_forever(self):
    while not self.stopped:
      self.handle_request()

  def force_stop(self):
    self.server_close()
    self.stopped = True
    self.serve_forever()

def run_server(uri, proto, msg, datum):
  url_obj = urlparse.urlparse(uri)
  server_addr = (url_obj.hostname, url_obj.port)
  global responder
  global server_should_shutdown
  server_should_shutdown = False
  responder = GenericResponder(proto, msg, datum)
  server = StoppableHTTPServer(server_addr, GenericHandler)
  print "Port: %s" % server.server_port
  sys.stdout.flush()
  server.allow_reuse_address = True
  print >> sys.stderr, "Starting server."
  server.serve_forever()

def send_message(uri, proto, msg, datum):
  url_obj = urlparse.urlparse(uri)
  client = ipc.HTTPTransceiver(url_obj.hostname, url_obj.port)
  proto_json = file(proto, 'r').read()
  requestor = ipc.Requestor(protocol.parse(proto_json), client)
  print requestor.request(msg, datum)

def file_or_stdin(f):
  if f == "-":
    return sys.stdin
  else:
    return file(f)

def main(args=sys.argv):
  if len(args) == 1:
    print "Usage: %s [dump|rpcreceive|rpcsend]" % args[0]
    return 1

  if args[1] == "dump":
    if len(args) != 3:
      print "Usage: %s dump input_file" % args[0]
      return 1
    for d in datafile.DataFileReader(file_or_stdin(args[2]), io.DatumReader()):
      print repr(d)
  elif args[1] == "rpcreceive":
    usage_str = "Usage: %s rpcreceive uri protocol_file " % args[0]
    usage_str += "message_name (-data d | -file f)"
    if len(args) not in [5, 7]:
      print usage_str
      return 1
    uri, proto, msg = args[2:5]
    datum = None
    if len(args) > 5:
      if args[5] == "-file":
        reader = open(args[6], 'rb')
        datum_reader = io.DatumReader()
        dfr = datafile.DataFileReader(reader, datum_reader)
        datum = dfr.next()
      elif args[5] == "-data":
        print "JSON Decoder not yet implemented."
        return 1
      else:
        print usage_str
        return 1
    run_server(uri, proto, msg, datum)
  elif args[1] == "rpcsend":
    usage_str = "Usage: %s rpcsend uri protocol_file " % args[0]
    usage_str += "message_name (-data d | -file f)"
    if len(args) not in [5, 7]:
      print usage_str
      return 1
    uri, proto, msg = args[2:5]
    datum = None
    if len(args) > 5:
      if args[5] == "-file":
        reader = open(args[6], 'rb')
        datum_reader = io.DatumReader()
        dfr = datafile.DataFileReader(reader, datum_reader)
        datum = dfr.next()
      elif args[5] == "-data":
        print "JSON Decoder not yet implemented."
        return 1
      else:
        print usage_str
        return 1
    send_message(uri, proto, msg, datum)
  return 0
  
if __name__ == "__main__":
  sys.exit(main(sys.argv))
