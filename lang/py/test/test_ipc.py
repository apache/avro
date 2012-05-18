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
There are currently no IPC tests within python, in part because there are no
servers yet available.
"""
import time
import unittest
from multiprocessing import Process

# This test does import this code, to make sure it at least passes
# compilation.
from avro import ipc, txipc
from twisted.web import server
from twisted.internet import reactor
from txsample_http_server import MailResponder, MAIL_PROTOCOL


def test_twisted_server():
  root = server.Site(txipc.AvroResponderResource(MailResponder()))
  reactor.listenTCP(9097, root)
  reactor.run()


class TestIPCClient(unittest.TestCase):
  def setUp(self):
    self.testserver = Process(target=test_twisted_server)
    self.testserver.start()
    # Is there a better way to wait until the server is ready to accept
    # connections?
    time.sleep(1)

  def tearDown(self):
    self.testserver.terminate()

  def test_reconnect(self):
    message = {
      'to': 'john@bar.com',
      'from': 'jane@baz.org',
      'body': 'hello world',
    }

    client = ipc.HTTPTransceiver('localhost', 9097)
    requestor = ipc.Requestor(MAIL_PROTOCOL, client)

    expected = u'Sent message to john@bar.com from jane@baz.org with body hello world'
    params = {'message': message}
    for msg_count in range(1):
      self.assertEqual(expected, requestor.request('send', params))
    self.tearDown()
    self.setUp()
    time.sleep(1)
    for msg_count in range(2):
      self.assertEqual(expected, requestor.request('send', params))


class TestIPC(unittest.TestCase):
  def test_placeholder(self):
    pass

  def test_server_with_path(self):
    client_with_custom_path = ipc.HTTPTransceiver('dummyserver.net', 80, req_resource='/service/article')
    self.assertEqual('/service/article', client_with_custom_path.req_resource)

    client_with_default_path = ipc.HTTPTransceiver('dummyserver.net', 80)
    self.assertEqual('/', client_with_default_path.req_resource)

if __name__ == '__main__':
  unittest.main()
