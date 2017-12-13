/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro;

import java.net.InetSocketAddress;

import org.apache.avro.ipc.Requestor;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.SocketServer;
import org.apache.avro.ipc.SocketTransceiver;
import org.apache.avro.ipc.Transceiver;


public class TestProtocolSpecificMeta extends TestProtocolSpecific {

  @Override
  public Server createServer(Responder testResponder) throws Exception {
    responder.addRPCPlugin(new RPCMetaTestPlugin("key1"));
    responder.addRPCPlugin(new RPCMetaTestPlugin("key2"));
    return new SocketServer(responder, new InetSocketAddress(0));
  }

  @Override
  public Transceiver createTransceiver() throws Exception {
    return new SocketTransceiver(new InetSocketAddress(server.getPort()));
  }

  public void addRpcPlugins(Requestor req){
    req.addRPCPlugin(new RPCMetaTestPlugin("key1"));
    req.addRPCPlugin(new RPCMetaTestPlugin("key2"));
  }
}
