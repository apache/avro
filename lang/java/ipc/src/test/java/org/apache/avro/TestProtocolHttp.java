/**
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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.generic.GenericData;
import org.apache.avro.test.Simple;

import org.junit.Test;

import java.net.URL;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;

public class TestProtocolHttp extends TestProtocolSpecific {

  @Override
  public Server createServer(Responder testResponder) throws Exception {
    return new HttpServer(testResponder, 0);
  }
  
  @Override
  public Transceiver createTransceiver() throws Exception{
    return new HttpTransceiver(new URL("http://127.0.0.1:"+server.getPort()+"/"));
  }
 
  protected int getExpectedHandshakeCount() {
    return REPEATING;
  }

  @Test(expected=SocketTimeoutException.class)
    public void testTimeout() throws Throwable {
    ServerSocket s = new ServerSocket(0);
    HttpTransceiver client =
      new HttpTransceiver(new URL("http://127.0.0.1:"+s.getLocalPort()+"/"));
    client.setTimeout(100);
    Simple proxy = SpecificRequestor.getClient(Simple.class, client);
    try {
      proxy.hello("foo");
    } catch (UndeclaredThrowableException e) {
      throw e.getCause();
    } finally {
      s.close();
    }
  }

  /** Test that Responder ignores one-way with stateless transport. */
  @Test public void testStatelessOneway() throws Exception {
    // a version of the Simple protocol that doesn't declare "ack" one-way
    Protocol protocol = new Protocol("Simple", "org.apache.avro.test");
    Protocol.Message message =
      protocol.createMessage("ack", null,
                             Schema.createRecord(new ArrayList<Field>()),
                             Schema.create(Schema.Type.NULL),
                             Schema.createUnion(new ArrayList<Schema>()));
    protocol.getMessages().put("ack", message);

    // call a server over a stateless protocol that has a one-way "ack"
    new GenericRequestor(protocol, createTransceiver())
      .request("ack", new GenericData.Record(message.getRequest()));
  }

}
