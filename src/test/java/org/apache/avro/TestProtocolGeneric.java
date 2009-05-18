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

import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRequestor;
import org.apache.avro.generic.GenericResponder;
import org.apache.avro.ipc.*;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;

public class TestProtocolGeneric {
  private static final Logger LOG
    = LoggerFactory.getLogger(TestProtocolGeneric.class);

  private static final File FILE = new File("src/test/schemata/simple.js");
  private static final Protocol PROTOCOL;
  static {
    try {
      PROTOCOL = Protocol.parse(FILE);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class TestResponder extends GenericResponder {
    public TestResponder() { super(PROTOCOL); }
    public Object respond(Message message, Object request)
      throws AvroRemoteException {
      @SuppressWarnings(value="unchecked")
      GenericRecord params = (GenericRecord)request;

      if ("hello".equals(message.getName())) {
        LOG.info("hello: "+params.get("greeting"));
        return new Utf8("goodbye");
      }

      if ("echo".equals(message.getName())) {
        Object record = params.get("record");
        LOG.info("echo: "+record);
        return record;
      }

      if ("echoBytes".equals(message.getName())) {
        Object data = params.get("data");
        LOG.info("echoBytes: "+data);
        return data;
      }

      if ("error".equals(message.getName())) {
        GenericRecord error =
          new GenericData.Record(PROTOCOL.getTypes().get("TestError"));
        error.put("message", new Utf8("an error"));
        throw new AvroRemoteException(error);
      }
      
      throw new AvroRuntimeException("unexpected message: "+message.getName());
    }

  }

  private static SocketServer server;
  private static Transceiver client;
  private static Requestor requestor;

  @BeforeMethod
  public void testStartServer() throws Exception {
    server = new SocketServer(new TestResponder(), new InetSocketAddress(0));
    client = new SocketTransceiver(new InetSocketAddress(server.getPort()));
    requestor = new GenericRequestor(PROTOCOL, client);
  }

  @Test
  public void testHello() throws IOException {
    GenericRecord params = 
      new GenericData.Record(PROTOCOL.getMessages().get("hello").getRequest());
    params.put("greeting", new Utf8("bob"));
    Utf8 response = (Utf8)requestor.request("hello", params);
    assertEquals(new Utf8("goodbye"), response);
  }

  @Test
  public void testEcho() throws IOException {
    GenericRecord record =
      new GenericData.Record(PROTOCOL.getTypes().get("TestRecord"));
    record.put("name", new Utf8("foo"));
    GenericRecord params =
      new GenericData.Record(PROTOCOL.getMessages().get("echo").getRequest());
    params.put("record", record);
    Object echoed = requestor.request("echo", params);
    assertEquals(record, echoed);
  }

  @Test
  public void testEchoBytes() throws IOException {
    Random random = new Random();
    int length = random.nextInt(1024*16);
    GenericRecord params =
      new GenericData.Record(PROTOCOL.getMessages().get("echoBytes").getRequest());
    ByteBuffer data = ByteBuffer.allocate(length);
    random.nextBytes(data.array());
    params.put("data", data);
    Object echoed = requestor.request("echoBytes", params);
    data.flip();
    assertEquals(data, echoed);
  }

  @Test
  public void testError() throws IOException {
    GenericRecord params =
      new GenericData.Record(PROTOCOL.getMessages().get("error").getRequest());
    AvroRemoteException error = null;
    try {
      requestor.request("error", params);
    } catch (AvroRemoteException e) {
      error = e;
    }
    assertNotNull(error);
    assertEquals("an error", ((Map)error.getValue()).get("message").toString());
  }

  @AfterMethod
  public void testStopServer() {
    server.close();
  }
}
