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

import java.io.*;
import java.net.*;
import java.util.*;
import java.nio.ByteBuffer;
import junit.framework.TestCase;
import org.codehaus.jackson.map.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.*;
import org.apache.avro.Protocol.Message;
import org.apache.avro.io.*;
import org.apache.avro.ipc.*;
import org.apache.avro.generic.*;
import org.apache.avro.specific.*;
import org.apache.avro.util.*;

import org.apache.avro.Test.TestRecord;
import org.apache.avro.Test.TestError;

public class TestProtocolSpecific extends TestCase {
  private static final Logger LOG
    = LoggerFactory.getLogger(TestProtocolSpecific.class);

  private static final File SERVER_PORTS_DIR
  = new File(System.getProperty("test.dir", "/tmp")+"/server-ports/");

  private static final File FILE = new File("src/test/schemata/test.js");
  private static final Protocol PROTOCOL;
  static {
    try {
      PROTOCOL = Protocol.parse(FILE);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static class TestImpl implements Test {
    public Utf8 hello(Utf8 greeting) { return new Utf8("goodbye"); }
    public TestRecord echo(TestRecord record) { return record; }
    public ByteBuffer echoBytes(ByteBuffer data) { return data; }
    public Void error() throws AvroRemoteException {
      TestError error = new TestError();
      error.message = new Utf8("an error");
      throw error;
    }
  }

  protected static SocketServer server;
  protected static Transceiver client;
  protected static Test proxy;

  public void testStartServer() throws Exception {
    server = new SocketServer(new SpecificResponder(Test.class, new TestImpl()),
                              new InetSocketAddress(0));
    client = new SocketTransceiver(new InetSocketAddress(server.getPort()));
    proxy = (Test)SpecificRequestor.getClient(Test.class, client);
  }

  public void testHello() throws IOException {
    Utf8 response = proxy.hello(new Utf8("bob"));
    assertEquals(new Utf8("goodbye"), response);
  }

  public void testEcho() throws IOException {
    TestRecord record = new TestRecord();
    record.name = new Utf8("foo");
    TestRecord echoed = proxy.echo(record);
    assertEquals(record.name, echoed.name);
  }

  public void testEchoBytes() throws IOException {
    Random random = new Random();
    int length = random.nextInt(1024*16);
    ByteBuffer data = ByteBuffer.allocate(length);
    random.nextBytes(data.array());
    ByteBuffer echoed = proxy.echoBytes(data);
    data.flip();
    assertEquals(data, echoed);
  }

  public void testEmptyEchoBytes() throws IOException {
    ByteBuffer data = ByteBuffer.allocate(0);
    ByteBuffer echoed = proxy.echoBytes(data);
    data.flip();
    assertEquals(data, echoed);
  }

  public void testError() throws IOException {
    TestError error = null;
    try {
      proxy.error();
    } catch (TestError e) {
      error = e;
    }
    assertNotNull(error);
    assertEquals("an error", error.message.toString());
  }

  public void testStopServer() {
    server.close();
  }

  public static class InteropTest extends TestCase{

    public void testClient() throws Exception {
      for (File f : SERVER_PORTS_DIR.listFiles()) {
        LineNumberReader reader = new LineNumberReader(new FileReader(f));
        int port = Integer.parseInt(reader.readLine());
        System.out.println("Validating java client to "+
            f.getName()+" - " + port);
        Transceiver client = new SocketTransceiver(
            new InetSocketAddress("localhost", port));
        proxy = (Test)SpecificRequestor.getClient(Test.class, client);
        TestProtocolSpecific proto = new TestProtocolSpecific();
        proto.testHello();
        proto.testEcho();
        proto.testEchoBytes();
        proto.testError();
        System.out.println("Done! Validation java client to "+
            f.getName()+" - " + port);
      }
    }

    /**
     * Starts the RPC server.
     */
    public static void main(String[] args) throws Exception {
      SocketServer server = new SocketServer(
          new SpecificResponder(Test.class, new TestImpl()),
          new InetSocketAddress(0));
      File portFile = new File(SERVER_PORTS_DIR, "java-port");
      FileWriter w = new FileWriter(portFile);
      w.write(Integer.toString(server.getPort()));
      w.close();
    }
  }
}
