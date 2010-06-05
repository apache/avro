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

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.SocketServer;
import org.apache.avro.ipc.SocketTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.avro.specific.SpecificResponder;
import org.apache.avro.test.Simple;
import org.apache.avro.test.Kind;
import org.apache.avro.test.MD5;
import org.apache.avro.test.TestError;
import org.apache.avro.test.TestRecord;
import org.apache.avro.util.Utf8;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;


public class TestProtocolSpecific {

  protected static final File SERVER_PORTS_DIR
  = new File(System.getProperty("test.dir", "/tmp")+"/server-ports/");

  public static class TestImpl implements Simple {
    public Utf8 hello(Utf8 greeting) { return new Utf8("goodbye"); }
    public int add(int arg1, int arg2) { return arg1 + arg2; }
    public TestRecord echo(TestRecord record) { return record; }
    public ByteBuffer echoBytes(ByteBuffer data) { return data; }
    public Void error() throws AvroRemoteException {
      TestError error = new TestError();
      error.message = new Utf8("an error");
      throw error;
    }
  }

  protected static Server server;
  protected static Transceiver client;
  protected static Simple proxy;

  @Before
  public void testStartServer() throws Exception {
    server = new SocketServer(new SpecificResponder(Simple.class, new TestImpl()),
                              new InetSocketAddress(0));
    client = new SocketTransceiver(new InetSocketAddress(server.getPort()));
    proxy = (Simple)SpecificRequestor.getClient(Simple.class, client);
  }

  @Test
  public void testHello() throws IOException {
    Utf8 response = proxy.hello(new Utf8("bob"));
    assertEquals(new Utf8("goodbye"), response);
  }

  @Test
  public void testHashCode() throws IOException {
    TestError error = new TestError();
    error.hashCode();
  }

  @Test
  public void testEcho() throws IOException {
    TestRecord record = new TestRecord();
    record.name = new Utf8("foo");
    record.kind = Kind.BAR;
    record.hash = new MD5();
    System.arraycopy(new byte[]{0,1,2,3,4,5,6,7,8,9,0,1,2,3,4,5}, 0,
                     record.hash.bytes(), 0, 16);
    TestRecord echoed = proxy.echo(record);
    assertEquals(record, echoed);
    assertEquals(record.hashCode(), echoed.hashCode());
  }

  @Test
  public void testAdd() throws IOException {
    int result = proxy.add(1, 2);
    assertEquals(3, result);
  }

  @Test
  public void testEchoBytes() throws IOException {
    Random random = new Random();
    int length = random.nextInt(1024*16);
    ByteBuffer data = ByteBuffer.allocate(length);
    random.nextBytes(data.array());
    data.flip();
    ByteBuffer echoed = proxy.echoBytes(data);
    assertEquals(data, echoed);
  }

  @Test
  public void testEmptyEchoBytes() throws IOException {
    ByteBuffer data = ByteBuffer.allocate(0);
    ByteBuffer echoed = proxy.echoBytes(data);
    data.flip();
    assertEquals(data, echoed);
  }

  @Test
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

  @After
  public void testStopServer() throws IOException {
    client.close();
    server.close();
  }

  public static class InteropTest {

  @Test
    public void testClient() throws Exception {
      for (File f : SERVER_PORTS_DIR.listFiles()) {
        LineNumberReader reader = new LineNumberReader(new FileReader(f));
        int port = Integer.parseInt(reader.readLine());
        System.out.println("Validating java client to "+
            f.getName()+" - " + port);
        Transceiver client = new SocketTransceiver(
            new InetSocketAddress("localhost", port));
        proxy = (Simple)SpecificRequestor.getClient(Simple.class, client);
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
          new SpecificResponder(Simple.class, new TestImpl()),
          new InetSocketAddress(0));
      File portFile = new File(SERVER_PORTS_DIR, "java-port");
      FileWriter w = new FileWriter(portFile);
      w.write(Integer.toString(server.getPort()));
      w.close();
    }
  }
}
