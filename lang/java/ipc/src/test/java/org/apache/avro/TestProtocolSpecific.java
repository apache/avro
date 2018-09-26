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

import org.apache.avro.specific.SpecificData;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.Requestor;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.SocketServer;
import org.apache.avro.ipc.SocketTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.test.Simple;
import org.apache.avro.test.Kind;
import org.apache.avro.test.MD5;
import org.apache.avro.test.TestError;
import org.apache.avro.test.TestRecord;
import org.junit.Test;
import org.junit.Before;
import org.junit.AfterClass;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;


/**
 * The type Test protocol specific.
 */
public class TestProtocolSpecific {

  /**
   * The constant REPEATING.
   */
  protected static final int REPEATING = -1;
  /**
   * The constant SERVER_PORTS_DIR.
   */
  protected static final File SERVER_PORTS_DIR
  = new File(System.getProperty("test.dir", "/tmp")+"/server-ports/");

  /**
   * The constant ackCount.
   */
  public static int ackCount;

  private static boolean throwUndeclaredError;

  /**
   * The type Test.
   */
  public static class TestImpl implements Simple {
    public String hello(String greeting) { return "goodbye"; }
    public int add(int arg1, int arg2) { return arg1 + arg2; }
    public TestRecord echo(TestRecord record) { return record; }
    public ByteBuffer echoBytes(ByteBuffer data) { return data; }
    public Void error() throws AvroRemoteException {
      if (throwUndeclaredError) throw new RuntimeException("foo");
      throw TestError.newBuilder().setMessage$("an error").build();
    }
    public void ack() { ackCount++; }
  }

  /**
   * The constant server.
   */
  protected static Server server;
  /**
   * The constant client.
   */
  protected static Transceiver client;
  /**
   * The constant proxy.
   */
  protected static Simple proxy;

  /**
   * The constant responder.
   */
  protected static SpecificResponder responder;

  /**
   * The constant monitor.
   */
  protected static HandshakeMonitor monitor;

  /**
   * Test start server.
   *
   * @throws Exception the exception
   */
  @Before
  public void testStartServer() throws Exception {
    if (server != null) return;
    responder = new SpecificResponder(Simple.class, new TestImpl());
    server = createServer(responder);
    server.start();

    client = createTransceiver();
    SpecificRequestor req = new SpecificRequestor(Simple.class, client);
    addRpcPlugins(req);
    proxy = SpecificRequestor.getClient(Simple.class, (SpecificRequestor)req);

    monitor = new HandshakeMonitor();
    responder.addRPCPlugin(monitor);
  }

  /**
   * Add rpc plugins.
   *
   * @param requestor the requestor
   */
  public void addRpcPlugins(Requestor requestor){}

  /**
   * Create server server.
   *
   * @param testResponder the test responder
   * @return the server
   * @throws Exception the exception
   */
  public Server createServer(Responder testResponder) throws Exception{
    return server = new SocketServer(testResponder,
                              new InetSocketAddress(0));
  }

  /**
   * Create transceiver transceiver.
   *
   * @return the transceiver
   * @throws Exception the exception
   */
  public Transceiver createTransceiver() throws Exception{
    return new SocketTransceiver(new InetSocketAddress(server.getPort()));
  }

  /**
   * Test class loader.
   *
   * @throws Exception the exception
   */
  @Test public void testClassLoader() throws Exception {
    ClassLoader loader = new ClassLoader() {};

    SpecificResponder responder
      = new SpecificResponder(Simple.class, new TestImpl(),
                              new SpecificData(loader));
    assertEquals(responder.getSpecificData().getClassLoader(), loader);

    SpecificRequestor requestor
      = new SpecificRequestor(Simple.class, client, new SpecificData(loader));
    assertEquals(requestor.getSpecificData().getClassLoader(), loader);
  }

  /**
   * Test get remote.
   *
   * @throws IOException the io exception
   */
  @Test public void testGetRemote() throws IOException {
    assertEquals(Simple.PROTOCOL, SpecificRequestor.getRemote(proxy));
  }

  /**
   * Test hello.
   *
   * @throws IOException the io exception
   */
  @Test
  public void testHello() throws IOException {
    String response = proxy.hello("bob");
    assertEquals("goodbye", response);
  }

  /**
   * Test hash code.
   *
   * @throws IOException the io exception
   */
  @Test
  public void testHashCode() throws IOException {
    TestError error = new TestError();
    error.hashCode();
  }

  /**
   * Test echo.
   *
   * @throws IOException the io exception
   */
  @Test
  public void testEcho() throws IOException {
    TestRecord record = new TestRecord();
    record.setName("foo");
    record.setKind(Kind.BAR);
    record.setHash(new MD5(new byte[]{0,1,2,3,4,5,6,7,8,9,0,1,2,3,4,5}));
    TestRecord echoed = proxy.echo(record);
    assertEquals(record, echoed);
    assertEquals(record.hashCode(), echoed.hashCode());
  }

  /**
   * Test add.
   *
   * @throws IOException the io exception
   */
  @Test
  public void testAdd() throws IOException {
    int result = proxy.add(1, 2);
    assertEquals(3, result);
  }

  /**
   * Test echo bytes.
   *
   * @throws IOException the io exception
   */
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

  /**
   * Test empty echo bytes.
   *
   * @throws IOException the io exception
   */
  @Test
  public void testEmptyEchoBytes() throws IOException {
    ByteBuffer data = ByteBuffer.allocate(0);
    ByteBuffer echoed = proxy.echoBytes(data);
    data.flip();
    assertEquals(data, echoed);
  }

  /**
   * Test error.
   *
   * @throws IOException the io exception
   */
  @Test
  public void testError() throws IOException {
    TestError error = null;
    try {
      proxy.error();
    } catch (TestError e) {
      error = e;
    }
    assertNotNull(error);
    assertEquals("an error", error.getMessage$());
  }

  /**
   * Test undeclared error.
   *
   * @throws Exception the exception
   */
  @Test
  public void testUndeclaredError() throws Exception {
    this.throwUndeclaredError = true;
    RuntimeException error = null;
    try {
      proxy.error();
    } catch (RuntimeException e) {
      error = e;
    } finally {
      this.throwUndeclaredError = false;
    }
    assertNotNull(error);
    assertTrue(error.toString().contains("foo"));
  }


  /**
   * Test one way.
   *
   * @throws IOException the io exception
   */
  @Test
  public void testOneWay() throws IOException {
    ackCount = 0;
    proxy.ack();
    proxy.hello("foo");                           // intermix normal req
    proxy.ack();
    try { Thread.sleep(100); } catch (InterruptedException e) {}
    assertEquals(2, ackCount);
  }

  /**
   * Test repeated access.
   *
   * @throws Exception the exception
   */
  @Test
  public void testRepeatedAccess() throws Exception {
    for (int x = 0; x < 1000; x++) {
      proxy.hello("hi!");
    }
  }

  /**
   * Test connection refused one way.
   *
   * @throws IOException the io exception
   */
  @Test(expected = Exception.class)
  public void testConnectionRefusedOneWay() throws IOException {
    Transceiver client = new HttpTransceiver(new URL("http://localhost:4444"));
    SpecificRequestor req = new SpecificRequestor(Simple.class, client);
    addRpcPlugins(req);
    Simple proxy = SpecificRequestor.getClient(Simple.class, (SpecificRequestor)req);
    proxy.ack();
  }

  /**
   * Test param variation.
   *
   * @throws Exception the exception
   */
  @Test
  /** Construct and use a protocol whose "hello" method has an extra
      argument to check that schema is sent to parse request. */
  public void testParamVariation() throws Exception {
    Protocol protocol = new Protocol("Simple", "org.apache.avro.test");
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("extra", Schema.create(Schema.Type.BOOLEAN),
                   null, null));
    fields.add(new Schema.Field("greeting", Schema.create(Schema.Type.STRING),
                   null, null));
    Protocol.Message message =
      protocol.createMessage("hello",
                             null /* doc */,
                             Schema.createRecord(fields),
                             Schema.create(Schema.Type.STRING),
                             Schema.createUnion(new ArrayList<>()));
    protocol.getMessages().put("hello", message);
    Transceiver t = createTransceiver();
    try {
      GenericRequestor r = new GenericRequestor(protocol, t);
      addRpcPlugins(r);
      GenericRecord params = new GenericData.Record(message.getRequest());
      params.put("extra", Boolean.TRUE);
      params.put("greeting", "bob");
      String response = r.request("hello", params).toString();
      assertEquals("goodbye", response);
    } finally {
      t.close();
    }
  }

  /**
   * Test handshake count.
   *
   * @throws IOException the io exception
   */
  @AfterClass
  public static void testHandshakeCount() throws IOException {
    monitor.assertHandshake();
  }

  /**
   * Test stop server.
   *
   * @throws IOException the io exception
   */
  @AfterClass
  public static void testStopServer() throws IOException {
    client.close();
    server.close();
    server = null;
  }

  /**
   * The type Handshake monitor.
   */
  public class HandshakeMonitor extends RPCPlugin{

    private int handshakes;
    private HashSet<String> seenProtocols = new HashSet<>();

    @Override
    public void serverConnecting(RPCContext context) {
      handshakes++;
      int expected = getExpectedHandshakeCount();
      if(expected > 0  && handshakes > expected){
        throw new IllegalStateException("Expected number of Protocol negotiation handshakes exceeded expected "+expected+" was "+handshakes);
      }

      // check that a given client protocol is only sent once
      String clientProtocol =
        context.getHandshakeRequest().clientProtocol;
      if (clientProtocol != null) {
        assertFalse(seenProtocols.contains(clientProtocol));
        seenProtocols.add(clientProtocol);
      }
    }

    /**
     * Assert handshake.
     */
    public void assertHandshake(){
      int expected = getExpectedHandshakeCount();
      if(expected != REPEATING){
        assertEquals("Expected number of handshakes did not take place.", expected, handshakes);
      }
    }
  }

  /**
   * Gets expected handshake count.
   *
   * @return the expected handshake count
   */
  protected int getExpectedHandshakeCount() {
   return 3;
  }

  /**
   * The type Interop test.
   */
  public static class InteropTest {

    /**
     * Test client.
     *
     * @throws Exception the exception
     */
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
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
      SocketServer server = new SocketServer(
          new SpecificResponder(Simple.class, new TestImpl()),
          new InetSocketAddress(0));
      server.start();
      File portFile = new File(SERVER_PORTS_DIR, "java-port");
      FileWriter w = new FileWriter(portFile);
      w.write(Integer.toString(server.getPort()));
      w.close();
    }
  }
}
