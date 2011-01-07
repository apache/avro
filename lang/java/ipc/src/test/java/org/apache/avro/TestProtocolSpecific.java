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
import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.Requestor;
import org.apache.avro.ipc.Responder;
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
import java.nio.ByteBuffer;
import java.util.Random;


public class TestProtocolSpecific {

  protected static final int REPEATING = -1;
  protected static final File SERVER_PORTS_DIR
  = new File(System.getProperty("test.dir", "/tmp")+"/server-ports/");

  public static int ackCount;

  private static boolean throwUndeclaredError;

  public static class TestImpl implements Simple {
    public CharSequence hello(CharSequence greeting) {
      return new Utf8("goodbye");
    }
    public int add(int arg1, int arg2) { return arg1 + arg2; }
    public TestRecord echo(TestRecord record) { return record; }
    public ByteBuffer echoBytes(ByteBuffer data) { return data; }
    public Void error() throws AvroRemoteException {
      if (throwUndeclaredError) throw new RuntimeException("foo");
      TestError error = new TestError();
      error.message = new Utf8("an error");
      throw error;
    }
    public void ack() { ackCount++; }
  }

  protected static Server server;
  protected static Transceiver client;
  protected static Simple proxy;

  protected static SpecificResponder responder;

  protected static HandshakeMonitor monitor;

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
  
  public void addRpcPlugins(Requestor requestor){}
  
  public Server createServer(Responder testResponder) throws Exception{
    return server = new SocketServer(testResponder,
                              new InetSocketAddress(0));   
  }
  
  public Transceiver createTransceiver() throws Exception{
    return new SocketTransceiver(new InetSocketAddress(server.getPort()));
  }

  @Test public void testGetRemote() throws IOException {
    assertEquals(Simple.PROTOCOL, SpecificRequestor.getRemote(proxy));
  }

  @Test
  public void testHello() throws IOException {
    CharSequence response = proxy.hello(new Utf8("bob"));
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
  }


  @Test
  public void testOneWay() throws IOException {
    ackCount = 0;
    proxy.ack();
    proxy.hello(new Utf8("foo"));                 // intermix normal req
    proxy.ack();
    try { Thread.sleep(100); } catch (InterruptedException e) {}
    assertEquals(2, ackCount);
  }
  
  @Test
  public void testRepeatedAccess() throws Exception {
    for (int x = 0; x < 1000; x++) {
      proxy.hello("hi!");
    }
  }
  
  @After
  public void testHandshakeOccursOnce() throws IOException{
    monitor.assertHandshake();
  }

  @AfterClass
  public static void testStopServer() throws IOException {
    client.close();
    server.close();
    server = null;
  }
  
  public class HandshakeMonitor extends RPCPlugin{
    
    private int handshakes;
    
    @Override
    public void serverConnecting(RPCContext context) {
      handshakes++;
      int expected = getExpectedHandshakeCount();
      if(expected > 0  && handshakes > expected){
        throw new IllegalStateException("Expected number of Protocol negotiation handshakes exceeded expected "+expected+" was "+handshakes);
      }
    }
    
    public void assertHandshake(){
      int expected = getExpectedHandshakeCount();
      if(expected != REPEATING){
        assertEquals("Expected number of handshakes did not take place.", expected, handshakes);
      }
    }
  }
  
  protected int getExpectedHandshakeCount() {
   return 1;
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
      server.start();
      File portFile = new File(SERVER_PORTS_DIR, "java-port");
      FileWriter w = new FileWriter(portFile);
      w.write(Integer.toString(server.getPort()));
      w.close();
    }
  }
}
