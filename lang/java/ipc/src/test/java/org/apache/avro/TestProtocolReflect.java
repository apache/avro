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

import org.apache.avro.reflect.ReflectData;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.SocketServer;
import org.apache.avro.ipc.SocketTransceiver;
import org.apache.avro.ipc.reflect.ReflectRequestor;
import org.apache.avro.ipc.reflect.ReflectResponder;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.Random;
import java.io.IOException;

/**
 * The type Test protocol reflect.
 */
public class TestProtocolReflect {

  /**
   * The type Test record.
   */
  public static class TestRecord {
    private String name;
    public int hashCode() { return this.name.hashCode(); }
    public boolean equals(Object that) {
      return this.name.equals(((TestRecord)that).name);
    }
  }

  /**
   * The interface Simple.
   */
  public interface Simple {
    /**
     * Hello string.
     *
     * @param greeting the greeting
     * @return the string
     */
    String hello(String greeting);

    /**
     * Echo test record.
     *
     * @param record the record
     * @return the test record
     */
    TestRecord echo(TestRecord record);

    /**
     * Add int.
     *
     * @param arg1 the arg 1
     * @param arg2 the arg 2
     * @return the int
     */
    int add(int arg1, int arg2);

    /**
     * Echo bytes byte [ ].
     *
     * @param data the data
     * @return the byte [ ]
     */
    byte[] echoBytes(byte[] data);

    /**
     * Error.
     *
     * @throws SimpleException the simple exception
     */
    void error() throws SimpleException;
  }

  private static boolean throwUndeclaredError;

  /**
   * The type Test.
   */
  public static class TestImpl implements Simple {
    public String hello(String greeting) { return "goodbye"; }
    public int add(int arg1, int arg2) { return arg1 + arg2; }
    public TestRecord echo(TestRecord record) { return record; }
    public byte[] echoBytes(byte[] data) { return data; }
    public void error() throws SimpleException {
      if (throwUndeclaredError) throw new RuntimeException("foo");
      throw new SimpleException("foo");
    }
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
   * Test start server.
   *
   * @throws Exception the exception
   */
  @Before
  public void testStartServer() throws Exception {
    if (server != null) return;
    server = new SocketServer(new ReflectResponder(Simple.class, new TestImpl()),
                              new InetSocketAddress(0));
    server.start();
    client = new SocketTransceiver(new InetSocketAddress(server.getPort()));
    proxy = ReflectRequestor.getClient(Simple.class, client);
  }

  /**
   * Test class loader.
   *
   * @throws Exception the exception
   */
  @Test public void testClassLoader() throws Exception {
    ClassLoader loader = new ClassLoader() {};

    ReflectResponder responder
      = new ReflectResponder(Simple.class, new TestImpl(),
                             new ReflectData(loader));
    assertEquals(responder.getReflectData().getClassLoader(), loader);

    ReflectRequestor requestor
      = new ReflectRequestor(Simple.class, client, new ReflectData(loader));
    assertEquals(requestor.getReflectData().getClassLoader(), loader);
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
   * Test echo.
   *
   * @throws IOException the io exception
   */
  @Test
  public void testEcho() throws IOException {
    TestRecord record = new TestRecord();
    record.name = "foo";
    TestRecord echoed = proxy.echo(record);
    assertEquals(record, echoed);
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
    byte[] data = new byte[length];
    random.nextBytes(data);
    byte[] echoed = proxy.echoBytes(data);
    assertArrayEquals(data, echoed);
  }

  /**
   * Test error.
   *
   * @throws IOException the io exception
   */
  @Test
  public void testError() throws IOException {
    SimpleException error = null;
    try {
      proxy.error();
    } catch (SimpleException e) {
      error = e;
    }
    assertNotNull(error);
    assertEquals("foo", error.getMessage());
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
   * Test stop server.
   *
   * @throws IOException the io exception
   */
  @AfterClass
  public static void testStopServer() throws IOException {
    client.close();
    server.close();
  }

}
