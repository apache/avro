/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.util.Random;
import java.io.IOException;

public class TestProtocolReflect {

  public static class TestRecord {
    private String name;

    public int hashCode() {
      return this.name.hashCode();
    }

    public boolean equals(Object that) {
      return this.name.equals(((TestRecord) that).name);
    }
  }

  public interface Simple {
    String hello(String greeting);

    TestRecord echo(TestRecord record);

    int add(int arg1, int arg2);

    byte[] echoBytes(byte[] data);

    void error() throws SimpleException;
  }

  private static boolean throwUndeclaredError;

  public static class TestImpl implements Simple {
    public String hello(String greeting) {
      return "goodbye";
    }

    public int add(int arg1, int arg2) {
      return arg1 + arg2;
    }

    public TestRecord echo(TestRecord record) {
      return record;
    }

    public byte[] echoBytes(byte[] data) {
      return data;
    }

    public void error() throws SimpleException {
      if (throwUndeclaredError)
        throw new RuntimeException("foo");
      throw new SimpleException("foo");
    }
  }

  protected static Server server;
  protected static Transceiver client;
  protected static Simple proxy;

  @BeforeEach
  public void testStartServer() throws Exception {
    if (server != null)
      return;
    server = new SocketServer(new ReflectResponder(Simple.class, new TestImpl()), new InetSocketAddress(0));
    server.start();
    client = new SocketTransceiver(new InetSocketAddress(server.getPort()));
    proxy = ReflectRequestor.getClient(Simple.class, client);
  }

  @Test
  void classLoader() throws Exception {
    ClassLoader loader = new ClassLoader() {
    };

    ReflectResponder responder = new ReflectResponder(Simple.class, new TestImpl(), new ReflectData(loader));
    assertEquals(responder.getReflectData().getClassLoader(), loader);

    ReflectRequestor requestor = new ReflectRequestor(Simple.class, client, new ReflectData(loader));
    assertEquals(requestor.getReflectData().getClassLoader(), loader);
  }

  @Test
  void hello() throws IOException {
    String response = proxy.hello("bob");
    assertEquals("goodbye", response);
  }

  @Test
  void echo() throws IOException {
    TestRecord record = new TestRecord();
    record.name = "foo";
    TestRecord echoed = proxy.echo(record);
    assertEquals(record, echoed);
  }

  @Test
  void add() throws IOException {
    int result = proxy.add(1, 2);
    assertEquals(3, result);
  }

  @Test
  void echoBytes() throws IOException {
    Random random = new Random();
    int length = random.nextInt(1024 * 16);
    byte[] data = new byte[length];
    random.nextBytes(data);
    byte[] echoed = proxy.echoBytes(data);
    assertArrayEquals(data, echoed);
  }

  @Test
  // FIXME: Why does this test fail under JDK 21?
  @EnabledForJreRange(min = JRE.JAVA_8, max = JRE.JAVA_17, disabledReason = "Doesn't work under JRE 21, no clue why")
  void error() throws IOException {
    SimpleException error = null;
    try {
      proxy.error();
    } catch (SimpleException e) {
      error = e;
    }
    assertNotNull(error);
    assertEquals("foo", error.getMessage());
  }

  @Test
  void undeclaredError() throws Exception {
    this.throwUndeclaredError = true;
    RuntimeException error = null;
    try {
      proxy.error();
    } catch (AvroRuntimeException e) {
      error = e;
    } finally {
      this.throwUndeclaredError = false;
    }
    assertNotNull(error);
    assertTrue(error.toString().contains("foo"));
  }

  @AfterAll
  public static void testStopServer() throws IOException {
    client.close();
    server.close();
  }

}
