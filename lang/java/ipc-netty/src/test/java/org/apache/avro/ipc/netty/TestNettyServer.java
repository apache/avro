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

package org.apache.avro.ipc.netty;

import java.io.IOException;
import java.io.OutputStream;
import io.netty.channel.socket.SocketChannel;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.test.Mail;
import org.apache.avro.test.Message;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestNettyServer {
  private static final Logger LOG = LoggerFactory.getLogger(TestNettyServer.class.getName());

  static final int CONNECT_TIMEOUT_MILLIS = 2000; // 2 sec
  protected static Server server;
  protected static Transceiver transceiver;
  protected static Mail proxy;
  protected static MailImpl mailService;
  protected static Consumer<SocketChannel> channelInitializer;

  public static class MailImpl implements Mail {

    private CountDownLatch allMessages = new CountDownLatch(5);

    // in this simple example just return details of the message
    public String send(Message message) {
      return "Sent message to [" + message.getTo() + "] from [" + message.getFrom() + "] with body ["
          + message.getBody() + "]";
    }

    public void fireandforget(Message message) {
      allMessages.countDown();
    }

    private void awaitMessages() throws InterruptedException {
      allMessages.await(2, TimeUnit.SECONDS);
    }

    private void assertAllMessagesReceived() {
      assertEquals(0, allMessages.getCount());
    }

    public void reset() {
      allMessages = new CountDownLatch(5);
    }
  }

  public static void initializeConnections(Consumer<SocketChannel> initializer) throws Exception {
    initializeConnections(initializer, initializer);
  }

  public static void initializeConnections(Consumer<SocketChannel> serverInitializer,
      Consumer<SocketChannel> transceiverInitializer) throws Exception {
    LOG.info("starting server...");
    channelInitializer = transceiverInitializer;
    mailService = new MailImpl();
    Responder responder = new SpecificResponder(Mail.class, mailService);
    server = new NettyServer(responder, new InetSocketAddress(0), serverInitializer);
    server.start();

    int serverPort = server.getPort();
    LOG.info("server port : {}", serverPort);

    transceiver = new NettyTransceiver(new InetSocketAddress(serverPort), CONNECT_TIMEOUT_MILLIS,
        transceiverInitializer, null);
    proxy = SpecificRequestor.getClient(Mail.class, transceiver);
  }

  @BeforeAll
  public static void initializeConnections() throws Exception {
    initializeConnections(null);
  }

  @AfterAll
  public static void tearDownConnections() throws Exception {
    transceiver.close();
    server.close();
  }

  @Test
  void requestResponse() throws Exception {
    for (int x = 0; x < 5; x++) {
      verifyResponse(proxy.send(createMessage()));
    }
  }

  private void verifyResponse(String result) {
    assertEquals("Sent message to [wife] from [husband] with body [I love you!]", result);
  }

  @Test
  void oneway() throws Exception {
    for (int x = 0; x < 5; x++) {
      proxy.fireandforget(createMessage());
    }
    mailService.awaitMessages();
    mailService.assertAllMessagesReceived();
  }

  @Test
  void mixtureOfRequests() throws Exception {
    mailService.reset();
    for (int x = 0; x < 5; x++) {
      Message createMessage = createMessage();
      proxy.fireandforget(createMessage);
      verifyResponse(proxy.send(createMessage));
    }
    mailService.awaitMessages();
    mailService.assertAllMessagesReceived();

  }

  @Test
  void connectionsCount() throws Exception {
    // It happens on a regular basis that the server still has a connection
    // that is in the process of being terminated (previous test?).
    // We wait for that to happen because otherwise this test will fail.
    assertNumberOfConnectionsOnServer(1, 1000);

    Transceiver transceiver2 = new NettyTransceiver(new InetSocketAddress(server.getPort()), CONNECT_TIMEOUT_MILLIS,
        channelInitializer);
    Mail proxy2 = SpecificRequestor.getClient(Mail.class, transceiver2);
    proxy.fireandforget(createMessage());
    proxy2.fireandforget(createMessage());
    assertNumberOfConnectionsOnServer(2, 0);
    transceiver2.close();

    // Check the active connections with some retries as closing at the client
    // side might not take effect on the server side immediately
    assertNumberOfConnectionsOnServer(1, 5000);
  }

  /**
   * Assert for the number of server connections. This does repeated checks (with
   * timeout) if it not matches at first because closing at the client side might
   * not take effect on the server side immediately.
   *
   * @param wantedNumberOfConnections How many do we want to have
   * @param maxWaitMs                 Within how much time (0= immediately)
   */
  private static void assertNumberOfConnectionsOnServer(int wantedNumberOfConnections, long maxWaitMs)
      throws InterruptedException {
    int numActiveConnections = ((NettyServer) server).getNumActiveConnections();
    if (numActiveConnections == wantedNumberOfConnections) {
      return; // We're good.
    }
    long startMs = System.currentTimeMillis();
    long waited = 0;
    if (maxWaitMs > 0) {
      boolean timeOut = false;
      while (numActiveConnections != wantedNumberOfConnections && !timeOut) {
        LOG.info("Server still has {} active connections (want {}, waiting for {}ms); retrying...",
            numActiveConnections, wantedNumberOfConnections, waited);
        Thread.sleep(100);
        numActiveConnections = ((NettyServer) server).getNumActiveConnections();
        waited = System.currentTimeMillis() - startMs;
        timeOut = waited > maxWaitMs;
      }
      LOG.info("Server has {} active connections", numActiveConnections);
    }
    assertEquals(wantedNumberOfConnections, numActiveConnections,
        "Not the expected number of connections after a wait of " + waited + " ms");
  }

  private Message createMessage() {
    Message msg = Message.newBuilder().setTo("wife").setFrom("husband").setBody("I love you!").build();
    return msg;
  }

  // send a malformed request (HTTP) to the NettyServer port
  @Test
  void badRequest() throws IOException {
    int port = server.getPort();
    String msg = "GET /status HTTP/1.1\n\n";
    InetSocketAddress sockAddr = new InetSocketAddress("127.0.0.1", port);

    try (Socket sock = new Socket()) {
      sock.connect(sockAddr);
      OutputStream out = sock.getOutputStream();
      out.write(msg.getBytes(StandardCharsets.UTF_8));
      out.flush();
      byte[] buf = new byte[2048];
      int bytesRead = sock.getInputStream().read(buf);
      assertEquals(bytesRead, -1);
    }
  }

}
