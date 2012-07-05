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

package org.apache.avro.ipc;

import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import junit.framework.Assert;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.test.Mail;
import org.apache.avro.test.Message;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.SocketChannel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.ssl.SslHandler;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNettyServerWithSSL {
  public static final String TEST_CERTIFICATE = "servercert.p12";
  public static final String TEST_CERTIFICATE_PASSWORD = "s3cret";
  static final long CONNECT_TIMEOUT_MILLIS = 2000; // 2 sec
  private static Server server;
  private static Transceiver transceiver;
  private static Mail proxy;
  private static MailImpl mailService;

  public static class MailImpl implements Mail {

    private CountDownLatch allMessages = new CountDownLatch(5);

    // in this simple example just return details of the message
    public String send(Message message) {
      return "Sent message to [" + message.getTo().toString() +
             "] from [" + message.getFrom().toString() + "] with body [" +
             message.getBody().toString() + "]";
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

  @BeforeClass
  public static void initializeConnections() throws Exception {
    // start server
    System.out.println("starting server...");
    mailService = new MailImpl();
    Responder responder = new SpecificResponder(Mail.class, mailService);
    ChannelFactory channelFactory = new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool()
    );
    server = new NettyServer(responder, new InetSocketAddress(0),
                             channelFactory, new SSLChannelPipelineFactory(),
                             null);
    server.start();

    int serverPort = server.getPort();
    System.out.println("server port : " + serverPort);

    transceiver = new NettyTransceiver(new InetSocketAddress(serverPort),
                                       new SSLChannelFactory(),
                                       CONNECT_TIMEOUT_MILLIS);
    proxy = SpecificRequestor.getClient(Mail.class, transceiver);
  }

  @AfterClass
  public static void tearDownConnections() throws Exception {
    transceiver.close();
    server.close();
  }

  @Test
  public void testRequestResponse() throws Exception {
    for (int x = 0; x < 5; x++) {
      verifyResponse(proxy.send(createMessage()));
    }
  }

  private void verifyResponse(String result) {
    Assert.assertEquals(
        "Sent message to [wife] from [husband] with body [I love you!]",
        result.toString());
  }

  @Test
  public void testOneway() throws Exception {
    for (int x = 0; x < 5; x++) {
      proxy.fireandforget(createMessage());
    }
    mailService.awaitMessages();
    mailService.assertAllMessagesReceived();
  }

  @Test
  public void testMixtureOfRequests() throws Exception {
    mailService.reset();
    for (int x = 0; x < 5; x++) {
      Message createMessage = createMessage();
      proxy.fireandforget(createMessage);
      verifyResponse(proxy.send(createMessage));
    }
    mailService.awaitMessages();
    mailService.assertAllMessagesReceived();

  }

  private Message createMessage() {
    Message msg = Message.newBuilder().
        setTo("wife").
        setFrom("husband").
        setBody("I love you!").
        build();
    return msg;
  }

  /**
   * Factory of SSL-enabled client channels
   */
  private static class SSLChannelFactory extends NioClientSocketChannelFactory {
    public SSLChannelFactory() {
      super(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
    }

    @Override
    public SocketChannel newChannel(ChannelPipeline pipeline) {
      try {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, new TrustManager[]{new BogusTrustManager()},
                        null);
        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(true);
        pipeline.addFirst("ssl", new SslHandler(sslEngine));
        return super.newChannel(pipeline);
      } catch (Exception ex) {
        throw new RuntimeException("Cannot create SSL channel", ex);
      }
    }
  }

  /**
   * Bogus trust manager accepting any certificate
   */
  private static class BogusTrustManager implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] certs, String s) {
      // nothing
    }

    @Override
    public void checkServerTrusted(X509Certificate[] certs, String s) {
      // nothing
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  /**
   * Factory of SSL-enabled server worker channel pipelines
   */
  private static class SSLChannelPipelineFactory
      implements ChannelPipelineFactory {

    private SSLContext createServerSSLContext() {
      try {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(
            TestNettyServer.class.getResource(TEST_CERTIFICATE).openStream(),
            TEST_CERTIFICATE_PASSWORD.toCharArray());

        // Set up key manager factory to use our key store
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(getAlgorithm());
        kmf.init(ks, TEST_CERTIFICATE_PASSWORD.toCharArray());

        SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(kmf.getKeyManagers(), null, null);
        return serverContext;
      } catch (Exception e) {
        throw new Error("Failed to initialize the server-side SSLContext", e);
      }
    }

    private String getAlgorithm() {
      String algorithm = Security.getProperty(
          "ssl.KeyManagerFactory.algorithm");
      if (algorithm == null) {
        algorithm = "SunX509";
      }
      return algorithm;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = Channels.pipeline();
      SSLEngine sslEngine = createServerSSLContext().createSSLEngine();
      sslEngine.setUseClientMode(false);
      pipeline.addLast("ssl", new SslHandler(sslEngine));
      return pipeline;
    }
  }
}
