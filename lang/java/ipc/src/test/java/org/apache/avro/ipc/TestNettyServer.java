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

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.test.Mail;
import org.apache.avro.test.Message;
import org.apache.avro.util.Utf8;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNettyServer {

  private static Server server;
  private static Transceiver transceiver;
  private static Mail proxy;
  private static MailImpl mailService;

  public static class MailImpl implements Mail {

    private CountDownLatch allMessages = new CountDownLatch(5);
    
    // in this simple example just return details of the message
    public CharSequence send(Message message) {
      return new Utf8("Sent message to [" + message.getTo().toString() + 
          "] from [" + message.getFrom().toString() + "] with body [" + 
          message.getBody().toString() + "]");
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
  public static void initializeConnections()throws Exception {
    // start server
    System.out.println("starting server...");
    mailService = new MailImpl();
    Responder responder = new SpecificResponder(Mail.class, mailService);
    server = new NettyServer(responder, new InetSocketAddress(0));
    server.start();
  
    int serverPort = server.getPort();
    System.out.println("server port : " + serverPort);

    transceiver = new NettyTransceiver(new InetSocketAddress(
        serverPort));
    proxy = SpecificRequestor.getClient(Mail.class, transceiver);
  }
  
  @AfterClass
  public static void tearDownConnections() throws Exception{
    transceiver.close();
    server.close();
  }

  @Test
  public void testRequestResponse() throws Exception {
      for(int x = 0; x < 5; x++) {
        verifyResponse(proxy.send(createMessage()));
      }
  }

  private void verifyResponse(CharSequence result) {
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
      setTo(new Utf8("wife")).
      setFrom(new Utf8("husband")).
      setBody(new Utf8("I love you!")).
      build();
    return msg;
  }

}
