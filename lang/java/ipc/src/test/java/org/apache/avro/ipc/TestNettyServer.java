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

import junit.framework.Assert;

import org.apache.avro.specific.SpecificRequestor;
import org.apache.avro.specific.SpecificResponder;
import org.apache.avro.test.Mail;
import org.apache.avro.test.Message;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class TestNettyServer {

  public static class MailImpl implements Mail {
    // in this simple example just return details of the message
    public CharSequence send(Message message) {
      return new Utf8("Sent message to [" + message.to.toString() + "] from ["
          + message.from.toString() + "] with body [" + message.body.toString()
          + "]");
    }
  }

  @Test
  public void test() throws Exception {
    // start server
    System.out.println("starting server...");
    Responder responder = new SpecificResponder(Mail.class, new MailImpl());
    Server server = new NettyServer(responder, new InetSocketAddress(0));
    server.start();

    int serverPort = server.getPort();
    System.out.println("server port : " + serverPort);

    // client
    Transceiver transceiver = new NettyTransceiver(new InetSocketAddress(
        serverPort));
    Mail proxy = SpecificRequestor.getClient(Mail.class, transceiver);

    Message msg = new Message();
    msg.to = new Utf8("wife");
    msg.from = new Utf8("husband");
    msg.body = new Utf8("I love you!");

    try {
      for(int x = 0; x < 5; x++) {
        CharSequence result = proxy.send(msg);
        System.out.println("Result: " + result);
        Assert.assertEquals(
            "Sent message to [wife] from [husband] with body [I love you!]",
            result.toString());
      }
    } finally {
      transceiver.close();
      server.close();
    }
  }

}
