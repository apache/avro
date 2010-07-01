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
    public Utf8 send(Message message) {
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
    Thread.sleep(1000); // waiting for server startup

    int serverPort = server.getPort();
    System.out.println("server port : " + serverPort);

    // client
    Transceiver transceiver = new NettyTransceiver(new InetSocketAddress(
        serverPort));
    Mail proxy = (Mail) SpecificRequestor.getClient(Mail.class, transceiver);

    Message msg = new Message();
    msg.to = new Utf8("wife");
    msg.from = new Utf8("husband");
    msg.body = new Utf8("I love you!");

    try {
      Utf8 result = proxy.send(msg);
      System.out.println("Result: " + result);
      Assert.assertEquals(
          "Sent message to [wife] from [husband] with body [I love you!]",
          result.toString());
    } finally {
      transceiver.close();
      server.close();
    }
  }

}
