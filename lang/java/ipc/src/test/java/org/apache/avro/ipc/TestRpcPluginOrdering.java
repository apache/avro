package org.apache.avro.ipc;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.LocalTransceiver;
import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.avro.specific.SpecificResponder;
import org.apache.avro.test.Mail;
import org.apache.avro.test.Message;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class TestRpcPluginOrdering {

  private static AtomicInteger orderCounter = new AtomicInteger();
  
  public class OrderPlugin extends RPCPlugin{

    public void clientStartConnect(RPCContext context) {
      assertEquals(0, orderCounter.getAndIncrement());
    }
    
    public void clientSendRequest(RPCContext context) {
      assertEquals(1, orderCounter.getAndIncrement());
    }
    
    public void clientReceiveResponse(RPCContext context) {
      assertEquals(6, orderCounter.getAndIncrement());
    }

    public void clientFinishConnect(RPCContext context) {
      assertEquals(5, orderCounter.getAndIncrement());
    }

    public void serverConnecting(RPCContext context) {
      assertEquals(2, orderCounter.getAndIncrement());
    }

    public void serverReceiveRequest(RPCContext context) {
      assertEquals(3, orderCounter.getAndIncrement());
    }

    public void serverSendResponse(RPCContext context) {
      assertEquals(4, orderCounter.getAndIncrement());
    }
  }
  
  @Test
  public void testRpcPluginOrdering() throws Exception {
    OrderPlugin plugin = new OrderPlugin();
    
    SpecificResponder responder = new SpecificResponder(Mail.class, new TestMailImpl());
    SpecificRequestor requestor = new SpecificRequestor(Mail.class, new LocalTransceiver(responder));
    responder.addRPCPlugin(plugin);
    requestor.addRPCPlugin(plugin);
    
    Mail client = SpecificRequestor.getClient(Mail.class, requestor);
    Message message = createTestMessage();
    client.send(message);
  }

  private Message createTestMessage() {
    Message message = new Message();
    message.to = new Utf8("me@test.com");
    message.from = new Utf8("you@test.com");
    message.body = new Utf8("plugin testing");
    return message;
  }
  
  private static class TestMailImpl implements Mail{
    public CharSequence send(Message message) throws AvroRemoteException {
      return new Utf8("Received");
    }
  }
}
