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
package org.apache.avro.tool;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericResponder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpServer;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
 * Receives one RPC call and responds.  (The moral equivalent
 * of "netcat".)
 */
public class RpcReceiveTool implements Tool {
  private PrintStream out;
  private Object response;
  /** Used to communicate between server thread (responder) and run() */
  private CountDownLatch latch;
  private Message expectedMessage;
  HttpServer server;

  @Override
  public String getName() {
    return "rpcreceive";
  }

  @Override
  public String getShortDescription() {
    return "Opens an HTTP RPC Server and listens for one message.";
  }
  
  private class SinkResponder extends GenericResponder {

    public SinkResponder(Protocol local) {
      super(local);
    }

    @Override
    public Object respond(Message message, Object request)
    throws AvroRemoteException {
      if (!message.equals(expectedMessage)) {
        out.println(String.format("Expected message '%s' but received '%s'.", 
            expectedMessage.getName(), message.getName()));
        latch.countDown();
        throw new IllegalArgumentException("Unexpected message.");
      }
      out.print(message.getName());
      out.print("\t");
      try {
        JsonGenerator jsonGenerator = new JsonFactory().createJsonGenerator(
            out, JsonEncoding.UTF8);
        JsonEncoder jsonEncoder = new JsonEncoder(message.getRequest(), jsonGenerator);

        GenericDatumWriter<Object> writer = new GenericDatumWriter<Object>(
            message.getRequest());
        writer.write(request, jsonEncoder);
        jsonGenerator.flush();
        jsonEncoder.flush();
        out.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      out.println();
      latch.countDown();
      return response;
    }
  }
  
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    // Split up into two functions for easier testing.
    int r = run1(in, out, err, args);
    if (r != 0) {
      return r;
    }
    return run2(err);
  }

  int run1(InputStream in, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    if (args.size() != 4) {
      err.println("Expected four arguments: protocol port message_name json_response");
      return 1;
    }
    Protocol protocol = Protocol.parse(args.get(0));
    int port = Integer.parseInt(args.get(1));
    String messageName = args.get(2);
    expectedMessage = protocol.getMessages().get(messageName);
    if (expectedMessage == null) {
      err.println(String.format("No message named '%s' found in protocol '%s'.",
          messageName, protocol));
      return 1;
    }
    String jsonData = args.get(3);
    this.out = out;
    
    this.response = Util.jsonToGenericDatum(expectedMessage.getResponse(), jsonData);
    
    latch = new CountDownLatch(1);
    server = new HttpServer(new SinkResponder(protocol), port);
    err.println("Listening on port " + server.getPort());
    return 0;
  }
  
  int run2(PrintStream err) throws InterruptedException {
    latch.await();
    err.println("Closing server.");
    server.close();
    return 0;
  }

}
