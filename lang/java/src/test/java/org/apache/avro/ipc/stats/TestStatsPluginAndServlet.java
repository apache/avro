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
package org.apache.avro.ipc.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRequestor;
import org.apache.avro.generic.GenericResponder;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.LocalTransceiver;
import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Transceiver;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.log.Log;

public class TestStatsPluginAndServlet {
  Protocol protocol = Protocol.parse("" + "{\"protocol\": \"Minimal\", "
      + "\"messages\": { \"m\": {"
      + "   \"request\": [{\"name\": \"x\", \"type\": \"int\"}], "
      + "   \"response\": \"int\"} } }");
  Message message = protocol.getMessages().get("m");

  private static final long MS = 1000*1000L;

  /** Returns an HTML string. */
  private String generateServletResponse(StatsPlugin statsPlugin)
      throws IOException {
    StatsServlet servlet = new StatsServlet(statsPlugin);
    StringWriter w = new StringWriter();
    servlet.writeStats(w);
    String o = w.toString();
    return o;
  }

  /** Expects 0 and returns 1. */
  static class TestResponder extends GenericResponder {
    public TestResponder(Protocol local) {
      super(local);
    }

    @Override
    public Object respond(Message message, Object request)
        throws AvroRemoteException {
      assertEquals(0, ((GenericRecord) request).get("x"));
      return 1;
    }

  }

  private void makeRequest(Transceiver t) throws IOException {
    GenericRecord params = new GenericData.Record(protocol.getMessages().get(
        "m").getRequest());
    params.put("x", 0);
    GenericRequestor r = new GenericRequestor(protocol, t);
    assertEquals(1, r.request("m", params));
  }

  @Test
  public void testFullServerPath() throws IOException {
    Responder r = new TestResponder(protocol);
    StatsPlugin statsPlugin = new StatsPlugin();
    r.addRPCPlugin(statsPlugin);
    Transceiver t = new LocalTransceiver(r);

    for (int i = 0; i < 10; ++i) {
      makeRequest(t);
    }

    String o = generateServletResponse(statsPlugin);
    assertTrue(o.contains("Number of calls: 10"));
  }

  @Test
  public void testMultipleRPCs() throws IOException {
    FakeTicks t = new FakeTicks();
    StatsPlugin statsPlugin = new StatsPlugin(t, StatsPlugin.DEFAULT_SEGMENTER);
    RPCContext context1 = makeContext();
    RPCContext context2 = makeContext();
    statsPlugin.serverReceiveRequest(context1);
    t.passTime(100*MS); // first takes 100ms
    statsPlugin.serverReceiveRequest(context2);
    String r = generateServletResponse(statsPlugin);
    // Check in progress RPCs
    assertTrue(r.contains("m: 0ms"));
    assertTrue(r.contains("m: 100ms"));
    statsPlugin.serverSendResponse(context1);
    t.passTime(900*MS); // second takes 900ms
    statsPlugin.serverSendResponse(context2);

    r = generateServletResponse(statsPlugin);
    assertTrue(r.contains("Average Duration: 500ms"));
  }

  private RPCContext makeContext() {
    RPCContext context = new RPCContext();
    context.setMessage(message);
    return context;
  }

  /** Sleeps as requested. */
  private static class SleepyResponder extends GenericResponder {
    public SleepyResponder(Protocol local) {
      super(local);
    }

    @Override
    public Object respond(Message message, Object request)
        throws AvroRemoteException {
      try {
        Thread.sleep((Long)((GenericRecord)request).get("millis"));
      } catch (InterruptedException e) {
        throw new AvroRemoteException(e);
      }
      return null;
    }
  }

  /**
   * Demo program for using RPC stats.  Tool can be used (as below)
   * to trigger RPCs.
   * <pre>
   * java -jar build/avro-tools-*.jar rpcsend '{"protocol":"sleepy","namespace":null,"types":[],"messages":{"sleep":{"request":[{"name":"millis","type":"long"}],"response":"null"}}}' sleep localhost 7002 '{"millis": 20000}'
   * </pre>
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      args = new String[] { "7002", "7003" };
    }
    Protocol protocol = Protocol.parse("{\"protocol\": \"sleepy\", "
        + "\"messages\": { \"sleep\": {"
        + "   \"request\": [{\"name\": \"millis\", \"type\": \"long\"}], "
        + "   \"response\": \"null\"} } }");
    Log.info("Using protocol: " + protocol.toString());
    Responder r = new SleepyResponder(protocol);
    StatsPlugin p = new StatsPlugin();
    r.addRPCPlugin(p);

    // Start Avro server
    HttpServer avroServer = new HttpServer(r, Integer.parseInt(args[0]));
    avroServer.start();

    // Ideally we could use the same Jetty server
    Server httpServer = new Server(Integer.parseInt(args[1]));
    new Context(httpServer, "/").addServlet(
        new ServletHolder(new StatsServlet(p)), "/*");
    httpServer.start();
    httpServer.join();
  }
}
