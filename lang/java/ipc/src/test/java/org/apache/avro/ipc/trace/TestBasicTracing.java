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
package org.apache.avro.ipc.trace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.ipc.generic.GenericResponder;
import org.apache.avro.ipc.trace.TracePlugin.StorageType;
import org.junit.Test;

public class TestBasicTracing {
  Protocol protocol = Protocol.parse("" + "{\"protocol\": \"Minimal\", "
      + "\"messages\": { \"m\": {"
      + "   \"request\": [{\"name\": \"x\", \"type\": \"int\"}], "
      + "   \"response\": \"int\"} } }");
  Message message = protocol.getMessages().get("m");

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

  @Test
  public void testBasicTrace() throws Exception {
    TracePluginConfiguration conf = new TracePluginConfiguration();
    conf.storageType = StorageType.MEMORY;
    conf.port = 51007;
    conf.clientPort = 12344;
    conf.traceProb = 1.0;
    TracePlugin responderPlugin = new TracePlugin(conf);
    conf.port = 51008;
    conf.clientPort = 12345;
    TracePlugin requestorPlugin = new TracePlugin(conf);
    
    Responder res = new TestResponder(protocol);
    res.addRPCPlugin(responderPlugin);
    
    HttpServer server = new HttpServer(res, 50000);
    server.start();
    
    HttpTransceiver trans = new HttpTransceiver(
        new URL("http://localhost:50000"));
    
    GenericRequestor r = new GenericRequestor(protocol, trans);
    r.addRPCPlugin(requestorPlugin);
    
    GenericRecord params = new GenericData.Record(protocol.getMessages().get(
    "m").getRequest());
    params.put("x", 0);
    r.request("m", params);
    
    List<Span> responderSpans = responderPlugin.storage.getAllSpans();
    assertEquals(1, responderSpans.size());
    
    List<Span> requestorSpans = requestorPlugin.storage.getAllSpans();
    assertEquals(1, requestorSpans.size());
    
    if ((responderSpans.size() == 1 && requestorSpans.size() == 1)) {
      Span responderSpan = responderSpans.get(0);
      Span requestorSpan = requestorSpans.get(0);
      
      // Check meta propagation     
      assertEquals(null, requestorSpan.parentSpanID);
      assertEquals(responderSpan.parentSpanID, requestorSpan.parentSpanID);
      assertEquals(responderSpan.traceID, requestorSpan.traceID);
      
      // Check other data
      assertEquals(2, requestorSpan.events.size());
      assertEquals(2, responderSpan.events.size());
      assertTrue("m".equals(requestorSpan.messageName.toString()));
      assertTrue("m".equals(responderSpan.messageName.toString()));
      assertFalse(requestorSpan.complete);
      assertFalse(responderSpan.complete);
    }
    
    server.close();
    
    requestorPlugin.clientFacingServer.stop();
    requestorPlugin.httpServer.close();
    
    responderPlugin.clientFacingServer.stop();
    responderPlugin.httpServer.close();
  }
  
  /*
   * Test a more complicated, recursive trace involving four agents and three
   * spans.
   * 
   * Messages are x, y, z which request/return 
   * incrementing int values (shown below).
   * 
   *   |-w-(1)-> |         |
   *   |         |-x-(2)-> | C
   *   |         | <-x-(3)-|        
   *   |         |    
   * A |       B |    
   *   |         |         |
   *   |         |-x-(4)-> | 
   *   |         | <-x-(5)-| D   
   *   |         |         | 
   *   |<-w-(6)- |         |
   *   
   *   Listening ports are B: 21005
   *                       C: 21006  
   *                       D: 21007
   */
  
  static Protocol advancedProtocol = Protocol.parse("{\"protocol\": \"Advanced\", "
      + "\"messages\": { " 
      +  "\"w\": { \"request\": [{\"name\": \"req\", \"type\": \"int\"}], "
      + "   \"response\": \"int\"},"
      + "\"x\": { \"request\": [{\"name\": \"req\", \"type\": \"int\"}], "
      + "   \"response\": \"int\"},"
      + "\"y\": { \"request\": [{\"name\": \"req\", \"type\": \"int\"}], "
      + "   \"response\": \"int\"}"
      + " } }");

  /** Middle Responder */
  static class RecursingResponder extends GenericResponder {
    HttpTransceiver transC;
    HttpTransceiver transD;
    GenericRequestor reqC;
    GenericRequestor reqD;
    Protocol protocol;
    
    public RecursingResponder(Protocol local, RPCPlugin plugin) 
    throws Exception {
      super(local);
      transC = new HttpTransceiver(
          new URL("http://localhost:21006"));
      transD = new HttpTransceiver(
          new URL("http://localhost:21007"));
      reqC = new GenericRequestor(local, transC);
      reqC.addRPCPlugin(plugin);
      reqD = new GenericRequestor(local, transD);
      reqD.addRPCPlugin(plugin);
      
      protocol = local; 
    }

    @Override
    public Object respond(Message message, Object request)
        throws IOException {
       assertTrue("w".equals(message.getName()));
       GenericRecord inParams = (GenericRecord)request;
       Integer currentCount = (Integer) inParams.get("req");
       assertTrue(currentCount.equals(1));
       
       GenericRecord paramsC = new GenericData.Record(
           protocol.getMessages().get("x").getRequest());
       paramsC.put("req", currentCount + 1);
       Integer returnC = (Integer) reqC.request("x", paramsC);
       assertTrue(returnC.equals(currentCount + 2));
       
       GenericRecord paramsD = new GenericData.Record(
           protocol.getMessages().get("x").getRequest());
       paramsD.put("req", currentCount + 3);
       Integer returnD = (Integer) reqD.request("x", paramsD);
       assertTrue(returnD.equals(currentCount + 4));
       
       return currentCount + 5;
    }
  }
  
  /** Alternative Middle Responder */
  static class NonRecursingResponder extends GenericResponder {
    public NonRecursingResponder(Protocol local) 
    throws Exception {
      super(local);
    }

    @Override
    public Object respond(Message message, Object request)
        throws IOException {
       assertTrue("w".equals(message.getName()));
      return 6;
    }
  }
  
  /** Endpoint responder */
  static class EndpointResponder extends GenericResponder {
    public EndpointResponder(Protocol local) {
      super(local);
    }

    @Override
    public Object respond(Message message, Object request)
        throws AvroRemoteException {
      GenericRecord inParams = (GenericRecord)request;
      Integer currentCount = (Integer) inParams.get("req");
      
      return currentCount + 1;
    }
  }

  @Test
  public void testRecursingTrace() throws Exception {
    TracePluginConfiguration conf = new TracePluginConfiguration();
    conf.storageType = StorageType.MEMORY;
    conf.traceProb = 1.0;
    conf.port = 51010;
    conf.clientPort = 12346;
    TracePlugin aPlugin = new TracePlugin(conf);
    conf.port = 51011;
    conf.clientPort = 12347;
    TracePlugin bPlugin = new TracePlugin(conf);
    conf.port = 51012;
    conf.clientPort = 12348;
    TracePlugin cPlugin = new TracePlugin(conf);
    conf.port = 51013;
    conf.clientPort = 12349;
    TracePlugin dPlugin = new TracePlugin(conf);
    
    // Responders
    Responder bRes = new RecursingResponder(advancedProtocol, bPlugin);
    bRes.addRPCPlugin(bPlugin);
    HttpServer server1 = new HttpServer(bRes, 21005);
    server1.start();

    Responder cRes = new EndpointResponder(advancedProtocol);
    cRes.addRPCPlugin(cPlugin);
    HttpServer server2 = new HttpServer(cRes, 21006);
    server2.start();
    
    Responder dRes = new EndpointResponder(advancedProtocol);
    dRes.addRPCPlugin(dPlugin);
    HttpServer server3 = new HttpServer(dRes, 21007);
    server3.start();
    
    // Root requestor
    HttpTransceiver trans = new HttpTransceiver(
        new URL("http://localhost:21005"));
    
    GenericRequestor r = new GenericRequestor(advancedProtocol, trans);
    r.addRPCPlugin(aPlugin);
    
    GenericRecord params = new GenericData.Record(
        advancedProtocol.getMessages().get("w").getRequest());
    params.put("req", 1);
    r.request("w", params);
    
    // Verify counts
    assertEquals(1, aPlugin.storage.getAllSpans().size());
    assertEquals(3, bPlugin.storage.getAllSpans().size());
    assertEquals(1, cPlugin.storage.getAllSpans().size());
    assertEquals(1, dPlugin.storage.getAllSpans().size());
    
    ID traceID = aPlugin.storage.getAllSpans().get(0).traceID;
    ID rootSpanID = null;
    
    // Verify event counts and trace ID propagation
    for (Span s: aPlugin.storage.getAllSpans()) {
      assertEquals(2, s.events.size());
      assertTrue(Util.idsEqual(traceID, s.traceID));
      assertFalse(s.complete);
      rootSpanID = s.spanID;
    }
    
    for (Span s: bPlugin.storage.getAllSpans()) {
      assertEquals(2, s.events.size());
      assertEquals(traceID, s.traceID);
      assertFalse(s.complete);
    }
    
    for (Span s: cPlugin.storage.getAllSpans()) {
      assertEquals(2, s.events.size());
      assertEquals(traceID, s.traceID);
      assertFalse(s.complete);
    }
    for (Span s: dPlugin.storage.getAllSpans()) {
      assertEquals(2, s.events.size());
      assertEquals(traceID, s.traceID);
      assertFalse(s.complete);
    }
    
    // Verify span propagation.
    ID firstSpanID = aPlugin.storage.getAllSpans().get(0).spanID;
    ID secondSpanID = cPlugin.storage.getAllSpans().get(0).spanID;
    ID thirdSpanID = dPlugin.storage.getAllSpans().get(0).spanID;
    
    boolean firstFound = false, secondFound = false, thirdFound = false;
    for (Span s: bPlugin.storage.getAllSpans()) {
      if (Util.idsEqual(s.spanID, firstSpanID)) {
        firstFound = true;
      }
      else if (Util.idsEqual(s.spanID, secondSpanID)) {
        secondFound = true;
      }
      else if (Util.idsEqual(s.spanID, thirdSpanID)) {
        thirdFound = true;
      }
    }
    assertTrue(firstFound);
    assertTrue(secondFound);
    assertTrue(thirdFound);
    
    server1.close();
    server2.close();
    server3.close();
    aPlugin.httpServer.close();
    aPlugin.clientFacingServer.stop();
    bPlugin.httpServer.close();
    bPlugin.clientFacingServer.stop();
    cPlugin.httpServer.close();
    cPlugin.clientFacingServer.stop();
    dPlugin.httpServer.close();
    dPlugin.clientFacingServer.stop();
    
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
   * Demo program for using RPC trace. This automatically generates
   * client RPC requests. 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    TracePluginConfiguration conf = new TracePluginConfiguration();
    conf.storageType = StorageType.MEMORY;
    conf.traceProb = 1.0;
    conf.port = 51010;
    conf.clientPort = 12346;
    TracePlugin aPlugin = new TracePlugin(conf);
    conf.port = 51011;
    conf.clientPort = 12347;
    TracePlugin bPlugin = new TracePlugin(conf);
    conf.port = 51012;
    conf.clientPort = 12348;
    TracePlugin cPlugin = new TracePlugin(conf);
    conf.port = 51013;
    conf.clientPort = 12349;
    TracePlugin dPlugin = new TracePlugin(conf);
    conf.port = 51014;
    conf.clientPort = 12350;
    TracePlugin ePlugin = new TracePlugin(conf);
    conf.port = 51015;
    conf.clientPort = 12351;
    TracePlugin fPlugin = new TracePlugin(conf);
    
    // Responders
    Responder bRes = new RecursingResponder(advancedProtocol, bPlugin);
    bRes.addRPCPlugin(bPlugin);
    HttpServer server1 = new HttpServer(bRes, 21005);
    server1.start();

    Responder cRes = new EndpointResponder(advancedProtocol);
    cRes.addRPCPlugin(cPlugin);
    HttpServer server2 = new HttpServer(cRes, 21006);
    server2.start();
    
    Responder dRes = new EndpointResponder(advancedProtocol);
    dRes.addRPCPlugin(dPlugin);
    HttpServer server3 = new HttpServer(dRes, 21007);
    server3.start();
    
    
    // Root requestors
    HttpTransceiver trans1 = new HttpTransceiver(
        new URL("http://localhost:21005")); // recurse
    HttpTransceiver trans2 = new HttpTransceiver(
        new URL("http://localhost:21007")); // no recurse
    
    
    GenericRequestor r1 = new GenericRequestor(advancedProtocol, trans1);
    r1.addRPCPlugin(aPlugin);
    
    GenericRequestor r2 = new GenericRequestor(advancedProtocol, trans2);
    r2.addRPCPlugin(fPlugin);
    
    GenericRecord params = new GenericData.Record(
        advancedProtocol.getMessages().get("w").getRequest());
    params.put("req", 1);
    
    while (true){
      r1.request("w", params);
      r2.request("x", params);
      Thread.sleep(100);
    }
  }
}
