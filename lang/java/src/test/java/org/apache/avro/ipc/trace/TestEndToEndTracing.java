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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRequestor;
import org.apache.avro.generic.GenericResponder;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.trace.SpanAggregator.SpanAggregationResults;
import org.apache.avro.ipc.trace.SpanAggregator.TraceFormationResults;
import org.apache.avro.ipc.trace.TracePlugin.StorageType;
import org.junit.Test;

/**
 * Tests which test logging, aggregation, and analysis of traces.
 */
public class TestEndToEndTracing {
  

  /*
   * Messages are w, x, y which request/return 
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
  
  Protocol advancedProtocol = Protocol.parse("{\"protocol\": \"Advanced\", "
      + "\"messages\": { " 
      + "\"w\": { \"request\": [{\"name\": \"req\", \"type\": \"int\"}], "
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
  public void testTraceAndCollectionMemory() throws Exception {
    TracePluginConfiguration conf = new TracePluginConfiguration();
    conf.storageType = StorageType.MEMORY;
    testTraceAndCollection(conf);
  }  
  
  @Test
  public void testTraceAndCollectionDisk() throws Exception {
    TracePluginConfiguration conf = new TracePluginConfiguration();
    conf.storageType = StorageType.DISK;
    conf.buffer = false;
    testTraceAndCollection(conf);
  }
  
  public void testTraceAndCollection(TracePluginConfiguration conf) throws Exception {
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
    Thread.sleep(1000);
    ArrayList<Span> allSpans = new ArrayList<Span>();
    
    allSpans.addAll(aPlugin.storage.getAllSpans());
    allSpans.addAll(bPlugin.storage.getAllSpans());
    allSpans.addAll(cPlugin.storage.getAllSpans());
    allSpans.addAll(dPlugin.storage.getAllSpans());
    
    SpanAggregationResults results = SpanAggregator.getFullSpans(allSpans);
    
    TraceFormationResults traces = SpanAggregator.getTraces(results.completeSpans);
    
    assertEquals(1, traces.traces.size());
    assertEquals(0, traces.rejectedSpans.size());
    
    // Test debug printing of traces
    String string1 = traces.traces.get(0).printWithTiming();
    assertTrue(string1.contains("w"));
    assertTrue(string1.contains("x"));
    assertTrue(string1.indexOf("x") != string1.lastIndexOf("x")); // assure two x's
    
    String string2 = traces.traces.get(0).printBrief();
    assertTrue(string2.contains("w"));
    assertTrue(string2.contains("x"));
    assertTrue(string2.indexOf("x") != string2.lastIndexOf("x")); // assure two x's
    
    // Just for fun, print to console
    System.out.println(traces.traces.get(0).printWithTiming());
    System.out.println(traces.traces.get(0).printBrief());
    
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
}
