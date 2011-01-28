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

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.ipc.trace.SpanAggregator.SpanAggregationResults;
import org.apache.avro.ipc.trace.TestBasicTracing.EndpointResponder;
import org.apache.avro.ipc.trace.TestBasicTracing.RecursingResponder;
import org.junit.Test;

public class TestTraceCollection {
  @Test
  public void testRecursingTrace() throws Exception {
    TracePluginConfiguration conf = new TracePluginConfiguration();
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
    Responder bRes = new RecursingResponder(TestBasicTracing.advancedProtocol, bPlugin);
    bRes.addRPCPlugin(bPlugin);
    HttpServer server1 = new HttpServer(bRes, 21005);
    server1.start();

    Responder cRes = new EndpointResponder(TestBasicTracing.advancedProtocol);
    cRes.addRPCPlugin(cPlugin);
    HttpServer server2 = new HttpServer(cRes, 21006);
    server2.start();
    
    Responder dRes = new EndpointResponder(TestBasicTracing.advancedProtocol);
    dRes.addRPCPlugin(dPlugin);
    HttpServer server3 = new HttpServer(dRes, 21007);
    server3.start();
    
    // Root requestor
    HttpTransceiver trans = new HttpTransceiver(
        new URL("http://localhost:21005"));
    
    GenericRequestor r = new GenericRequestor(TestBasicTracing.advancedProtocol, trans);
    r.addRPCPlugin(aPlugin);
    
    GenericRecord params = new GenericData.Record(
        TestBasicTracing.advancedProtocol.getMessages().get("w").getRequest());
    params.put("req", 1);
    
    for (int i = 0; i < 40; i++) {
      r.request("w", params);  
    }
    
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

    List<Span> allSpans = new ArrayList<Span>();
    allSpans.addAll(aPlugin.storage.getAllSpans());
    allSpans.addAll(bPlugin.storage.getAllSpans()); 
    allSpans.addAll(cPlugin.storage.getAllSpans());
    allSpans.addAll(dPlugin.storage.getAllSpans());

    SpanAggregationResults results = SpanAggregator.getFullSpans(allSpans);
    
    assertEquals(0, results.incompleteSpans.size());
    List<Span> merged = results.completeSpans;
    List<Trace> traces = SpanAggregator.getTraces(merged).traces;
    
    assertEquals(40, traces.size());
    TraceCollection collection = new TraceCollection(traces.get(0));
    for (Trace t: traces) {
      collection.addTrace(t);
    }
  }
}
