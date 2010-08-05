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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.apache.avro.ipc.trace.Util.idValue;
import static org.apache.avro.ipc.trace.Util.idsEqual;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.trace.SpanAggregator.SpanAggregationResults;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import edu.emory.mathcs.backport.java.util.Arrays;

/**
 * Tests for span aggregation utility methods.
 */
public class TestSpanAggregation {
  /**
   * Test merging of two basic spans.
   */
  @Test
  public void testSpanCompletion1() {
    Span span1a = createClientSpan(idValue(1), idValue(1), null, new Utf8("a"));
    Span span1b = createServerSpan(idValue(1), idValue(1), null, new Utf8("a"));
    
    List<Span> partials = new ArrayList<Span>();
    partials.add(span1a);
    partials.add(span1b);
    SpanAggregationResults results = SpanAggregator.getFullSpans(partials);
    
    assertNotNull(results.completeSpans);
    assertNotNull(results.incompleteSpans);
    assertTrue(results.incompleteSpans.size() == 0); 
    assertTrue(results.completeSpans.size() == 1);
    
    Span result = results.completeSpans.get(0);
    assertEquals(null, result.parentSpanID);
    assertTrue(idsEqual(idValue(1), result.spanID));
    assertEquals(4, result.events.size());
  }
  
  /**
   * Test span merging with some extra invalid spans;
   */
  @Test
  public void testInvalidSpanCompletion() {
    // Trace: 1, Span: 1, Parent: null 
    Span span1a = createClientSpan(idValue(1), idValue(1), null, new Utf8("a"));
    Span span1b = createServerSpan(idValue(1), idValue(1), null, new Utf8("a"));
    
    // Trace: 1, Span: 10, Parent: 3 
    Span spanBogus1 = createClientSpan(idValue(1), idValue(10), idValue(3), new Utf8("not"));
    Span spanBogus2 = createServerSpan(idValue(1), idValue(10), idValue(3), new Utf8("equal"));
    
    // Trace: 1, Span: 5, Parent: (2/3) 
    Span spanBogus3 = createClientSpan(idValue(1), idValue(5), idValue(2), new Utf8("equal"));
    Span spanBogus4 = createServerSpan(idValue(1), idValue(5), idValue(3), new Utf8("equal"));
    
    // Trace:1, Span: 4, Parent: 1
    Span spanBogus5 = createClientSpan(idValue(1), idValue(4), idValue(1), new Utf8("alone"));
    
    List<Span> partials = new ArrayList<Span>();
    partials.add(span1a);
    partials.add(span1b);
    partials.add(spanBogus1);
    partials.add(spanBogus2);
    partials.add(spanBogus3);
    partials.add(spanBogus4);
    partials.add(spanBogus5);
    
    SpanAggregationResults results = SpanAggregator.getFullSpans(partials);
    assertNotNull(results.completeSpans);
    assertNotNull(results.incompleteSpans);

    assertTrue(results.incompleteSpans.size() == 5);
    assertTrue(results.incompleteSpans.contains(spanBogus1));
    assertTrue(results.incompleteSpans.contains(spanBogus2));
    assertTrue(results.incompleteSpans.contains(spanBogus3));
    assertTrue(results.incompleteSpans.contains(spanBogus4));
    assertTrue(results.incompleteSpans.contains(spanBogus5));
    
    assertTrue(results.completeSpans.size() == 1);
    Span result = results.completeSpans.get(0);
    assertTrue(result.complete);
    assertTrue(idsEqual(idValue(1), result.spanID));
    assertEquals(new Utf8("requestorHostname"), result.requestorHostname);
    assertEquals(new Utf8("responderHostname"), result.responderHostname);
    assertNull(result.parentSpanID);
    assertEquals(new Utf8("a"), result.messageName);
  }
  
  /**
   * Test basic formation of a trace.
   *      a                      
   *      b                    
   *    c   d
   *        e                    
   */
  @Test
  public void testTraceFormation1() {
    Span a1 = createClientSpan(idValue(1), idValue(1), null, new Utf8("a"));
    Span a2 = createServerSpan(idValue(1), idValue(1), null, new Utf8("a"));
    
    Span b1 = createClientSpan(idValue(1), idValue(2), idValue(1), new Utf8("b"));
    Span b2 = createServerSpan(idValue(1), idValue(2), idValue(1), new Utf8("b"));

    Span c1 = createClientSpan(idValue(1), idValue(3), idValue(2), new Utf8("c"));
    Span c2 = createServerSpan(idValue(1), idValue(3), idValue(2), new Utf8("c"));
    
    Span d1 = createClientSpan(idValue(1), idValue(4), idValue(2), new Utf8("d"));
    Span d2 = createServerSpan(idValue(1), idValue(4), idValue(2), new Utf8("d"));
    
    Span e1 = createClientSpan(idValue(1), idValue(5), idValue(4), new Utf8("e"));
    Span e2 = createServerSpan(idValue(1), idValue(5), idValue(4), new Utf8("e"));
    
    List<Span> spans = new LinkedList<Span>();
    spans.addAll(Arrays.asList(new Span[] {a1, a2, b1, b2, c1, c2, d1, d2, e1, e2}));
    
    List<Span> merged = SpanAggregator.getFullSpans(spans).completeSpans;
    
    assertEquals(5, merged.size());
    for (Span s: merged) {
      assertEquals(new Utf8("requestorHostname"), s.requestorHostname);
      assertEquals(new Utf8("responderHostname"), s.responderHostname);
    }
    
    List<Trace> traces = SpanAggregator.getTraces(merged).traces;
    assertEquals(1, traces.size());
    
    assertEquals("Trace: (a (b (c) (d (e))))", traces.get(0).printBrief());
    
  }
  
  /**
   * Make a mock Span including client-side timing data.
   */
  public Span createClientSpan(ID traceID, ID spanID, ID parentID, Utf8 msgName) {
    Span out = new Span();
    out.spanID = spanID;
    out.traceID = traceID;
    out.requestorHostname = new Utf8("requestorHostname");
    
    if (parentID != null) {
      out.parentSpanID = parentID;
    }
    out.messageName = msgName;
    out.complete = false;
    
    TimestampedEvent event1 = new TimestampedEvent();
    event1.event = SpanEvent.CLIENT_SEND;
    event1.timeStamp = System.currentTimeMillis() * 1000000;
    
    TimestampedEvent event2 = new TimestampedEvent();
    event2.event = SpanEvent.CLIENT_RECV;
    event2.timeStamp = System.currentTimeMillis() * 1000000;
    
    out.events = new GenericData.Array(
        2, Schema.createArray(TimestampedEvent.SCHEMA$));
    out.events.add(event1);
    out.events.add(event2);
    
    return out;
  }
  
  /**
   * Make a mock Span including server-side timing data.
   */
  public Span createServerSpan(ID traceID, ID spanID, ID parentID, Utf8 msgName) {
    Span out = new Span();
    out.spanID = spanID;
    out.traceID = traceID;
    out.responderHostname = new Utf8("responderHostname");
    
    if (parentID != null) {
      out.parentSpanID = parentID;
    }
    out.messageName = msgName;
    out.complete = false;
    
    TimestampedEvent event1 = new TimestampedEvent();
    event1.event = SpanEvent.SERVER_RECV;
    event1.timeStamp = System.currentTimeMillis();
    
    TimestampedEvent event2 = new TimestampedEvent();
    event2.event = SpanEvent.SERVER_SEND;
    event2.timeStamp = System.currentTimeMillis();
    
    out.events = new GenericData.Array(
        2, Schema.createArray(TimestampedEvent.SCHEMA$));
    out.events.add(event1);
    out.events.add(event2);
    
    return out;
  }
}
