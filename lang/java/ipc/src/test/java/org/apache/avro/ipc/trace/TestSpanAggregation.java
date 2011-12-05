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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.trace.SpanAggregator.SpanAggregationResults;
import org.apache.avro.util.Utf8;
import org.junit.Test;

/**
 * Tests for span aggregation utility methods.
 */
public class TestSpanAggregation {
  /**
   * Test merging of two basic spans.
   */
  @Test
  public void testSpanCompletion1() {
    Span span1a = createClientSpan(idValue(1), idValue(1), null, new String("a"));
    span1a.setRequestPayloadSize(10L);
    span1a.setResponsePayloadSize(0L);
    
    Span span1b = createServerSpan(idValue(1), idValue(1), null, new String("a"));
    span1b.setRequestPayloadSize(0L);
    span1b.setResponsePayloadSize(11L);
    
    List<Span> partials = new ArrayList<Span>();
    partials.add(span1a);
    partials.add(span1b);
    SpanAggregationResults results = SpanAggregator.getFullSpans(partials);
    
    assertNotNull(results.completeSpans);
    assertNotNull(results.incompleteSpans);
    assertTrue(results.incompleteSpans.size() == 0); 
    assertTrue(results.completeSpans.size() == 1);
    
    Span result = results.completeSpans.get(0);
    assertEquals(null, result.getParentSpanID());
    assertTrue(idsEqual(idValue(1), result.getSpanID()));
    assertEquals(4, result.getEvents().size());
    assertEquals(new Long(10), result.getRequestPayloadSize());
    assertEquals(new Long(11), result.getResponsePayloadSize());
  }
  
  /**
   * Test span merging with some extra invalid spans;
   */
  @Test
  public void testInvalidSpanCompletion() {
    // Trace: 1, Span: 1, Parent: null 
    Span span1a = createClientSpan(idValue(1), idValue(1), null, new String("a"));
    Span span1b = createServerSpan(idValue(1), idValue(1), null, new String("a"));
    
    // Trace: 1, Span: 10, Parent: 3 
    Span spanBogus1 = createClientSpan(idValue(1), idValue(10), idValue(3), new String("not"));
    Span spanBogus2 = createServerSpan(idValue(1), idValue(10), idValue(3), new String("equal"));
    
    // Trace: 1, Span: 5, Parent: (2/3) 
    Span spanBogus3 = createClientSpan(idValue(1), idValue(5), idValue(2), new String("equal"));
    Span spanBogus4 = createServerSpan(idValue(1), idValue(5), idValue(3), new String("equal"));
    
    // Trace:1, Span: 4, Parent: 1
    Span spanBogus5 = createClientSpan(idValue(1), idValue(4), idValue(1), new String("alone"));
    
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
    assertTrue(result.getComplete());
    assertTrue(idsEqual(idValue(1), result.getSpanID()));
    assertEquals(new String("requestorHostname"), result.getRequestorHostname());
    assertEquals(new String("responderHostname"), result.getResponderHostname());
    assertNull(result.getParentSpanID());
    assertEquals(new String("a"), result.getMessageName());
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
    Span a1 = createClientSpan(idValue(1), idValue(1), null, new String("a"));
    Span a2 = createServerSpan(idValue(1), idValue(1), null, new String("a"));
    
    Span b1 = createClientSpan(idValue(1), idValue(2), idValue(1), new String("b"));
    Span b2 = createServerSpan(idValue(1), idValue(2), idValue(1), new String("b"));

    Span c1 = createClientSpan(idValue(1), idValue(3), idValue(2), new String("c"));
    Span c2 = createServerSpan(idValue(1), idValue(3), idValue(2), new String("c"));
    
    Span d1 = createClientSpan(idValue(1), idValue(4), idValue(2), new String("d"));
    Span d2 = createServerSpan(idValue(1), idValue(4), idValue(2), new String("d"));
    
    Span e1 = createClientSpan(idValue(1), idValue(5), idValue(4), new String("e"));
    Span e2 = createServerSpan(idValue(1), idValue(5), idValue(4), new String("e"));
    
    List<Span> spans = new LinkedList<Span>();
    spans.addAll(Arrays.asList(new Span[] {a1, a2, b1, b2, c1, c2, d1, d2, e1, e2}));
    
    List<Span> merged = SpanAggregator.getFullSpans(spans).completeSpans;
    
    assertEquals(5, merged.size());
    for (Span s: merged) {
      assertEquals(new String("requestorHostname"), s.getRequestorHostname());
      assertEquals(new String("responderHostname"), s.getResponderHostname());
    }
    
    List<Trace> traces = SpanAggregator.getTraces(merged).traces;
    assertEquals(1, traces.size());
    
    assertEquals("Trace: (a (b (c) (d (e))))", traces.get(0).printBrief());
    
  }
  
  /**
   * Make a mock Span including client-side timing data.
   */
  public Span createClientSpan(ID traceID, ID spanID, ID parentID, String msgName) {
    Span out = new Span();
    out.setSpanID(spanID);
    out.setTraceID(traceID);
    out.setRequestorHostname(new String("requestorHostname"));
    
    if (parentID != null) {
      out.setParentSpanID(parentID);
    }
    out.setMessageName(msgName);
    out.setComplete(false);
    
    TimestampedEvent event1 = new TimestampedEvent();
    event1.setEvent(SpanEvent.CLIENT_SEND);
    event1.setTimeStamp(System.currentTimeMillis() * 1000000);
    
    TimestampedEvent event2 = new TimestampedEvent();
    event2.setEvent(SpanEvent.CLIENT_RECV);
    event2.setTimeStamp(System.currentTimeMillis() * 1000000);
    
    out.setEvents(new GenericData.Array<TimestampedEvent>(
        2, Schema.createArray(TimestampedEvent.SCHEMA$)));
    out.getEvents().add(event1);
    out.getEvents().add(event2);
    
    return out;
  }
  
  /**
   * Make a mock Span including server-side timing data.
   */
  public Span createServerSpan(ID traceID, ID spanID, ID parentID, String msgName) {
    Span out = new Span();
    out.setSpanID(spanID);
    out.setTraceID(traceID);
    out.setResponderHostname(new String("responderHostname"));
    
    if (parentID != null) {
      out.setParentSpanID(parentID);
    }
    out.setMessageName(msgName);
    out.setComplete(false);
    
    TimestampedEvent event1 = new TimestampedEvent();
    event1.setEvent(SpanEvent.SERVER_RECV);
    event1.setTimeStamp(System.currentTimeMillis());
    
    TimestampedEvent event2 = new TimestampedEvent();
    event2.setEvent(SpanEvent.SERVER_SEND);
    event2.setTimeStamp(System.currentTimeMillis());
    
    out.setEvents(new GenericData.Array<TimestampedEvent>(
        2, Schema.createArray(TimestampedEvent.SCHEMA$)));
    out.getEvents().add(event1);
    out.getEvents().add(event2);
    
    return out;
  }
}
