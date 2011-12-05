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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class TestSpanTraceFormation {
  
  @Test
  public void testSpanEquality() {
    Span root = new Span();
    root.setSpanID(Util.idValue(10));
    root.setParentSpanID(null);
    root.setMessageName(new String("startCall"));
    
    Span a = new Span();
    a.setSpanID(Util.idValue(11));
    a.setParentSpanID(Util.idValue(10));
    a.setMessageName(new String("childCall1"));
    
    Span b = new Span();
    b.setSpanID(Util.idValue(12));
    b.setParentSpanID(Util.idValue(10));
    b.setMessageName(new String("childCall2"));
    
    Span c = new Span();
    c.setSpanID(Util.idValue(13));
    c.setParentSpanID(Util.idValue(10));
    c.setMessageName(new String("childCall3"));
    
    List<Span> spans = new LinkedList<Span>();
    spans.add(root);
    spans.add(a);
    spans.add(b);
    spans.add(c);
    Trace trace1 = Trace.extractTrace(spans);
    
    Span d = new Span();
    d.setSpanID(Util.idValue(11));
    d.setParentSpanID(Util.idValue(10));
    d.setMessageName(new String("childCall1"));
    
    Span e = new Span();
    e.setSpanID(Util.idValue(12));
    e.setParentSpanID(Util.idValue(10));
    e.setMessageName(new String("childCall2"));
    
    Span f = new Span();
    f.setSpanID(Util.idValue(13));
    f.setParentSpanID(Util.idValue(10));
    f.setMessageName(new String("childCall3"));
    
    spans.clear();
    spans.add(d);
    spans.add(e);
    spans.add(f);
    spans.add(root);
    Trace trace2 = Trace.extractTrace(spans);
    
    assertEquals(trace1.executionPathHash(), trace2.executionPathHash());
  }
  
  
  @Test
  public void testSpanEquality2() {
    Span root = new Span();
    root.setSpanID(Util.idValue(10));
    root.setParentSpanID(null);
    root.setMessageName(new String("startCall"));
    
    Span a = new Span();
    a.setSpanID(Util.idValue(11));
    a.setParentSpanID(Util.idValue(10));
    a.setMessageName(new String("childCall1"));
    
    Span b = new Span();
    b.setSpanID(Util.idValue(12));
    b.setParentSpanID(Util.idValue(10));
    b.setMessageName(new String("childCall2"));
    
    Span c = new Span();
    c.setSpanID(Util.idValue(13));
    c.setParentSpanID(Util.idValue(10));
    c.setMessageName(new String("childCall3"));
    
    List<Span> spans = new LinkedList<Span>();
    spans.add(root);
    spans.add(a);
    spans.add(b);
    spans.add(c);
    Trace trace1 = Trace.extractTrace(spans);
    
    Span d = new Span();
    d.setSpanID(Util.idValue(11));
    d.setParentSpanID(Util.idValue(10));
    d.setMessageName(new String("childCall1"));
    
    Span e = new Span();
    e.setSpanID(Util.idValue(12));
    e.setParentSpanID(Util.idValue(10));
    e.setMessageName(new String("childCall2"));
    
    Span f = new Span();
    f.setSpanID(Util.idValue(13));
    f.setParentSpanID(Util.idValue(10));
    f.setMessageName(new String("childCall3"));
    
    Span g = new Span();
    g.setSpanID(Util.idValue(14));
    g.setParentSpanID(Util.idValue(13));
    g.setMessageName(new String("childCall4"));
    
    spans.clear();
    spans.add(d);
    spans.add(e);
    spans.add(f);
    spans.add(g);
    spans.add(root);
    Trace trace2 = Trace.extractTrace(spans);
    
    assertFalse(trace1.executionPathHash() == trace2.executionPathHash());
  }
  
  /** Create a span with bogus timing events. */
  public static Span createFullSpan(Long id, Long parentID, String messageName) {
    Span out = new Span();
    out.setSpanID(Util.idValue(id));
    if (parentID != null) {
      out.setParentSpanID(Util.idValue(parentID));
    }
    out.setMessageName(new String(messageName));
    
    out.setEvents(new GenericData.Array<TimestampedEvent>(
        4, Schema.createArray(TimestampedEvent.SCHEMA$)));
    
    for (SpanEvent ev: SpanEvent.values()) {
      TimestampedEvent newEvent = new TimestampedEvent();
      newEvent.setTimeStamp(System.currentTimeMillis() * 1000000);
      newEvent.setEvent(ev);
      out.getEvents().add(newEvent);
    }
    
    out.setComplete(true);
    
    return out;
  }
  
  /**
   * Describe this trace with two different orderings and make sure
   * they equate to being equal. 
   * 
   *    b
   *   / 
   *  a -b    g-i
   *   \     /
   *    d-e-f
   *        \
   *         g-i
   */
 
  @Test
  public void testSpanEquality3() {
    
    Span a = createFullSpan((long) 1, null, "a");
    Span b1 = createFullSpan((long) 2, (long) 1, "b");
    Span b2 = createFullSpan((long) 3, (long) 1, "b");
    Span d = createFullSpan((long) 4, (long) 1, "d");
    Span e = createFullSpan((long) 5, (long) 4, "e");
    Span f = createFullSpan((long) 6, (long) 5, "f");
    Span g1 = createFullSpan((long) 7, (long) 6, "g");
    Span g2 = createFullSpan((long) 8, (long) 6, "g");
    Span i1 = createFullSpan((long) 9, (long) 7, "i");
    Span i2 = createFullSpan((long) 10, (long) 8, "i");
    
    List<Span> spans = new LinkedList<Span>();
    spans.addAll(Arrays.asList(new Span[] {a, b1, b2, d, e, f, g1, g2, i1, i2}));
    Trace trace1 = Trace.extractTrace(spans);
    
    // Re-order and make sure still equivalent
    spans.clear();
    spans.addAll(Arrays.asList(new Span[] {i2, b1, g1, e, f, d, b2, g2, i1, a}));
    Trace trace2 = Trace.extractTrace(spans);
    assertNotNull(trace1);
    assertNotNull(trace2);
    assertEquals(trace1.executionPathHash(), trace2.executionPathHash());
    assertEquals(trace1.executionPathHash(), trace2.executionPathHash());
    
    // Remove a span and make sure not equivalent
    spans.clear();
    spans.addAll(Arrays.asList(new Span[] {i2, b1, g1, e, f, d, b2, g2, a}));
    Trace trace3 = Trace.extractTrace(spans);
    assertNotNull(trace3);
    assertTrue(trace1.executionPathHash() == trace2.executionPathHash());
    assertTrue(trace1.executionPathHash() != trace3.executionPathHash());
    assertTrue(trace2.executionPathHash() != trace3.executionPathHash());
  }
  
  @Test
  public void testBasicTraceFormation() {
    Span root = new Span();
    root.setSpanID(Util.idValue(10));
    root.setParentSpanID(null);
    root.setMessageName(new String("startCall"));
    
    Span a = new Span();
    a.setSpanID(Util.idValue(11));
    a.setParentSpanID(Util.idValue(10));
    a.setMessageName(new String("childCall1"));
    
    Span b = new Span();
    b.setSpanID(Util.idValue(12));
    b.setParentSpanID(Util.idValue(10));
    b.setMessageName(new String("childCall2"));
    
    Span c = new Span();
    c.setSpanID(Util.idValue(13));
    c.setParentSpanID(Util.idValue(10));
    c.setMessageName(new String("childCall3"));
    
    List<Span> spans = new LinkedList<Span>();
    spans.add(root);
    spans.add(a);
    spans.add(b);
    spans.add(c);
    Trace trace = Trace.extractTrace(spans);
    
    assertNotNull(trace);
    
    TraceNode rootNode = trace.getRoot();
    assertEquals(rootNode.span.getMessageName(), new String("startCall"));
    assertEquals(3, rootNode.children.size());
    boolean found1, found2, found3;
    found1 = found2 = found3 = false;
    
    for (TraceNode tn: rootNode.children) {
      if (tn.span.getMessageName().equals(new String("childCall1"))) {
        found1 = true;
      }
      if (tn.span.getMessageName().equals(new String("childCall2"))) {
        found2 = true;
      }
      if (tn.span.getMessageName().equals(new String("childCall3"))) {
        found3 = true;
      }
      assertNotNull(tn.children);
      assertEquals(0, tn.children.size());
    }
    assertTrue(found1);
    assertTrue(found2);
    assertTrue(found3);
   }
 }
