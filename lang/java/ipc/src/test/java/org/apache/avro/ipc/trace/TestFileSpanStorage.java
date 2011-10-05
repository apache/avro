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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.util.Utf8;
import org.junit.Test;

/**
 * Unit tests for { @link FileSpanStorage }.
 */
public class TestFileSpanStorage {
  
  @Test
  public void testBasicStorage() {
    TracePluginConfiguration conf = new TracePluginConfiguration();
    FileSpanStorage test = new FileSpanStorage(false, conf);
    Span s = Util.createEventlessSpan(Util.idValue(1), Util.idValue(1), null);
    s.setMessageName(new Utf8("message"));
    test.addSpan(s);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertTrue(test.getAllSpans().contains(s));
    test.clear();
  }
  
  @Test
  public void testTonsOfSpans() {
    TracePluginConfiguration conf = new TracePluginConfiguration();
    FileSpanStorage test = new FileSpanStorage(false, conf);
    test.setMaxSpans(100000);
    List<Span> spans = new ArrayList<Span>(50000);
    for (int i = 0; i < 50000; i++) {
      Span s = Util.createEventlessSpan(Util.idValue(i), Util.idValue(i), null);
      s.setMessageName(new Utf8("message"));
      test.addSpan(s);
      spans.add(s);
    }
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertEquals(50000, test.getAllSpans().size());

    // Test fewer spans but explicitly call containsAll
    TracePluginConfiguration conf2 = new TracePluginConfiguration();
    FileSpanStorage test2 = new FileSpanStorage(false, conf2);
    test.setMaxSpans(100000);
    spans.clear();
    for (int i = 0; i < 5000; i++) {
      Span s = Util.createEventlessSpan(Util.idValue(i), Util.idValue(i), null);
      s.setMessageName(new Utf8("message"));
      test2.addSpan(s);
      spans.add(s);
    }
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertTrue(test.getAllSpans().containsAll(spans));
    test.clear();
    test2.clear();
  }
  
  @Test
  public void testBasicMaxSpans() {
    TracePluginConfiguration conf = new TracePluginConfiguration();
    FileSpanStorage test = new FileSpanStorage(false, conf);
    test.setMaxSpans(10);
    
    // Add a bunch of spans
    for (int i = 0; i < 100; i++) {
      Span s = Util.createEventlessSpan(Util.idValue(i), Util.idValue(i), null);
      s.setMessageName(new Utf8("message"));
      test.addSpan(s);
    }
    
    List<Span> lastNine = new LinkedList<Span>();
    for (int i = 0; i < 9; i++) {
      Span s = Util.createEventlessSpan(Util.idValue(100 + i), Util.idValue(100 + i), null);
      s.setMessageName(new Utf8("message"));
      lastNine.add(s);
      test.addSpan(s);
    }
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    List<Span> retreived = test.getAllSpans();
    assertEquals(9, retreived.size());
    assertTrue(retreived.containsAll(lastNine));
    
    test.clear();
  }
  
  @Test
  public void testRangeQuery1() {
    TracePluginConfiguration conf = new TracePluginConfiguration();
    conf.fileGranularitySeconds = 1;
    FileSpanStorage test = new FileSpanStorage(false, conf);
    test.setMaxSpans(10000);
    
    long cutOff1 = 0;
    long cutOff2 = 0;
    
    int numSpans = 10;
    
    Span[] spans = new Span[numSpans];
    // Add some spans
    for (int i = 0; i < numSpans; i++) {
      if (i == 1) { cutOff1 = (System.currentTimeMillis() - 20) * 1000000; }
      
      Span s = Util.createEventlessSpan(Util.idValue(i), Util.idValue(i), null);
      TimestampedEvent te1 = new TimestampedEvent();
      te1.setTimeStamp(System.currentTimeMillis() * 1000000);
      te1.setEvent(SpanEvent.CLIENT_SEND);
      
      TimestampedEvent te2 = new TimestampedEvent();
      te2.setTimeStamp(System.currentTimeMillis() * 1000000);
      te2.setEvent(SpanEvent.CLIENT_RECV);
      s.getEvents().add(te1);
      s.getEvents().add(te2);
      
      s.setMessageName(new Utf8("message"));
      test.addSpan(s);
      spans[i] = s;
      
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      
      }
      if (i == numSpans - 2) {
        cutOff2 = (System.currentTimeMillis() - 20) * 1000000; 
      }
    }
    
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
    }
    
    List<Span> retrieved = test.getSpansInRange(cutOff1, cutOff2);
    assertEquals(numSpans - 2, retrieved.size());
    
    assertFalse(retrieved.contains(spans[0]));
    for (int j=1; j < numSpans - 2; j++) {
      assertTrue(retrieved.contains(spans[j]));
    }
    assertFalse(retrieved.contains((spans[spans.length - 1])));
    
    test.clear();
  }
}
