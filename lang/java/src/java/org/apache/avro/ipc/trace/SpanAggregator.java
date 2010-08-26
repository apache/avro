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

import static org.apache.avro.ipc.trace.Util.idsEqual;
import static org.apache.avro.ipc.trace.Util.longValue;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import edu.emory.mathcs.backport.java.util.Arrays;

/**
 * Utility methods for aggregating spans together at various
 * points of trace analysis.
 * 
 * The general workflow of trace analysis is:
 * 
 * Partial {@link Span}'s --> Complete {@link Span}'s -->
 * {@link Trace}'s --> {@link TraceCollection}'s
 */
public class SpanAggregator {
  /**
   * Class to store the results of span aggregation.
   */
  public static class SpanAggregationResults {
    /** Spans which have data from client and server. */
    public List<Span> completeSpans;
    
    /** Spans which have data only from client or server, or in which
     * an ID collision was detected.*/
    public List<Span> incompleteSpans;
    
    public SpanAggregationResults() {
      completeSpans = new LinkedList<Span>();
      incompleteSpans = new LinkedList<Span>();
    }
  }
  
  /**
   * Class to store the results of trace formation.
   */
  public static class TraceFormationResults {
    /** Traces which were successfully created. */
    public List<Trace> traces;
    
    /** Spans which did not describe a complete trace. */
    public List<Span> rejectedSpans;
    
    public TraceFormationResults() {
      traces = new LinkedList<Trace>();
      rejectedSpans = new LinkedList<Span>();
    }
  }
  
  /**
   * Merge a list of incomplete spans (data filled in from only client or 
   * server) into complete spans (full client and server data). 
   */
  @SuppressWarnings("unchecked")
  static SpanAggregationResults getFullSpans(List<Span> partials) {
    SpanAggregationResults out = new SpanAggregationResults();
    HashMap<Long, Span> seenSpans = new HashMap<Long, Span>();
    List<SpanEvent> allEvents = (List<SpanEvent>) Arrays.asList(
        SpanEvent.values());
    
    for (Span s: partials) {
      EnumSet<SpanEvent> foundEvents = Util.getAllEvents(s);
      
      // Span has complete data already
      if (foundEvents.containsAll(allEvents)) {
        out.completeSpans.add(s);
      } 
      // We haven't seen other half yet
      else if (!seenSpans.containsKey(
          Util.longValue(s.spanID))) {
        seenSpans.put(Util.longValue(s.spanID), s);
      }    
      // We have seen other half
      else {
        Span other = seenSpans.remove(Util.longValue(s.spanID));  
        if (!other.messageName.equals(s.messageName) ||
            !idsEqual(other.parentSpanID, s.parentSpanID)) {
          out.incompleteSpans.add(s);
          out.incompleteSpans.add(other);
        } else {
          foundEvents.addAll(Util.getAllEvents(other));
          if (other.requestorHostname != null) {
            s.requestorHostname = other.requestorHostname;
          }
          if (other.responderHostname != null) {
            s.responderHostname = other.responderHostname;
          }
          
          // We have a complete span between the two
          if (foundEvents.containsAll(allEvents)) {
            for (TimestampedEvent event: other.events) {
              s.events.add(event);
            }
            s.requestPayloadSize = Math.max(
                s.requestPayloadSize, other.requestPayloadSize);
            s.responsePayloadSize = Math.max(
                s.responsePayloadSize, other.responsePayloadSize);
            s.complete = true;
            out.completeSpans.add(s);
          }
        }
      }
    }
    
    // Flush unmatched spans
    for (Span s: seenSpans.values()) {
      out.incompleteSpans.add(s);
    }
    return out;
  }
  
  /**
   * Given a list of Spans extract as many Trace objects as possible.
   * A {@link Trace} is a tree-like data structure containing spans.
   */
  static TraceFormationResults getTraces(List<Span> spans) {
    /** Traces indexed by traceID. */
    HashMap<Long, List<Span>> traces = new HashMap<Long, List<Span>>();
    
    for (Span s: spans) {
      if (traces.get(longValue(s.traceID)) == null) {
         traces.put(longValue(s.traceID), new ArrayList<Span>());
      }
      traces.get(longValue(s.traceID)).add(s);
    }    
    
    TraceFormationResults out = new TraceFormationResults();
    
    for (List<Span> spanSet : traces.values()) {
       Trace trace = Trace.extractTrace(spanSet);
       if (trace != null) {
         out.traces.add(trace);
       }
       else {
         out.rejectedSpans.addAll(spanSet);
       }
    } 
    return out;
  }
  
  /**
   * Given a list of Traces, group traces which share an execution pattern
   * and return TraceCollection results for each one.
   */
  static List<TraceCollection> getTraceCollections(List<Trace> traces) {
    HashMap<Integer, TraceCollection> collections = 
      new HashMap<Integer, TraceCollection>();
    for (Trace t: traces) {
      if (!collections.containsKey(t.executionPathHash())) {
        TraceCollection collection = new TraceCollection(t);
        collections.put(t.executionPathHash(), collection);
      }
      collections.get(t.executionPathHash()).addTrace(t);
    }
    return new LinkedList<TraceCollection>(collections.values());
  }
}
