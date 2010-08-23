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

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Collections;

/**
 * A Trace is a tree of spans which reflects the actual call structure of a 
 * recursive RPC call tree. Each node in a Trace represents a RPC 
 * request/response pair. Each node also has zero or more child nodes.
 */
public class Trace {  
  private TraceNode root;

  /**
   * Construct a trace given a root TraceNode.
   */
  public Trace(TraceNode root) {
    this.root = root;
  }
  
  /**
   * Return the root node of this trace.
   */
  public TraceNode getRoot() {
    return this.root;
  }
 
  /**
   * Provide a hashCode unique to the execution path of this trace.
   * 
   * This is useful for grouping several traces which represent the same
   * execution path (for instance, when we want to calculate averages for a
   * large number of identical traces).
   */
  public int executionPathHash() {
    // The string representation will be unique to a call tree, so we
    // can borrow the hashCode from that string.
    return this.printBrief().hashCode();
  }

  private class NodeComparator implements Comparator<TraceNode> {
    @Override @SuppressWarnings("unchecked")
    public int compare(TraceNode tn0, TraceNode tn1) {
      // We sort nodes alphabetically by the message name
      int result = ((Comparable)tn0.span.messageName).compareTo((Comparable)tn1.span.messageName);
      
      if (result != 0) {
        return result;
      }
      /* NOTE:
       * If two spans containing the *same* RPC message share a parent, we need
       * a way to consistently order them. Here, we use the send time to
       * break ties. This will only work deterministically for non-async
       * clients. For asynchronous clients, aggregated statistics based on this 
       * ordering may be incorrect, since we have no way to disambiguate one
       * function call from another. 
       */
      else {
        Long tn0SendTime = tn0.extractEventTime(tn0, SpanEvent.CLIENT_SEND);
        Long tn1SendTime = tn1.extractEventTime(tn1, SpanEvent.CLIENT_SEND);
        
        return tn0SendTime.compareTo(tn1SendTime);
      }
    }
  }
  
  /**
   * Print a brief description of this trace describing the execution
   * path, but not timing data. This is for debugging or quickly profiling
   * traces.
   * 
   * For instance the trace:
   *     x
   *    /
   *   w
   *    \
   *     y--z
   *     
   * is encoded as:
   * (w (x) (y (z)))
   */
  public String printBrief() {
    if (this.root == null) { return "Trace: <empty>"; }
    String out = "Trace: (";
    out += this.root.span.messageName + " ";
    out += printBriefRecurse(root.children);
    out += ")";
    return out;
  }
  
  private String printBriefRecurse(List<TraceNode> children) {
    String out = "";
    // We sort so equivalent traces always print identically 
    Collections.sort(children, new NodeComparator());
    for (int i = 0; i < children.size(); i++) {
      TraceNode tn = children.get(i);
      out += "(" + tn.span.messageName;
      if (tn.children.size() > 0) {
        out += " ";
        out += printBriefRecurse(tn.children);
      }
      out += ")";
      if (i != children.size() - 1) {
        out += " ";
      }
    }
    return out;
  }
  
  /**
   * Print a description of this trace which includes timing data. This is for 
   * debugging or quickly profiling traces.
   * 
   * For instance the trace:
   *     x
   *    /
   *   w
   *    \
   *     x
   *     
   * Might print as:
   * w 87ms
   *  x 10ms
   *  x 2ms
   */
  public String printWithTiming() {
    if (this.root == null) { return "Trace: <empty>"; }
    String out = "Trace: " + "\n";
    List<TraceNode> rootList = new LinkedList<TraceNode>();
    rootList.add(this.root);
    out += printWithTimingRecurse(rootList, 0);
    return out;
  }
  
  private String printWithTimingRecurse(List<TraceNode> children, int depth) {
    String out = "";
    // We sort so equivalent traces always print identically 
    Collections.sort(children, new NodeComparator());
    for (TraceNode tn : children) {
      long clientSend = 0;
      long clientReceive = 0;
      for (TimestampedEvent te: tn.span.events) {
        if (te.event instanceof SpanEvent) {
          SpanEvent ev = (SpanEvent) te.event;
          if (ev.equals(SpanEvent.CLIENT_RECV)) {
            clientReceive = te.timeStamp / 1000000;
          } else if (ev.equals(SpanEvent.CLIENT_SEND)) {
            clientSend = te.timeStamp / 1000000;
          }
        }
      }
      
      for (int i = 0; i < depth; i++) { out = out + "  "; } // indent
      out += tn.span.messageName + " " + (clientReceive - clientSend) + "ms\n";
      if (tn.children.size() > 0) {
        out += printWithTimingRecurse(tn.children, depth + 1);
      }
    }

    return out;
  }
  
  /**
   * Construct a Trace from a list of Span objects. If no such trace
   * can be created (if the list does not describe a complete trace)
   * returns null.
   */
  public static Trace extractTrace(List<Span> spans) {
    /**
     * Map of span id's to a list of child span id's
     */
    HashMap<Long, List<Long>> children = new HashMap<Long, List<Long>>();
    
    /**
     * Map of span id's to spans
     */
    HashMap<Long, Span> spanRef = new HashMap<Long, Span>();
    
    /**
     * Root span
     */
    Span rootSpan = null;
    
    for (Span s: spans) {
      spanRef.put(Util.longValue(s.spanID), s);
      if (s.parentSpanID == null) {
        rootSpan = s;
      } else {
        if (children.get(Util.longValue(s.parentSpanID)) == null) {
          LinkedList<Long> list = new LinkedList<Long>();
          list.add(Util.longValue(s.spanID));
          children.put(Util.longValue(s.parentSpanID), list);
        } else {
          children.get(Util.longValue(s.parentSpanID)).add(
              Util.longValue(s.spanID));
        }
      }
    }
    if (rootSpan == null) { // We never found a root
      return null;
    }
    TraceNode rootNode = getNode(rootSpan, spanRef, children);
    return new Trace(rootNode);
  }
  
  /**
   * Recursive helper method to create a span tree. 
   */
  private static TraceNode getNode(
      Span s, HashMap<Long, Span> spanRef, HashMap<Long, List<Long>> children) {
    TraceNode out = new TraceNode(s, new LinkedList<TraceNode>());
    
    List<Long> kids = children.get(Util.longValue(s.spanID));
    if (kids == null) { return out; } // no children (base case) 
    
    for (long childID: kids) {
      Span childSpan = spanRef.get(childID);
      if (childSpan == null) { return null; } // invalid span reference
      out.children.add(getNode(childSpan, spanRef, children));
    }
    return out;
  }
}
