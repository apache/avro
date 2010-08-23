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

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;


/**
 * Provides statistics and representation of a collection of {@link Trace}'s
 * which share the same execution path.
 * 
 * For instance: given the following execution pattern:
 *   
 *     b--d
 *    /
 *   a
 *    \
 *     c--e
 *     
 * We might want a report detailing statistics on across several Traces's.
 * Such as a display of average timing data:
 * 
 * [message]         --- = average link time    +++ = average compute time    
 * a                 <----++++++++++++++++++++++++++++++++++++++++--->
 *   b                    <----+++++++++++++++++++--->
 *     d                         <-----++++----->
 *   c                                                <---+++++->
 *     e                                                   <+>
 *     
 * This requires calculating averages of different timing data at each node
 * in the call tree.
 */
public class TraceCollection {
  
  /**
   * Class to store the timing data associated with a particluar trace.
   */
  public class TraceTiming implements Comparable<TraceTiming> {
    public long preLinkTime;
    public long computeTime;
    public long postLinkTime;
    
    public TraceTiming(Long preLinkTime, Long computeTime, Long postLinkTime) {
      this.preLinkTime = preLinkTime;
      this.computeTime = computeTime;
      this.postLinkTime = postLinkTime;
    }
    
    public Long getTotalTime() {
      return new Long(this.preLinkTime + this.computeTime + this.postLinkTime);
    }
    
    public int compareTo(TraceTiming other) {
      return this.getTotalTime().compareTo(other.getTotalTime());
    }
  }
  
  /**
   * Class to store statistics for a particular node in the RPC call tree.
   */
  public class TraceNodeStats {
    CharSequence messageName;
    List<TraceNodeStats> children;
    
    TreeSet<Long> requestPayloads;
    TreeSet<Long> responsePayloads;
    
    // Time can be divided into three components
    public TreeSet<TraceTiming> traceTimings;
    
    /**
     * Create a TraceNodeStats given a root TraceNode.
     */
    public TraceNodeStats(TraceNode root) {
      this.requestPayloads = new TreeSet<Long>();
      this.responsePayloads = new TreeSet<Long>();
      this.traceTimings = new TreeSet<TraceTiming>();
     
      this.messageName = root.span.messageName;
      this.children = new LinkedList<TraceNodeStats>();
      
      for (TraceNode tn: root.children) {
        this.children.add(new TraceNodeStats(tn));
      }
    }
    
    // Velocity requires getters
    public SortedSet<Long> getRequestPayloads() { return this.requestPayloads; }
    public SortedSet<Long> getResponsePayloads() { return this.responsePayloads; }
    public SortedSet<TraceTiming> getTraceTimings() { return this.traceTimings; }
    public List<TraceNodeStats> getChildren() { return this.children; }
    public CharSequence getMessageName() { return this.messageName; } 
    
    // Convenience methods for templates
    public String getAverageTime(SortedSet<TraceTiming> input) {
      return Util.printableTime(getTimingAverage(input));
    }
    public String getMinTime(SortedSet<TraceTiming> input) {
      return Util.printableTime(input.first().getTotalTime());
    }
    public String getMaxTime(SortedSet<TraceTiming> input) {
      return Util.printableTime(input.last().getTotalTime());
    }
 
    public String getAverageBytes(SortedSet<Long> input) {
      return Util.printableBytes(getLongAverage(input));
    }
    public String getMinBytes(SortedSet<Long> input) {
      return Util.printableBytes(input.first());
    }
    public String getMaxBytes(SortedSet<Long> input) {
      return Util.printableBytes(input.last());
    }
    
    public String printBrief() {
      String out = "'" + messageName + "' ";
      out += "(Averages) " + "Request Payload: " + 
        this.getAverageBytes(this.requestPayloads) + " Response Payload: " + 
        this.getAverageBytes(this.responsePayloads) + " RTT: " +
        this.getAverageTime(this.traceTimings);
      return out;
    }
    
  }
  
  public TraceNodeStats getNodeWithID(int hashCode) {
    return getNodeWithIDRecurse(hashCode, this.root);
  }
  
  public TraceNodeStats getNodeWithIDRecurse(int hashCode, TraceNodeStats start) {
    if (start.hashCode() == hashCode) {
      return start; // base case, we've found it
    }
    else {
      for (TraceNodeStats tn: start.children) {
        TraceNodeStats potential = getNodeWithIDRecurse(hashCode, tn);
        if (potential != null) { return potential; }
      }
    }
    return null;
  }

  public class TraceComparotor implements Comparator<Trace> {
    @Override
    public int compare(Trace o1, Trace o2) {
      Long rtt1 = o1.getRoot().getPreLinkTime() + 
                  o1.getRoot().getProcessTime() + 
                  o1.getRoot().getPostLinkTime(); 
      Long rtt2 = o2.getRoot().getPreLinkTime() + 
                  o2.getRoot().getProcessTime() + 
                  o2.getRoot().getPostLinkTime();
      return rtt1.compareTo(rtt2);
    }
  }
  
  private static long getLongAverage(Collection<Long> c) {
    double out = 0;
    for (Long l: c) {
      out += (double) l /c.size(); // Do like this to avoid overflow
    }
    return (long) out;
  }
  
  private static long getTimingAverage(Collection<TraceTiming> c) {
    if (c == null) return 0;
    double out = 0;
    for (TraceTiming l: c) {
      Long val = l.getTotalTime();
      out += (double) val /c.size(); // Do like this to avoid overflow
    }
    return (long) out;
  }
  
  private int exectuionPathHash;
  private TraceNodeStats root;
  private TreeSet<Trace> traces; // Store traces, sorted by RTT
  
  /**
   * Create a TraceCollection using the given Trace as a model. Note that 
   * we do not add this trace to the statistics tracking in this constructor.
   */
  public TraceCollection(Trace t) {
    this.exectuionPathHash = t.executionPathHash();
    this.root = new TraceNodeStats(t.getRoot());
    this.traces = new TreeSet<Trace>(new TraceComparotor());
  }
  
  // Getters for Velocity
  public TraceNodeStats getRootNode() { return this.root; }
  public int getExecutionPathHash() { return this.exectuionPathHash; }  
  public TreeSet<Trace> getTraces() { return this.traces; }
  
  /**
   * Returns the [count] longest traces in this collection.
   */
  @SuppressWarnings("unchecked")
  public List<Trace> longestTraces(int count) {
    TreeSet<Trace> cloned = (TreeSet<Trace>) this.traces.clone();
    LinkedList<Trace> out = new LinkedList<Trace>();
    for (int i = 0; i < count; i++) {
      Trace toAdd = cloned.pollLast();
      if (toAdd == null) { break; }
      out.add(toAdd);
    }
    return out;
  }
  
  /**
   * Add a trace to this collection. Timing data from this trace will then be
   * included in aggregate statistics.
   */
  public void addTrace(Trace t) {
    this.traces.add(t);
    if (t.executionPathHash() != this.exectuionPathHash) {
      throw new IllegalArgumentException("Trace added which does not match" +
          " required execution path.");
    }
    recursiveProcess(t.getRoot(), this.root);
  }
  
  private void recursiveProcess(TraceNode tn, TraceNodeStats tns) { 
    if (tn.children.size() != tns.children.size() ||
        !tns.messageName.equals(tn.span.messageName)) {
      throw new IllegalArgumentException("Trace added does not match existing" +
          "trace");
    }
    
    tns.requestPayloads.add(tn.span.requestPayloadSize);
    tns.responsePayloads.add(tn.span.responsePayloadSize);
    tns.traceTimings.add(new TraceTiming(tn.getPreLinkTime(), 
        tn.getProcessTime(), tn.getPostLinkTime()));
    tns.messageName = tn.span.messageName;

    for (int i = 0; i < tn.children.size(); i++) {
      recursiveProcess(tn.children.get(i), tns.children.get(i));
    }
  }
  
  /**
   * Print a brief description of this Trace Collection with some summary 
   * data. Useful for debugging or quick profiling.
   */
  public String printBrief() {
    String out = "TraceCollection:\n";
    out += printRecurse(this.root, 0);
    return out;
  }

  public String printRecurse(TraceNodeStats n, int depth) {
    String out = "";
    for (int i = 0; i < depth; i++) {
      out = out + "  ";
    }
    out += n.printBrief() + "\n";
    for (TraceNodeStats child: n.children) {
      out = out + printRecurse(child, depth + 1);
    }
    return out;
  }
}
