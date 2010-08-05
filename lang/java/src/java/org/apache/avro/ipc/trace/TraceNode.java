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

import java.util.List;

/**
 * A node of of an RPC {@link Trace}. Each node stores a {@link Span} object
 * and a list of zero or more child nodes.
 */
class TraceNode {
  /**
   * The {@link Span} to which corresponds to this node in the call tree.
   */
  public Span span;
  
  /**
   * A list of this TraceNode's children.
   */
  public List<TraceNode> children;

  public TraceNode(Span span, List<TraceNode> children) {
    this.span = span;
    this.children = children;
  }
  
  public TraceNode() {
    
  }
 
  /** 
   * Return the time stamp associated with a particular SpanEvent in this
   * Trace Node. Return -1 if the TraceNode's Span did not contain that event.
   */
  public long extractEventTime(TraceNode tn, SpanEvent e) {
    for (TimestampedEvent te: tn.span.events) {
      if ((te.event instanceof SpanEvent) && 
          (SpanEvent) te.event == e) {
        return te.timeStamp;
      }
    }
    return -1;
  }
 
  /**
   * Return time delta between { @link SpanEvent.CLIENT_SEND } and 
   * { @link SpanEvent.SERVER_RECV }. This may be negative or zero in the 
   * case of clock skew.
   */
  public long getPreNetworkTime() {
    long clientSend = extractEventTime(this, SpanEvent.CLIENT_SEND);
    long serverReceive = extractEventTime(this, SpanEvent.SERVER_RECV);
    
    return serverReceive - clientSend;
  }
  
  /**
   * Return time delta between { @link SpanEvent.SERVER_SEND } and 
   * { @link SpanEvent.CLIENT_RECV }. This may be negative or zero in the 
   * case of clock skew.
   */
  public long getPostNetworkTime() {
    long serverSend = extractEventTime(this, SpanEvent.SERVER_SEND);
    long clientReceive = extractEventTime(this, SpanEvent.CLIENT_RECV);
    
    return clientReceive - serverSend;
  }
  
  /**
   * Return time delta between { @link SpanEvent.SERVER_RECV } and 
   * { @link SpanEvent.SERVER_SEND}.
   */
  public long getProcessTime() {
    long serverReceive = extractEventTime(this, SpanEvent.SERVER_RECV);
    long serverSend = extractEventTime(this, SpanEvent.SERVER_SEND);
    
    return serverSend - serverReceive;
  } 
}
