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
public class TraceNode {
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
  public long getPreLinkTime() {
    long clientSend = extractEventTime(this, SpanEvent.CLIENT_SEND);
    long serverReceive = extractEventTime(this, SpanEvent.SERVER_RECV);
    
    return serverReceive - clientSend;
  }
  
  /**
   * Return pre-link time as a string.
   */
  public String getPreLinkTimeString() {
    return Util.printableTime(this.getPreLinkTime());
  }
  
  /**
   * Return time delta between { @link SpanEvent.SERVER_SEND } and 
   * { @link SpanEvent.CLIENT_RECV }. This may be negative or zero in the 
   * case of clock skew.
   */
  public long getPostLinkTime() {
    long serverSend = extractEventTime(this, SpanEvent.SERVER_SEND);
    long clientReceive = extractEventTime(this, SpanEvent.CLIENT_RECV);
    
    return clientReceive - serverSend;
  }
  
  /**
   * Return post-link time as a string.
   */
  public String getPostLinkTimeString() {
    return Util.printableTime(this.getPreLinkTime());
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
  
  /**
   * Return cpu time as a string.
   */
  public String getProcessTimeString() {
    return Util.printableTime(this.getProcessTime());
  }
  
  /**
   * Return the children of this node.
   */
  public List<TraceNode> getChildren() {
    return this.children;
  }
  
  // Span data getters for Velicty
  public String getRequestPayloadSize() { 
    return Util.printableBytes(this.span.requestPayloadSize);
  }
  public String getResponsePayloadSize() { 
    return Util.printableBytes(this.span.responsePayloadSize);
  }
  public String getRequestorHostname() {
    return this.span.requestorHostname.toString();
  }
  public String getResponderHostname() {
    return this.span.responderHostname.toString();
  }
  public String getMessageName() {
    return this.span.messageName.toString();
  }
  public String getLatencyTimeString() {
    return Util.printableTime(this.getPreLinkTime() + 
        this.getProcessTime() + this.getPostLinkTime());
  }
}
