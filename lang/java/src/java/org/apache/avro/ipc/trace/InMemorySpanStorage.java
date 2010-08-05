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

import java.util.LinkedList;
import java.util.List;

/**
 * Example implementation of SpanStorage which keeps spans in memory.
 * 
 * This is designed as a prototype for demonstration and testing. It should
 * not be used in production settings.
 *
 */
public class InMemorySpanStorage implements SpanStorage {
  private static final long DEFAULT_MAX_SPANS = 10000;
  
  protected LinkedList<Span> spans;
  private long maxSpans;

  public InMemorySpanStorage() {
    this.spans = new LinkedList<Span>();
    this.maxSpans = DEFAULT_MAX_SPANS;
  }
  
  @Override
  public void addSpan(Span s) {
    synchronized (this.spans) {  
      this.spans.add(s);
      if (this.spans.size() > this.maxSpans) {
        this.spans.removeFirst();
      }
    }
  }

  @Override
  public void setMaxSpans(long maxSpans) {
    this.maxSpans = maxSpans;
    
    synchronized (this.spans) {
      while (this.spans.size() > maxSpans) {
        this.spans.removeFirst();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Span> getAllSpans() {
    return (LinkedList<Span>) this.spans.clone();
  }
}
