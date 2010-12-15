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
 * Responsible for storing spans locally and answering span queries.
 * 
 * Since query for a given set of spans may persist over several RPC
 * calls, they are indexed by a handle.
 *
 */
public interface SpanStorage {
  long DEFAULT_MAX_SPANS = 10000L;
  long MILLIS_PER_SECOND = 1000L;
  long NANOS_PER_SECOND = 1000000000L;
  
  /**
   * Add a span. 
   * @param s
   */
  void addSpan(Span s);
  
  /**
   * Set the maximum number of spans to have in storage at any given time.
   */
  void setMaxSpans(long maxSpans);
  
  /**
   * Return a list of all spans currently stored. For testing.
   */
  List<Span> getAllSpans();

  /**
   * Return a list of all spans that fall within the time given range. 
   * @param start UNIX time (in nanoseconds) as a long
   * @param end UNIX time (in nanoseconds) as a long
   */
  List<Span> getSpansInRange(long start, long end);
}
