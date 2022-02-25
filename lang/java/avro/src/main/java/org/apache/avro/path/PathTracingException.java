/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.path;

import org.apache.avro.Schema;

/**
 * interface for exceptions that can trace the AvroPath of an error
 *
 * @param <T> the regular (user-facing) exception that will be
 *            {@link #summarize(Schema)}ed out of this class
 */
public interface PathTracingException<T extends Throwable> {
  /**
   * appends a path element to the trace. expected to be called in reverse-order
   * as the exception bubbles up the stack
   *
   * @param step an AvroPath step tracing back from the location of the original
   *             exception towards the root of the data graph
   */
  void tracePath(PathElement step);

  /**
   * produces a user-facing exception to be thrown back out to user code
   * 
   * @param root the root object for the operation that generated the exception
   * @return an exception
   */
  T summarize(Schema root);
}
