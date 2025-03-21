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

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.util.SchemaUtil;

/**
 * an {@link AvroTypeException} with extra fields used to trace back the path to
 * a bad value through an object graph
 */
public class TracingAvroTypeException extends AvroTypeException implements PathTracingException<AvroTypeException> {
  private final List<PathElement> reversePath;

  public TracingAvroTypeException(AvroTypeException cause) {
    super(cause.getMessage(), cause);
    this.reversePath = new ArrayList<>(3); // expected to be short
  }

  @Override
  public void tracePath(PathElement step) {
    reversePath.add(step);
  }

  @Override
  public AvroTypeException summarize(Schema root) {
    AvroTypeException cause = (AvroTypeException) getCause();

    StringBuilder sb = new StringBuilder();
    sb.append(cause.getMessage());

    if (reversePath != null && !reversePath.isEmpty()) {
      sb.append(" at ");
      if (root != null) {
        sb.append(SchemaUtil.describe(root));
      }
      for (int i = reversePath.size() - 1; i >= 0; i--) {
        PathElement step = reversePath.get(i);
        sb.append(step.toString());
      }
    }
    return new AvroTypeException(sb.toString(), cause);
  }
}
