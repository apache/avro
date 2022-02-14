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
package org.apache.avro.generic;

import org.apache.avro.PathTracingException;
import org.apache.avro.Schema;
import org.apache.avro.path.MapKeyPredicate;
import org.apache.avro.path.PathElement;
import org.apache.avro.util.SchemaUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * a {@link NullPointerException} with extra fields used to trace back the path
 * to a null value through an object graph
 */
public class TracingNullPointException extends NullPointerException
    implements PathTracingException<NullPointerException> {
  private final NullPointerException cause;
  private final Schema expected;
  private final boolean customCoderUsed;
  private final List<PathElement> reversePath;

  public TracingNullPointException(NullPointerException cause, Schema expected, boolean customCoderUsed) {
    this.cause = cause;
    this.expected = expected;
    this.customCoderUsed = customCoderUsed;
    this.reversePath = new ArrayList<>(1); // assume short
  }

  @Override
  public void tracePath(PathElement step) {
    reversePath.add(step);
  }

  @Override
  public NullPointerException getCause() {
    return cause;
  }

  /**
   * @return a hopefully helpful error message
   */
  @Override
  public NullPointerException summarize(Schema root) {
    StringBuilder sb = new StringBuilder();
    sb.append("null value for (non-nullable) ");
    if (reversePath == null || reversePath.isEmpty()) {
      // very simple "shallow" NPE, no nesting at all, or custom coders used means we
      // have no data
      if (customCoderUsed) {
        sb.append("field or map key. no further details available as custom coders were used");
      } else {
        sb.append(SchemaUtil.describe(expected));
      }
    } else {
      PathElement innerMostElement = reversePath.get(0);
      boolean isNullMapKey = innerMostElement instanceof MapKeyPredicate
          && ((MapKeyPredicate) innerMostElement).getKey() == null;
      if (isNullMapKey) {
        sb.delete(0, sb.length()); // clear
        sb.append("null key in map");
      } else {
        sb.append(SchemaUtil.describe(expected));
      }
      sb.append(" at ");
      if (root != null) {
        sb.append(SchemaUtil.describe(root));
      }
      for (int i = reversePath.size() - 1; i >= 0; i--) {
        PathElement step = reversePath.get(i);
        sb.append(step.toString());
      }
    }
    NullPointerException summary = new NullPointerException(sb.toString());
    summary.initCause(cause);
    return summary;
  }
}
