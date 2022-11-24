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
package org.apache.avro.idl;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Avro Schema utilities, to traverse...
 */
public final class Schemas {

  private Schemas() {
  }

  /**
   * Depth first visit.
   */
  public static <T> T visit(final Schema start, final SchemaVisitor<T> visitor) {
    // Set of Visited Schemas
    IdentityHashMap<Schema, Schema> visited = new IdentityHashMap<>();
    // Stack that contains the Schemas to process and afterVisitNonTerminal
    // functions.
    // Deque<Either<Schema, Supplier<SchemaVisitorAction>>>
    // Using Either<...> has a cost we want to avoid...
    Deque<Object> dq = new ArrayDeque<>();
    dq.push(start);
    Object current;
    while ((current = dq.poll()) != null) {
      if (current instanceof Supplier) {
        // We are executing a non-terminal post visit.
        @SuppressWarnings("unchecked")
        SchemaVisitorAction action = ((Supplier<SchemaVisitorAction>) current).get();
        switch (action) {
        case CONTINUE:
          break;
        case SKIP_SUBTREE:
          throw new UnsupportedOperationException();
        case SKIP_SIBLINGS:
          while (dq.peek() instanceof Schema) {
            dq.remove();
          }
          break;
        case TERMINATE:
          return visitor.get();
        default:
          throw new UnsupportedOperationException("Invalid action " + action);
        }
      } else {
        Schema schema = (Schema) current;
        boolean terminate;
        if (visited.containsKey(schema)) {
          terminate = visitTerminal(visitor, schema, dq);
        } else {
          Schema.Type type = schema.getType();
          switch (type) {
          case ARRAY:
            terminate = visitNonTerminal(visitor, schema, dq, Collections.singleton(schema.getElementType()));
            visited.put(schema, schema);
            break;
          case RECORD:
            terminate = visitNonTerminal(visitor, schema, dq, () -> schema.getFields().stream().map(Field::schema)
                .collect(Collectors.toCollection(ArrayDeque::new)).descendingIterator());
            visited.put(schema, schema);
            break;
          case UNION:
            terminate = visitNonTerminal(visitor, schema, dq, schema.getTypes());
            visited.put(schema, schema);
            break;
          case MAP:
            terminate = visitNonTerminal(visitor, schema, dq, Collections.singleton(schema.getValueType()));
            visited.put(schema, schema);
            break;
          default:
            terminate = visitTerminal(visitor, schema, dq);
            break;
          }
        }
        if (terminate) {
          return visitor.get();
        }
      }
    }
    return visitor.get();
  }

  private static boolean visitNonTerminal(final SchemaVisitor<?> visitor, final Schema schema, final Deque<Object> dq,
      final Iterable<Schema> itSupp) {
    SchemaVisitorAction action = visitor.visitNonTerminal(schema);
    switch (action) {
    case CONTINUE:
      dq.push((Supplier<SchemaVisitorAction>) () -> visitor.afterVisitNonTerminal(schema));
      itSupp.forEach(dq::push);
      break;
    case SKIP_SUBTREE:
      dq.push((Supplier<SchemaVisitorAction>) () -> visitor.afterVisitNonTerminal(schema));
      break;
    case SKIP_SIBLINGS:
      while (dq.peek() instanceof Schema) {
        dq.remove();
      }
      break;
    case TERMINATE:
      return true;
    default:
      throw new UnsupportedOperationException("Invalid action " + action + " for " + schema);
    }
    return false;
  }

  private static boolean visitTerminal(final SchemaVisitor<?> visitor, final Schema schema, final Deque<Object> dq) {
    SchemaVisitorAction action = visitor.visitTerminal(schema);
    switch (action) {
    case CONTINUE:
      break;
    case SKIP_SIBLINGS:
      while (dq.peek() instanceof Schema) {
        dq.remove();
      }
      break;
    case TERMINATE:
      return true;
    case SKIP_SUBTREE:
    default:
      throw new UnsupportedOperationException("Invalid action " + action + " for " + schema);
    }
    return false;
  }
}
