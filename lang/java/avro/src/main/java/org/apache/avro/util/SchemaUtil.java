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
package org.apache.avro.util;

import org.apache.avro.Schema;

import java.util.StringJoiner;

public class SchemaUtil {

  private SchemaUtil() {
    // utility class
  }

  public static String describe(Schema schema) {
    if (schema == null) {
      return "unknown";
    }
    switch (schema.getType()) {
    case UNION:
      StringJoiner csv = new StringJoiner(", ");
      for (Schema branch : schema.getTypes()) {
        csv.add(describe(branch));
      }
      return "[" + csv + "]";
    case MAP:
      return "Map<String, " + describe(schema.getValueType()) + ">";
    case ARRAY:
      return "List<" + describe(schema.getElementType()) + ">";
    default:
      return schema.getName();
    }
  }

  public static String describe(Object datum) {
    if (datum == null) {
      return "null";
    }
    return datum + " (a " + datum.getClass().getName() + ")";
  }
}
