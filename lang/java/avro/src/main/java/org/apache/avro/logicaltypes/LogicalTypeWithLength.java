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
package org.apache.avro.logicaltypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

public abstract class LogicalTypeWithLength extends LogicalType implements AvroPrimitive {
  public static final String LENGTH_PROP = "length";

  private int length;

  public LogicalTypeWithLength(String name, int length) {
    super(name);
    this.length = length;
  }

  public LogicalTypeWithLength(String name, Schema schema) {
    super(name);
    this.length = getLengthProperty(schema);
  }

  public int getLength() {
    return length;
  }

  public static Integer getLengthProperty(Schema schema) {
    Object p = schema.getObjectProp(LENGTH_PROP);
    if (p == null) {
      throw new IllegalArgumentException("Schema is missing the length property");
    } else if (p instanceof Integer) {
      return (Integer) p;
    } else if (p instanceof String) {
      try {
        return Integer.valueOf(p.toString());
      } catch (NumberFormatException e) {
      }
    }
    throw new IllegalArgumentException("Expected an integer for length property but got \"" + p.toString() + "\"");
  }

  public static int getLength(String text) {
    String[] parts = text.split("[\\(\\)]");
    if (parts.length > 1) {
      return Integer.parseInt(parts[1]);
    }
    throw new IllegalArgumentException(
        "Data type string must be in the format DATATYPE(length) with length being a positive integer");
  }

  @Override
  public void validate(Schema schema) {
    super.validate(schema);
    // validate the type
    if (schema.getType() != Schema.Type.STRING) {
      throw new IllegalArgumentException("Logical type " + getName() + " must be backed by string");
    }
    if (length <= 0) {
      throw new IllegalArgumentException("Invalid length: " + length + " (must be positive)");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    LogicalTypeWithLength typewithlength = (LogicalTypeWithLength) o;

    if (length != typewithlength.length)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(length);
  }

  @Override
  public Schema addToSchema(Schema schema) {
    super.addToSchema(schema);
    schema.addProp(LENGTH_PROP, length);
    return schema;
  }

  @Override
  public String toString() {
    return getName() + "(" + length + ")";
  }

  public static int getLengthPortion(String text) {
    // TODO: Error handling
    int i = text.indexOf('(');
    int j = text.indexOf(')');
    String l = text.substring(i + 1, j);
    return Integer.valueOf(l);
  }
}
