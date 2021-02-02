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

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper of the Avro Type.INT
 *
 */
public class AvroInt implements AvroPrimitive {
  public static final String NAME = "INT";
  private static final AvroInt element = new AvroInt();
  private static final Schema schema = Schema.create(Type.INT);

  private AvroInt() {
    super();
  }

  public static AvroInt create() {
    return element;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Integer convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof String) {
      try {
        return Integer.valueOf((String) value);
      } catch (NumberFormatException e) {
        throw new AvroTypeException("Cannot convert the string \"" + value + "\" into a Integer");
      }
    } else if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Integer");
  }

  @Override
  public Integer convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Integer) {
      return (Integer) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Integer");
  }

  @Override
  public Type getBackingType() {
    return Type.INT;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROINT;
  }

  @Override
  public Class<?> getConvertedType() {
    return Integer.class;
  }

}
