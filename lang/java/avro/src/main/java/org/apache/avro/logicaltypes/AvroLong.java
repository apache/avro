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
 * Wrapper of the Avro Type.LONG
 *
 */
public class AvroLong implements AvroPrimitive {
  public static final String NAME = "LONG";
  private static final AvroLong ELEMENT = new AvroLong();
  private static final Schema SCHEMA = Schema.create(Type.LONG);

  private AvroLong() {
    super();
  }

  public static AvroLong create() {
    return ELEMENT;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Long convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof String) {
      try {
        return Long.valueOf((String) value);
      } catch (NumberFormatException e) {
        throw new AvroTypeException("Cannot convert the string \"" + value + "\" into a Long");
      }
    } else if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Long");
  }

  @Override
  public Long convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Long) {
      return (Long) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Long");
  }

  @Override
  public Type getBackingType() {
    return Type.LONG;
  }

  @Override
  public Schema getRecommendedSchema() {
    return SCHEMA;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROLONG;
  }

  @Override
  public Class<?> getConvertedType() {
    return Long.class;
  }

}
