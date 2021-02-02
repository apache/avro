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
 * Wrapper of the Avro Type.FLOAT, a 32 bit IEEE 754 floating-point number.
 *
 */
public class AvroFloat implements AvroPrimitive {
  public static final String NAME = "FLOAT";
  private static final AvroFloat ELEMENT = new AvroFloat();
  private static final Schema SCHEMA = Schema.create(Type.FLOAT);

  private AvroFloat() {
    super();
  }

  public static AvroFloat create() {
    return ELEMENT;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Float convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Float) {
      return (Float) value;
    } else if (value instanceof String) {
      try {
        return Float.valueOf((String) value);
      } catch (NumberFormatException e) {
        throw new AvroTypeException("Cannot convert the string \"" + value + "\" into a Float");
      }
    } else if (value instanceof Number) {
      return ((Number) value).floatValue();
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Float");
  }

  @Override
  public Float convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Float) {
      return (Float) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Float");
  }

  @Override
  public Type getBackingType() {
    return Type.FLOAT;
  }

  @Override
  public Schema getRecommendedSchema() {
    return SCHEMA;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROFLOAT;
  }

  @Override
  public Class<?> getConvertedType() {
    return Float.class;
  }

}
