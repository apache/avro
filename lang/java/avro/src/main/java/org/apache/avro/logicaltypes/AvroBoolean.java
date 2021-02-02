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
 * Wrapper for the Avro BOOLEAN type.
 */
public class AvroBoolean implements AvroPrimitive {
  public static final String NAME = "BOOLEAN";
  private static final AvroBoolean element = new AvroBoolean();
  private static final Schema schema = Schema.create(Type.BOOLEAN);

  private AvroBoolean() {
    super();
  }

  public static AvroBoolean create() {
    return element;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Boolean convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof String) {
      if ("TRUE".equalsIgnoreCase((String) value)) {
        return Boolean.TRUE;
      } else if ("FALSE".equalsIgnoreCase((String) value)) {
        return Boolean.FALSE;
      }
    } else if (value instanceof Number) {
      int v = ((Number) value).intValue();
      if (v == 1) {
        return Boolean.TRUE;
      } else if (v == 0) {
        return Boolean.FALSE;
      }
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Boolean");
  }

  @Override
  public Boolean convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Boolean) {
      return (Boolean) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Boolean");
  }

  @Override
  public Type getBackingType() {
    return Type.BOOLEAN;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROBOOLEAN;
  }

  @Override
  public Class<?> getConvertedType() {
    return Boolean.class;
  }

}
