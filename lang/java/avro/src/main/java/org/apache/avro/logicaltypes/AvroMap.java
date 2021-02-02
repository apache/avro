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

import java.util.Map;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper around the Avro Type.MAP data type
 *
 */
public class AvroMap implements AvroPrimitive {
  public static final String NAME = "MAP";
  private static final AvroMap element = new AvroMap();

  public static AvroMap create() {
    return element;
  }

  private AvroMap() {
    super();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    return true;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Map<?, ?> convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Map) {
      return (Map<?, ?>) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a GenericFixed");
  }

  @Override
  public Map<?, ?> convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Map) {
      return (Map<?, ?>) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a GenericFixed");
  }

  @Override
  public Type getBackingType() {
    return Type.MAP;
  }

  @Override
  public Schema getRecommendedSchema() {
    return null;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROMAP;
  }

  @Override
  public Class<?> getConvertedType() {
    return Map.class;
  }

}
