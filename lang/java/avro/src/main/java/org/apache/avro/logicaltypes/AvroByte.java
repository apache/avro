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
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Based on an INT but is supposed to hold data from -127 to +128 only. A single
 * signed byte.
 *
 */

public class AvroByte extends LogicalType implements AvroPrimitive {
  public static final String NAME = "BYTE";
  public static final String TYPENAME = NAME;
  private static final AvroByte ELEMENT = new AvroByte();
  private static final Schema SCHEMA;

  static {
    SCHEMA = ELEMENT.addToSchema(Schema.create(Type.INT));
  }

  private AvroByte() {
    super(TYPENAME);
  }

  public static AvroByte create() {
    return ELEMENT;
  }

  @Override
  public void validate(Schema schema) {
    super.validate(schema);
    // validate the type
    if (schema.getType() != Schema.Type.INT) {
      throw new IllegalArgumentException("Logical type must be backed by an integer");
    }
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
        throw new AvroTypeException("Cannot convert the string \"" + value + "\" into an Integer");
      }
    } else if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into an Integer");
  }

  @Override
  public Type getBackingType() {
    return Type.INT;
  }

  @Override
  public Schema getRecommendedSchema() {
    return SCHEMA;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROBYTE;
  }

  @Override
  public Byte convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Byte) {
      return (Byte) value;
    } else if (value instanceof Integer) {
      return ((Integer) value).byteValue();
    } else if (value instanceof Number) {
      return ((Number) value).byteValue();
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Byte");
  }

  @Override
  public Class<?> getConvertedType() {
    return Integer.class;
  }

}
