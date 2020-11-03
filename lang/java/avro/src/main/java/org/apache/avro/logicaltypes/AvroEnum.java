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
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericEnumSymbol;

/**
 * Wrapper around the Avro Type.ENUM data type
 *
 */
public class AvroEnum implements AvroPrimitive {
  public static final String NAME = "ENUM";
  private Schema schema;

  public AvroEnum(Schema schema) {
    super();
    this.schema = schema;
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
  public int hashCode() {
    return 1;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public GenericEnumSymbol<?> convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof GenericEnumSymbol) {
      return (GenericEnumSymbol<?>) value;
    } else {
      return new EnumSymbol(schema, value);
    }
  }

  @Override
  public GenericEnumSymbol<?> convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof GenericEnumSymbol) {
      return (GenericEnumSymbol<?>) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a GenericEnumSymbol");
  }

  @Override
  public void toString(StringBuffer b, Object value) {
    if (value != null) {
      b.append('\"');
      b.append(value.toString());
      b.append('\"');
    }
  }

  @Override
  public Type getBackingType() {
    return Type.ENUM;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROENUM;
  }

  @Override
  public Class<?> getConvertedType() {
    return GenericEnumSymbol.class;
  }

}
