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
import org.apache.avro.Schema.Type;

/**
 * Avro Type.STRING backed datatype for URI/URL data
 *
 */
public class AvroUri extends LogicalType implements AvroPrimitive {
  public static final String NAME = "URI";
  public static final String TYPENAME = NAME;
  private static final AvroUri element = new AvroUri();
  private static final Schema schema;

  static {
    schema = element.addToSchema(Schema.create(Type.STRING));
  }

  private AvroUri() {
    super(TYPENAME);
  }

  public static AvroUri create() {
    return element;
  }

  @Override
  public void validate(Schema schema) {
    super.validate(schema);
    // validate the type
    if (schema.getType() != Schema.Type.STRING) {
      throw new IllegalArgumentException("Logical type varchar must be backed by string");
    }
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public CharSequence convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof CharSequence) {
      return (CharSequence) value;
    } else {
      return value.toString();
    }
  }

  @Override
  public CharSequence convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof CharSequence) {
      return (CharSequence) value;
    } else {
      return value.toString();
    }
  }

  @Override
  public Type getBackingType() {
    return Type.STRING;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROURI;
  }

  @Override
  public Class<?> getConvertedType() {
    return CharSequence.class;
  }

}
