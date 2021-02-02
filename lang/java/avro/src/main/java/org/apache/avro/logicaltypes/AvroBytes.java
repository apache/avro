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

import java.nio.ByteBuffer;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Is the Avro Type.BYTES datatype, a binary store of any length. A BLOB column.
 *
 */
public class AvroBytes implements AvroPrimitive {
  public static final String NAME = "BYTES";
  private static final AvroBytes element = new AvroBytes();
  private static final Schema schema = Schema.create(Type.BYTES);

  private AvroBytes() {
    super();
  }

  public static AvroBytes create() {
    return element;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public ByteBuffer convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof ByteBuffer) {
      return (ByteBuffer) value;
    } else if (value instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) value);
    } else if (value instanceof Number) {
      byte[] b = new byte[1];
      b[0] = ((Number) value).byteValue();
      return ByteBuffer.wrap(b);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a ByteBuffer");
  }

  @Override
  public ByteBuffer convertToLogicalType(Object value) {
    return (ByteBuffer) value;
  }

  @Override
  public Type getBackingType() {
    return Type.BYTES;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROBYTES;
  }

  @Override
  public Class<?> getConvertedType() {
    return ByteBuffer.class;
  }

}
