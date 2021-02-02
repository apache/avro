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

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.TimeMillis;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.data.TimeConversions.TimeMillisConversion;

/**
 * Wrapper of LogicalTypes.timeMillis()
 *
 */
public class AvroTimeMillis extends TimeMillis implements AvroPrimitive {
  private static final Schema schema;
  private static final AvroTimeMillis element = new AvroTimeMillis();
  public static final String NAME = "TIME";
  public static final String TYPENAME = LogicalTypes.TIME_MILLIS;
  private static final TimeMillisConversion CONVERTER = new TimeMillisConversion();

  static {
    schema = element.addToSchema(Schema.create(Type.INT));
  }

  private AvroTimeMillis() {
    super();
  }

  public static AvroTimeMillis create() {
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
    } else if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof Instant) {
      Instant t = (Instant) value;
      return convertToRawType(t.atOffset(ZoneOffset.UTC).toLocalTime());
    } else if (value instanceof Date) {
      return convertToRawType(((Date) value).toInstant());
    } else if (value instanceof CharSequence) {
      return convertToRawType(LocalTime.parse((CharSequence) value));
    } else if (value instanceof LocalTime) {
      return CONVERTER.toInt((LocalTime) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Time");
  }

  @Override
  public LocalTime convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Integer) {
      return CONVERTER.fromInt((Integer) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Time");
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
    return AvroType.AVROTIMEMILLIS;
  }

  @Override
  public Class<?> getConvertedType() {
    return Integer.class;
  }

}
