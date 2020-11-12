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
import org.apache.avro.LogicalTypes.TimeMicros;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.data.TimeConversions.TimeMicrosConversion;

/**
 * Wrapper of LogicalTypes.timeMicros()
 *
 */
public class AvroTimeMicros extends TimeMicros implements AvroPrimitive {
  private static Schema schema;
  private static AvroTimeMicros element = new AvroTimeMicros();
  public static final String NAME = "TIMEMICROS";
  public static final String TYPENAME = LogicalTypes.TIME_MICROS;
  public static final TimeMicrosConversion CONVERTER = new TimeMicrosConversion();

  static {
    schema = element.addToSchema(Schema.create(Type.LONG));
  }

  public AvroTimeMicros() {
    super();
  }

  public static AvroTimeMicros create() {
    return element;
  }

  @Override
  public Schema addToSchema(Schema schema) {
    return super.addToSchema(schema);
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
    } else if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof Instant) {
      Instant t = (Instant) value;
      return convertToRawType(t.atOffset(ZoneOffset.UTC).toLocalTime());
    } else if (value instanceof LocalTime) {
      return CONVERTER.toLong((LocalTime) value, null, this);
    } else if (value instanceof Date) {
      return convertToRawType(((Date) value).toInstant());
    } else if (value instanceof CharSequence) {
      return convertToRawType(LocalTime.parse((CharSequence) value));
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a TimeMicros");
  }

  @Override
  public LocalTime convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Long) {
      return CONVERTER.fromLong((Long) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a TimeMicros");
  }

  @Override
  public void toString(StringBuffer b, Object value) {
    if (value != null) {
      if (value instanceof Long) {
        LocalTime time = CONVERTER.fromLong((Long) value, schema, null);
        b.append('\"');
        b.append(time.toString());
        b.append('\"');
      }
    }
  }

  @Override
  public Type getBackingType() {
    return Type.LONG;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROTIMEMICROS;
  }

  @Override
  public Class<?> getConvertedType() {
    return LocalTime.class;
  }

}
