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
import java.time.ZonedDateTime;
import java.util.Date;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.TimestampMillis;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.data.TimeConversions.TimestampMillisConversion;

/**
 * Wrapper of LogicalTypes.TimestampMillis. A timestamp is a point in time in
 * UTC timezone, an Instant in Java.
 *
 */
public class AvroTimestampMillis extends TimestampMillis implements AvroPrimitive {
  private static final Schema SCHEMA;
  private static final AvroTimestampMillis ELEMENT = new AvroTimestampMillis();
  static {
    SCHEMA = ELEMENT.addToSchema(Schema.create(Type.LONG));
  }
  public static final String NAME = "TIMESTAMPMILLIS";
  public static final String TYPENAME = LogicalTypes.TIMESTAMP_MILLIS;
  public static final TimestampMillisConversion CONVERTER = new TimestampMillisConversion();

  private AvroTimestampMillis() {
    super();
  }

  public static AvroTimestampMillis create() {
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
    } else if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof Date) {
      return convertToRawType(((Date) value).toInstant());
    } else if (value instanceof ZonedDateTime) {
      return convertToRawType(((ZonedDateTime) value).toInstant());
    } else if (value instanceof CharSequence) {
      return convertToRawType(Instant.parse((CharSequence) value));
    } else if (value instanceof Instant) {
      return CONVERTER.toLong((Instant) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Timestamp");
  }

  @Override
  public Instant convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Long) {
      return CONVERTER.fromLong((Long) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Timestamp");
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
    return AvroType.AVROTIMESTAMPMILLIS;
  }

  @Override
  public Class<?> getConvertedType() {
    return Instant.class;
  }

}
