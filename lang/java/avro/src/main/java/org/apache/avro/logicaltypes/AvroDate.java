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
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Date;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.data.TimeConversions.DateConversion;

/**
 * Based on a Avro Type.INT holds the date portion without time.
 *
 */
public class AvroDate extends LogicalTypes.Date implements AvroPrimitive {
  public static final String NAME = "DATE";
  public static final String TYPENAME = LogicalTypes.DATE;
  private static Schema schema;
  private static AvroDate element = new AvroDate();
  private DateConversion converter = new DateConversion();

  static {
    schema = element.addToSchema(Schema.create(Type.INT));
  }

  public AvroDate() {
    super();
  }

  public static AvroDate create() {
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
    } else if (value instanceof LocalDate) {
      return converter.toInt((LocalDate) value, null, this);
    } else if (value instanceof Number) {
      return convertToRawType(((Number) value).intValue());
    } else if (value instanceof Date) {
      return convertToRawType(((Date) value).toInstant());
    } else if (value instanceof ZonedDateTime) {
      return convertToRawType(((ZonedDateTime) value).toInstant());
    } else if (value instanceof Instant) {
      Instant d = (Instant) value;
      return (int) d.getLong(ChronoField.EPOCH_DAY);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Date");
  }

  @Override
  public LocalDate convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Integer) {
      return converter.fromInt((Integer) value, null, this);
    } else if (value instanceof Number) {
      int v = ((Number) value).intValue();
      return converter.fromInt(v, null, null);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Date");
  }

  @Override
  public void toString(StringBuffer b, Object value) {
    if (value != null) {
      if (value instanceof Integer || value instanceof Long) {
        LocalDate date = convertToLogicalType(value);
        b.append('\"');
        b.append(date.toString());
        b.append('\"');
      }
    }
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
    return AvroType.AVRODATE;
  }

  @Override
  public Class<?> getConvertedType() {
    return LocalDate.class;
  }

}
