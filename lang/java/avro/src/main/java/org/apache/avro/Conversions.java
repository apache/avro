/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;

public class Conversions {

  public static class UUIDConversion extends Conversion<UUID> {
    @Override
    public Class<UUID> getConvertedType() {
      return UUID.class;
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
    }

    @Override
    public String getLogicalTypeName() {
      return "uuid";
    }

    @Override
    public UUID fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
      return UUID.fromString(value.toString());
    }

    @Override
    public CharSequence toCharSequence(UUID value, Schema schema, LogicalType type) {
      return value.toString();
    }
  }

  public static class DecimalConversion extends Conversion<BigDecimal> {
    @Override
    public Class<BigDecimal> getConvertedType() {
      return BigDecimal.class;
    }

    @Override
    public Schema getRecommendedSchema() {
      throw new UnsupportedOperationException(
          "No recommended schema for decimal (scale is required)");
    }

    @Override
    public String getLogicalTypeName() {
      return "decimal";
    }

    @Override
    public BigDecimal fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
      int scale = ((LogicalTypes.Decimal) type).getScale();
      // always copy the bytes out because BigInteger has no offset/length ctor
      byte[] bytes = new byte[value.remaining()];
      value.get(bytes);
      return new BigDecimal(new BigInteger(bytes), scale);
    }

    @Override
    public ByteBuffer toBytes(BigDecimal value, Schema schema, LogicalType type) {
      int scale = ((LogicalTypes.Decimal) type).getScale();
      if (scale != value.scale()) {
        throw new AvroTypeException("Cannot encode decimal with scale " +
            value.scale() + " as scale " + scale);
      }
      return ByteBuffer.wrap(value.unscaledValue().toByteArray());
    }

    @Override
    public BigDecimal fromFixed(GenericFixed value, Schema schema, LogicalType type) {
      int scale = ((LogicalTypes.Decimal) type).getScale();
      return new BigDecimal(new BigInteger(value.bytes()), scale);
    }

    @Override
    public GenericFixed toFixed(BigDecimal value, Schema schema, LogicalType type) {
      int scale = ((LogicalTypes.Decimal) type).getScale();
      if (scale != value.scale()) {
        throw new AvroTypeException("Cannot encode decimal with scale " +
            value.scale() + " as scale " + scale);
      }

      byte fillByte = (byte) (value.signum() < 0 ? 0xFF : 0x00);
      byte[] unscaled = value.unscaledValue().toByteArray();
      byte[] bytes = new byte[schema.getFixedSize()];
      int offset = bytes.length - unscaled.length;

      for (int i = 0; i < bytes.length; i += 1) {
        if (i < offset) {
          bytes[i] = fillByte;
        } else {
          bytes[i] = unscaled[i - offset];
        }
      }

      return new GenericData.Fixed(schema, bytes);
    }
  }

  /**
   * Convert a underlying representation of a logical type (such as a
   * ByteBuffer) to a higher level object (such as a BigDecimal).
   * @param datum The object to be converted.
   * @param schema The schema of datum. Cannot be null if datum is not null.
   * @param type The {@link org.apache.avro.LogicalType} of datum. Cannot
   *             be null if datum is not null.
   * @param conversion The tool used to finish the conversion. Cannot
   *                   be null if datum is not null.
   * @return The result object, which is a high level object of the logical
   * type. If a null datum is passed in, a null value will be returned.
   * @throws IllegalArgumentException if a null schema, type or conversion
   * is passed in while datum is not null.
   */
  public static Object convertToLogicalType(Object datum, Schema schema, LogicalType type,
                                            Conversion<?> conversion) {
    if (datum == null) {
      return null;
    }

    if (schema == null || type == null || conversion == null) {
      throw new IllegalArgumentException("Parameters cannot be null! Parameter values:" +
          Arrays.deepToString(new Object[]{datum, schema, type, conversion}));
    }

    try {
      switch (schema.getType()) {
        case RECORD:  return conversion.fromRecord((IndexedRecord) datum, schema, type);
        case ENUM:    return conversion.fromEnumSymbol((GenericEnumSymbol) datum, schema, type);
        case ARRAY:   return conversion.fromArray((Collection) datum, schema, type);
        case MAP:     return conversion.fromMap((Map<?, ?>) datum, schema, type);
        case FIXED:   return conversion.fromFixed((GenericFixed) datum, schema, type);
        case STRING:  return conversion.fromCharSequence((CharSequence) datum, schema, type);
        case BYTES:   return conversion.fromBytes((ByteBuffer) datum, schema, type);
        case INT:     return conversion.fromInt((Integer) datum, schema, type);
        case LONG:    return conversion.fromLong((Long) datum, schema, type);
        case FLOAT:   return conversion.fromFloat((Float) datum, schema, type);
        case DOUBLE:  return conversion.fromDouble((Double) datum, schema, type);
        case BOOLEAN: return conversion.fromBoolean((Boolean) datum, schema, type);
      }
      return datum;
    } catch (ClassCastException e) {
      throw new AvroRuntimeException("Cannot convert " + datum + ":" +
          datum.getClass().getSimpleName() + ": expected generic type", e);
    }
  }

  /**
   * Convert a high level representation of a logical type (such as a BigDecimal)
   * to the its underlying representation object (such as a ByteBuffer)
   * @param datum The object to be converted.
   * @param schema The schema of datum. Cannot be null if datum is not null.
   * @param type The {@link org.apache.avro.LogicalType} of datum. Cannot
   *             be null if datum is not null.
   * @param conversion The tool used to finish the conversion. Cannot
   *                   be null if datum is not null.
   * @return The result object, which is an underlying representation object
   * of the logical type. If the input param datum is null, a null value will
   * be returned.
   * @throws IllegalArgumentException if a null schema, type or conversion
   * is passed in while datum is not null.
   */
  public static <T> Object convertToRawType(Object datum, Schema schema, LogicalType type,
                                            Conversion<T> conversion) {
    if (datum == null) {
      return null;
    }

    if (schema == null || type == null || conversion == null) {
      throw new IllegalArgumentException("Parameters cannot be null! Parameter values:" +
          Arrays.deepToString(new Object[]{datum, schema, type, conversion}));
    }

    try {
      Class<T> fromClass = conversion.getConvertedType();
      switch (schema.getType()) {
        case RECORD:
          return conversion.toRecord(fromClass.cast(datum), schema, type);
        case ENUM:
          return conversion.toEnumSymbol(fromClass.cast(datum), schema, type);
        case ARRAY:
          return conversion.toArray(fromClass.cast(datum), schema, type);
        case MAP:
          return conversion.toMap(fromClass.cast(datum), schema, type);
        case FIXED:
          return conversion.toFixed(fromClass.cast(datum), schema, type);
        case STRING:
          return conversion.toCharSequence(fromClass.cast(datum), schema, type);
        case BYTES:
          return conversion.toBytes(fromClass.cast(datum), schema, type);
        case INT:
          return conversion.toInt(fromClass.cast(datum), schema, type);
        case LONG:
          return conversion.toLong(fromClass.cast(datum), schema, type);
        case FLOAT:
          return conversion.toFloat(fromClass.cast(datum), schema, type);
        case DOUBLE:
          return conversion.toDouble(fromClass.cast(datum), schema, type);
        case BOOLEAN:
          return conversion.toBoolean(fromClass.cast(datum), schema, type);
      }
      return datum;
    } catch (ClassCastException e) {
      throw new AvroRuntimeException("Cannot convert " + datum + ":" +
          datum.getClass().getSimpleName() + ": expected logical type", e);
    }
  }

}
