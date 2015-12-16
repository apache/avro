/**
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
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;

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
      byte[] bytes = value.get(new byte[value.remaining()]).array();
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

}
