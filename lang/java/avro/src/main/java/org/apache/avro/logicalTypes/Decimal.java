/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.logicalTypes;

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Set;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

  /** Decimal represents arbitrary-precision fixed-scale decimal numbers  */
public final class Decimal extends AbstractLogicalType {

    private static final Set<String> RESERVED = AbstractLogicalType.reservedSet("precision", "scale");

    private final MathContext mc;
    private final int scale;

    public Decimal(int precision, int scale, Schema.Type type) {
      super(type, RESERVED, "decimal", ImmutableMap.of("precision", (Object) precision, "scale", scale));
      if (precision <= 0) {
        throw new IllegalArgumentException("Invalid " + this.logicalTypeName + " precision: " +
            precision + " (must be positive)");
      }
      if (scale < 0) {
        throw new IllegalArgumentException("Invalid " + this.logicalTypeName + " scale: " +
            scale + " (must be positive)");
      } else if (scale > precision) {
        throw new IllegalArgumentException("Invalid " + this.logicalTypeName + " scale: " +
            scale + " (greater than precision: " + precision + ")");
      }
      mc = new MathContext(precision, RoundingMode.HALF_EVEN);
      this.scale = scale;
    }

    public Decimal(JsonNode node, Schema.Type type) {
        this(node.get("precision").asInt(), node.get("scale").asInt(), type);
    }

    @Override
    public void validate(Schema schema) {
      // validate the type
      if (schema.getType() != Schema.Type.FIXED &&
          schema.getType() != Schema.Type.BYTES &&
          schema.getType() != Schema.Type.STRING) {
        throw new IllegalArgumentException(this.logicalTypeName + " must be backed by fixed or bytes");
      }
      int precision = mc.getPrecision();
      if (precision > maxPrecision(schema)) {
        throw new IllegalArgumentException(
            "fixed(" + schema.getFixedSize() + ") cannot store " +
                precision + " digits (max " + maxPrecision(schema) + ")");
      }
   }

    @Override
    public Set<String> reserved() {
      return RESERVED;
    }

    private long maxPrecision(Schema schema) {
      if (schema.getType() == Schema.Type.BYTES
              || schema.getType() == Schema.Type.STRING) {
        // not bounded
        return Integer.MAX_VALUE;
      } else if (schema.getType() == Schema.Type.FIXED) {
        int size = schema.getFixedSize();
        return Math.round(          // convert double to long
            Math.floor(Math.log10(  // number of base-10 digits
                Math.pow(2, 8 * size - 1) - 1)  // max value stored
            ));
      } else {
        // not valid for any other type
        return 0;
      }
    }

    @Override
    public Class<?> getLogicalJavaType() {
        return BigDecimal.class;
    }

    @Override
    public Object deserialize(Object object) {
      switch (type) {
        case STRING:
          BigDecimal result = new BigDecimal(((CharSequence) object).toString(), mc);
          if (result.scale() > scale) {
                      // Rounding might be an option.
            // this will probably need to be made configurable in the future.
            throw new AvroRuntimeException("Received Decimal " + object + " is not compatible with scale " + scale);
          }
          return result;
        case BYTES:
          //ByteBuffer buf = ByteBuffer.wrap((byte []) object);
          ByteBuffer buf = (ByteBuffer) object;
          buf.rewind();
          int lscale = buf.getInt();
          if (lscale > scale) {
                      // Rounding might be an option.
            // this will probably need to be made configurable in the future.
            throw new AvroRuntimeException("Received Decimal " + object + " is not compatible with scale " + scale);
          }
          byte[] unscaled = new byte[buf.remaining()];
          buf.get(unscaled);
          BigInteger unscaledBi = new BigInteger(unscaled);
          return new BigDecimal(unscaledBi, lscale);
        default:
          throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
      }

    }

    @Override
    public Object serialize(Object object) {
      BigDecimal decimal = (BigDecimal) object;
      switch (type) {
        case STRING:
          if (decimal.scale() > scale) {
            return decimal.setScale(scale, RoundingMode.HALF_DOWN).toPlainString();
          } else {
            return decimal.toPlainString();
          }
        case BYTES:
          int lscale;
          if (decimal.scale() > scale) {
            lscale = scale;
            decimal = decimal.setScale(lscale, RoundingMode.HALF_DOWN);
          } else {
            lscale = decimal.scale();
          }
          byte[] unscaledValue = decimal.unscaledValue().toByteArray();
          ByteBuffer buf = ByteBuffer.allocate(4 + unscaledValue.length);
          buf.putInt(lscale);
          buf.put(unscaledValue);
          buf.rewind();
          return buf;
        default:
          throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
      }
    }
  }
