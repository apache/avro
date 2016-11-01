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
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
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
      Schema.Type type1 = schema.getType();
      // validate the type
      if (type1 != Schema.Type.BYTES &&
          type1 != Schema.Type.STRING) {
        throw new IllegalArgumentException(this.logicalTypeName + " must be backed by fixed or bytes, not" + type1);
      }
      int precision = mc.getPrecision();
      if (precision > maxPrecision(schema)) {
        throw new IllegalArgumentException("Invalid precision " + precision);
      }
   }

    public static boolean is(final Schema schema) {
      Schema.Type type1 = schema.getType();
      // validate the type
      if (type1 != Schema.Type.BYTES && type1 != Schema.Type.STRING) {
        return false;
      }
      LogicalType logicalType = schema.getLogicalType();
      if (logicalType == null) {
        return false;
      }
      return logicalType.getClass() == Decimal.class;
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
          BigDecimal result = new BigDecimal(object.toString(), mc);
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
          int lscale = readInt(buf);
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
          if (decimal.scale() > scale) {
            decimal = decimal.setScale(scale, RoundingMode.HALF_DOWN);
          }
          ByteBuffer buf = toBytes(decimal);
          return buf;
        default:
          throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
      }
    }

  public static ByteBuffer toBytes(BigDecimal decimal) {
    byte[] unscaledValue = decimal.unscaledValue().toByteArray();
    ByteBuffer buf = ByteBuffer.allocate(5 + unscaledValue.length);
    writeInt(decimal.scale(), buf);
    buf.put(unscaledValue);
    buf.flip();
    return buf;
  }

  public static int readInt(final ByteBuffer buf) {
    int n = 0;
    int b;
    int shift = 0;
    do {
      b = buf.get() & 0xff;
      n |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return (n >>> 1) ^ -(n & 1); // back to two's-complement
      }
      shift += 7;
    } while (shift < 32);
    throw new RuntimeException("Invalid int encoding" + buf);
  }

  public static void writeInt(final int n, final ByteBuffer buf) {
    int val = (n << 1) ^ (n >> 31);
    if ((val & ~0x7F) == 0) {
      buf.put((byte) val);
      return;
    } else if ((val & ~0x3FFF) == 0) {
      buf.put((byte) (0x80 | val));
      buf.put((byte) (val >>> 7));
      return;
    }
    byte [] tmp = new byte[5];
    int len = BinaryData.encodeInt(n, tmp, 0);
    buf.put(tmp, 0, len);
  }


}
