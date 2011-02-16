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
package org.apache.avro.io;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder.BufferAccessor;

/** Utilities for binary-encoded data. */
public class BinaryData {

  private BinaryData() {}                      // no public ctor

  private static class Decoders {
     private final BufferAccessor b1, b2;
     private final BinaryDecoder d1, d2;
    public Decoders() {
       this.d1 = new BinaryDecoder(new byte[0], 0, 0);
       this.d2 = new BinaryDecoder(new byte[0], 0, 0);
       this.b1 = d1.getBufferAccessor();
       this.b2 = d2.getBufferAccessor();
    }
     public void set(byte[] data1, int off1, int len1, 
         byte[] data2, int off2, int len2) {
       this.d1.init(data1, off1, len1);
       this.d2.init(data2, off2, len2);
  }
  }                     // no public ctor

  private static final ThreadLocal<Decoders> DECODERS
    = new ThreadLocal<Decoders>() {
    @Override protected Decoders initialValue() { return new Decoders(); }
  };

  /** Compare binary encoded data.  If equal, return zero.  If greater-than,
   * return 1, if less than return -1. Order is consistent with that of {@link
   * org.apache.avro.generic.GenericData#compare(Object, Object, Schema)}.*/
  public static int compare(byte[] b1, int s1,
                            byte[] b2, int s2,
                            Schema schema) {
    return compare(b1, s1, b1.length - s1, b2, s2, b2.length - s2, schema);
  }

  /** Compare binary encoded data.  If equal, return zero.  If greater-than,
   * return 1, if less than return -1. Order is consistent with that of {@link
   * org.apache.avro.generic.GenericData#compare(Object, Object, Schema)}.*/
  public static int compare(byte[] b1, int s1, int l1,
                            byte[] b2, int s2, int l2,
                            Schema schema) {
    Decoders decoders = DECODERS.get();
    decoders.set(b1, s1, l1, b2, s2, l2);
    try {
      return compare(decoders, schema);
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  /** If equal, return the number of bytes consumed.  If greater than, return
   * GT, if less than, return LT. */
  private static int compare(Decoders d, Schema schema) throws IOException {
    Decoder d1 = d.d1; Decoder d2 = d.d2;
    switch (schema.getType()) {
    case RECORD: {
      for (Field field : schema.getFields()) {
        if (field.order() == Field.Order.IGNORE) {
          GenericDatumReader.skip(field.schema(), d1);
          GenericDatumReader.skip(field.schema(), d2);
          continue;
        }
        int c = compare(d, field.schema());
        if (c != 0)
          return (field.order() != Field.Order.DESCENDING) ? c : -c;
      }
      return 0;
    }
    case ENUM: case INT: {
      int i1 = d1.readInt();
      int i2 = d2.readInt();
      return i1 == i2 ? 0 : (i1 > i2 ? 1 : -1);
    }
    case LONG: {
      long l1 = d1.readLong();
      long l2 = d2.readLong();
      return l1 == l2 ? 0 : (l1 > l2 ? 1 : -1);
    }
    case ARRAY: {
      long i = 0;                                 // position in array
      long r1 = 0, r2 = 0;                        // remaining in current block
      long l1 = 0, l2 = 0;                        // total array length
      while (true) {
        if (r1 == 0) {                            // refill blocks(s)
          r1 = d1.readLong();
          if (r1 < 0) { r1 = -r1; d1.readLong(); }
          l1 += r1;
        }
        if (r2 == 0) {
          r2 = d2.readLong();
          if (r2 < 0) { r2 = -r2; d2.readLong(); }
          l2 += r2;
        }
        if (r1 == 0 || r2 == 0)                   // empty block: done
          return (l1 == l2) ? 0 : ((l1 > l2) ? 1 : -1);
        long l = Math.min(l1, l2);
        while (i < l) {                           // compare to end of block
          int c = compare(d, schema.getElementType());
          if (c != 0) return c;
          i++; r1--; r2--;
        }
      }
    }
    case MAP:
      throw new AvroRuntimeException("Can't compare maps!");
    case UNION: {
      int i1 = d1.readInt();
      int i2 = d2.readInt();
      if (i1 == i2) {
        return compare(d, schema.getTypes().get(i1));
      } else {
        return i1 - i2;
      }
    }
    case FIXED: {
      int size = schema.getFixedSize();
      int c = compareBytes(d.b1.getBuf(), d.b1.getPos(), size,
                           d.b2.getBuf(), d.b2.getPos(), size);
      d.d1.skipFixed(size);
      d.d2.skipFixed(size);
      return c;
    }
    case STRING: case BYTES: {
      int l1 = d1.readInt();
      int l2 = d2.readInt();
      int c = compareBytes(d.b1.getBuf(), d.b1.getPos(), l1,
                           d.b2.getBuf(), d.b2.getPos(), l2);
      d.d1.skipFixed(l1);
      d.d2.skipFixed(l2);
      return c;
    }
    case FLOAT: {
      float f1 = d1.readFloat();
      float f2 = d2.readFloat();
      return (f1 == f2) ? 0 : ((f1 > f2) ? 1 : -1);
    }
    case DOUBLE: {
      double f1 = d1.readDouble();
      double f2 = d2.readDouble();
      return (f1 == f2) ? 0 : ((f1 > f2) ? 1 : -1);
    }
    case BOOLEAN:
      boolean b1 = d1.readBoolean();
      boolean b2 = d2.readBoolean();
      return (b1 == b2) ? 0 : (b1 ? 1 : -1);
    case NULL:
      return 0;
    default:
      throw new AvroRuntimeException("Unexpected schema to compare!");
    }
  }

  /** Lexicographically compare bytes.  If equal, return zero.  If greater-than,
   * return a positive value, if less than return a negative value. */
  public static int compareBytes(byte[] b1, int s1, int l1,
                                 byte[] b2, int s2, int l2) {
    int end1 = s1 + l1;
    int end2 = s2 + l2;
    for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
      int a = (b1[i] & 0xff);
      int b = (b2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return l1 - l2;
  }

  private static class HashData {
    private final BufferAccessor bytes;
    private final BinaryDecoder decoder;
    public HashData() {
      this.decoder = new BinaryDecoder(new byte[0], 0, 0);
      this.bytes = decoder.getBufferAccessor();
    }
    public void set(byte[] bytes, int start, int len) {
      this.decoder.init(bytes, start, len);
    }
  }

  private static final ThreadLocal<HashData> HASH_DATA
    = new ThreadLocal<HashData>() {
    @Override protected HashData initialValue() { return new HashData(); }
  };

  /** Hash binary encoded data. Consistent with {@link
   * org.apache.avro.generic.GenericData#hashCode(Object, Schema)}.*/
  public static int hashCode(byte[] bytes, int start, int length,
                             Schema schema) {
    HashData data = HASH_DATA.get();
    data.set(bytes, start, length);
    try {
      return hashCode(data, schema);
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  private static int hashCode(HashData data, Schema schema)
    throws IOException {
    Decoder decoder = data.decoder;
    switch (schema.getType()) {
    case RECORD: {
      int hashCode = 1;
      for (Field field : schema.getFields()) {
        if (field.order() == Field.Order.IGNORE) {
          GenericDatumReader.skip(field.schema(), decoder);
          continue;
        }
        hashCode = hashCode*31 + hashCode(data, field.schema());
      }
      return hashCode;
    }
    case ENUM: case INT:
      return decoder.readInt();
    case FLOAT:
      return Float.floatToIntBits(decoder.readFloat());
    case LONG: {
      long l = decoder.readLong();
      return (int)(l^(l>>>32));
    }
    case DOUBLE: {
      long l = Double.doubleToLongBits(decoder.readDouble());
      return (int)(l^(l>>>32));
    }
    case ARRAY: {
      Schema elementType = schema.getElementType();
      int hashCode = 1;
      for (long l = decoder.readArrayStart(); l != 0; l = decoder.arrayNext())
        for (long i = 0; i < l; i++)
          hashCode = hashCode*31 + hashCode(data, elementType);
      return hashCode;
    }
    case MAP:
      throw new AvroRuntimeException("Can't hashCode maps!");
    case UNION:
      return hashCode(data, schema.getTypes().get(decoder.readInt()));
    case FIXED:
      return hashBytes(1, data, schema.getFixedSize(), false);
    case STRING:
      return hashBytes(0, data, decoder.readInt(), false);
    case BYTES:
      return hashBytes(1, data, decoder.readInt(), true);
    case BOOLEAN:
      return decoder.readBoolean() ? 1231 : 1237;
    case NULL:
      return 0;
    default:
      throw new AvroRuntimeException("Unexpected schema to hashCode!");
    }
  }

  private static int hashBytes(int init, HashData data, int len, boolean rev)
    throws IOException {
    int hashCode = init;
    byte[] bytes = data.bytes.getBuf();
    int start = data.bytes.getPos();
    int end = start+len;
    if (rev) 
      for (int i = end-1; i >= start; i--)
        hashCode = hashCode*31 + bytes[i];
    else
      for (int i = start; i < end; i++)
        hashCode = hashCode*31 + bytes[i];
    data.decoder.skipFixed(len);
    return hashCode;
  }

  /** Skip a binary-encoded long, returning the position after it. */
  public static int skipLong(byte[] bytes, int start) {
    int i = start;
    for (int b = bytes[i++]; ((b & 0x80) != 0); b = bytes[i++]) {}
    return i;
  }

}
