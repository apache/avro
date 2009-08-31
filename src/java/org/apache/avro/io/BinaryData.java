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

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;

/** Utilities for binary-encoded data. */
public class BinaryData {

  private BinaryData() {}                      // no public ctor

  private static final int GT = -1;
  private static final int LT = -2;

  /** Compare binary encoded data.  If equal, return zero.  If greater-than,
   * return 1, if less than return -1. */
  public static int compare(byte[] b1, int s1,
                            byte[] b2, int s2,
                            Schema schema) {
    int comp = comp(b1, s1, b2, s2, schema);          // compare
    return (comp >= 0) ? 0 : ((comp == GT) ? 1 : -1); // decode comparison
  }
   
  /** If equal, return the number of bytes consumed.  If greater than, return
   * GT, if less than, return LT. */
  private static int comp(byte[] b1, int s1, byte[] b2, int s2, Schema schema) {
    switch (schema.getType()) {
    case RECORD: {
      int size = 0;
      for (Map.Entry<String, Schema> entry : schema.getFieldSchemas()) {
        int comp = comp(b1, s1+size, b2, s2+size, entry.getValue());
        if (comp < 0) return comp;
        size += comp;
      }
      return size;
    }
    case ENUM: case INT: case LONG: {
      long l1 = readLong(b1, s1);
      long l2 = readLong(b2, s2);
      return (l1 == l2) ? longSize(l1) : ((l1 > l2) ? GT : LT);
    }
    case ARRAY: {
      long i = 0;                                 // position in array
      long r1 = 0, r2 = 0;                        // remaining in current block
      long l1 = 0, l2 = 0;                        // total array length
      int size1 = 0, size2 = 0;                   // total array size
      while (true) {
        if (r1 == 0) {                            // refill blocks(s)
          r1 = readLong(b1, s1+size1);
          size1 += longSize(r1);
          l1 += r1;
        }
        if (r2 == 0) {
          r2 = readLong(b2, s2+size2);
          size2 += longSize(r2);
          l2 += r2;
        }
        if (r1 == 0 || r2 == 0)                   // empty block: done
          return (l1 == l2) ? size1 : ((l1 > l2) ? GT : LT);
        long l = Math.min(l1, l2);
        while (i < l) {                           // compare to end of block
          int comp = comp(b1, s1+size1, b2, s2+size2, schema.getElementType());
          if (comp < 0) return comp;
          size1 += comp;
          size2 += comp;
          i++; r1--; r2--;
        }
      }
      }
    case MAP:
      throw new AvroRuntimeException("Can't compare maps!");
    case UNION: {
      int i1 = readInt(b1, s1);
      int i2 = readInt(b2, s2);
      if (i1 == i2) {
        int size = intSize(i1);
        return comp(b1, s1+size, b2, s2+size, schema.getTypes().get(i1));
      } else {
        return (i1 > i2) ? GT : LT;
      }
    }
    case FIXED: {
      int size = schema.getFixedSize();
      int c = compareBytes(b1, s1, size, b2, s2, size);
      return (c == 0) ? size : ((c > 0) ? GT : LT);
    }
    case STRING: case BYTES: {
      int l1 = readInt(b1, s1);
      int l2 = readInt(b2, s2);
      int size1 = intSize(l1);
      int size2 = intSize(l2);
      int c = compareBytes(b1, s1+size1, l1, b2, s2+size2, l2);
      return (c == 0) ? size1+l1 : ((c > 0) ? GT : LT);
    }
    case FLOAT: {
      int n1 = 0, n2 = 0;
      for (int i = 0, shift = 0; i < 4; i++, shift += 8) {
        n1 |= (b1[s1+i] & 0xff) << shift;
        n2 |= (b2[s2+i] & 0xff) << shift;
      }
      float f1 = Float.intBitsToFloat(n1);
      float f2 = Float.intBitsToFloat(n2);
      return (f1 == f2) ? 4 : ((f1 > f2) ? GT : LT);
    }
    case DOUBLE: {
      long n1 = 0, n2 = 0;
      for (int i = 0, shift = 0; i < 8; i++, shift += 8) {
        n1 |= (b1[s1+i] & 0xffL) << shift;
        n2 |= (b2[s2+i] & 0xffL) << shift;
      }
      double d1 = Double.longBitsToDouble(n1);
      double d2 = Double.longBitsToDouble(n2);
      return (d1 == d2) ? 8 : ((d1 > d2) ? GT : LT);
    }
    case BOOLEAN:
      return b1[s1] == b2[s2] ? 1 : ((b1[s1] > b2[s2]) ? GT : LT);
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

  private static int readInt(byte[] b, int s) {
    long l = readLong(b, s);
    if (l < Integer.MIN_VALUE || Integer.MAX_VALUE < l)
      throw new AvroRuntimeException("Integer overflow.");
    return (int)l;
  }


  private static long readLong(byte[] buffer, int s) {
    long n = 0;
    for (int shift = 0; ; shift += 7) {
      long b = buffer[s++];
      n |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
    }
    return (n >>> 1) ^ -(n & 1);                  // back to two's-complement
  }

  private static int intSize(int i) { return longSize(i); }

  private static int longSize(long n) {
    int size = 1;
    n = (n << 1) ^ (n >> 63);                     // move sign to low-order bit
    while ((n & ~0x7F) != 0) {
      size++;
      n >>>= 7;
    }
    return size;
  }

}
