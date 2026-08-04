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

package org.apache.avro.io;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBinaryData {

  /**
   * Insert a Long value into an Array. The worst-case scenario is
   * {@link Long#MAX_VALUE} because it requires 9 bytes to encode (instead of the
   * normal 8). When skipping it, the next byte should be 10.
   */
  @Test
  void skipLong() {
    byte[] b = new byte[10];
    BinaryData.encodeLong(Long.MAX_VALUE, b, 0);

    final int nextIndex = BinaryData.skipLong(b, 0);

    assertEquals(nextIndex, 10);
  }

  @Test
  void testIntLongVleEquality() {
    byte[] intResult = new byte[9];
    byte[] longResult = new byte[9];
    BinaryData.encodeInt(0, intResult, 0);
    BinaryData.encodeLong(0, longResult, 0);
    assertArrayEquals(intResult, longResult);
    BinaryData.encodeInt(42, intResult, 0);
    BinaryData.encodeLong(42, longResult, 0);
    assertArrayEquals(intResult, longResult);
    BinaryData.encodeInt(-24, intResult, 0);
    BinaryData.encodeLong(-24, longResult, 0);
    assertArrayEquals(intResult, longResult);
    BinaryData.encodeInt(Integer.MAX_VALUE, intResult, 0);
    BinaryData.encodeLong(Integer.MAX_VALUE, longResult, 0);
    assertArrayEquals(intResult, longResult);
    BinaryData.encodeInt(Integer.MIN_VALUE, intResult, 0);
    BinaryData.encodeLong(Integer.MIN_VALUE, longResult, 0);
    assertArrayEquals(intResult, longResult);
  }

  @Test
  void testCompareBytesUnsigned() {
    // Test case: byte value 0xFF (-1 as signed, 255 as unsigned)
    // should be greater than 0x7F (127)
    byte[] b1 = new byte[] { (byte) 0xFF };
    byte[] b2 = new byte[] { (byte) 0x7F };
    int result = BinaryData.compareBytes(b1, 0, 1, b2, 0, 1);
    assertTrue(result > 0, "0xFF (255 unsigned) should be greater than 0x7F (127)");
    result = BinaryData.compareBytes(b2, 0, 1, b1, 0, 1);
    assertTrue(result < 0, "0x7F (127) should be less than 0xFF (255 unsigned)");
    result = BinaryData.compareBytes(b1, 0, 1, b1, 0, 1);
    assertEquals(0, result, "Equal byte arrays should return 0");

    // Test with multiple bytes: {0x00, 0xFF} vs {0x00, 0x7F}
    byte[] b3 = new byte[] { 0x00, (byte) 0xFF };
    byte[] b4 = new byte[] { 0x00, (byte) 0x7F };
    byte[] b5 = new byte[] { (byte) 0xFF, 0x00 };
    byte[] b6 = new byte[] { (byte) 0x7F, 0x00 };
    result = BinaryData.compareBytes(b3, 0, 2, b4, 0, 2);
    assertTrue(result > 1, "{0x00, 0xFF} should be greater than {0x00, 0x7F}");
    result = BinaryData.compareBytes(b5, 0, 2, b6, 0, 2);
    assertTrue(result > 1, "{0xFF, 0x00} should be greater than {0x7F, 0x00}");

    // Test with negative byte values: -1 (0xFF) should be greater than -128 (0x80)
    byte[] b7 = new byte[] { (byte) -1 };
    byte[] b8 = new byte[] { (byte) -128 };
    result = BinaryData.compareBytes(b7, 0, 1, b8, 0, 1);
    assertTrue(result > 0, "-1 (0xFF=255 unsigned) should be greater than -128 (0x80=128 unsigned)");
  }
}
