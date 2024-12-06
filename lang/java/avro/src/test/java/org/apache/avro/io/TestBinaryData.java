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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

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
}
