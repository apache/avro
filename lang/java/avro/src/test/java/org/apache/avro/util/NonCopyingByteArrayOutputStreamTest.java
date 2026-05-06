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

package org.apache.avro.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import org.apache.avro.SystemLimitException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NonCopyingByteArrayOutputStreamTest {

  /**
   * Basic test: write then read.
   */
  @Test
  public void testDefaultWriteWorks() throws IOException {
    NonCopyingByteArrayOutputStream out = new NonCopyingByteArrayOutputStream(1);
    out.write('a');
    final byte[] b = "string".getBytes();
    out.write(b, 0, b.length);
    out.close();
    final ByteBuffer buffer = out.asByteBuffer();
    assertEquals('a', buffer.get());
    for (byte value : b) {
      assertEquals(value, buffer.get());
    }
  }

  /**
   * Test write limiting.
   */
  @Test
  public void testLimitedWrite() throws IOException {
    NonCopyingByteArrayOutputStream out = NonCopyingByteArrayOutputStream.capacityLimitedOutputStream(1, 4);
    out.write('a');
    // it's impossible to go over the limit in a write(bytes) call.
    final byte[] b = "longstring".getBytes();
    assertThrows(SystemLimitException.class, () -> out.write(b),
        "Buffer size 11 (bytes) exceeds maximum allowed size 4.");
    // we can still write up to the limit...the buffer has not been written to yet.
    out.write(b, 0, 2);
    out.write('z');
    // now at end of file, so another write shall fail.
    assertThrows(SystemLimitException.class, () -> out.write('x'));
    out.close();
    // validate everything successfully written is there
    final ByteBuffer buffer = out.asByteBuffer();
    for (byte value : "aloz".getBytes()) {
      assertEquals(value, buffer.get());
    }
  }

  @Test
  public void testInnerLimitCheck() throws Throwable {
    assertThrows(SystemLimitException.class, () -> SystemLimitException.checkMaxDecompressCapacity(256L, 0, 100_000));
    SystemLimitException.checkMaxDecompressCapacity(256L, 0, 256);
  }
}
