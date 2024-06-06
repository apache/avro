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
package org.apache.avro.util;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("SpellCheckingInspection")
class UtfTextUtilsTest {
  @Test
  void validateCharsetDetectionWithBOM() {
    assertEquals("UTF-32", testDetection("0000FEFF").name());
    assertEquals("UTF-32", testDetection("FFFE0000").name());
    assertEquals("UTF-16", testDetection("FEFF0041").name());
    assertEquals("UTF-16", testDetection("FFFE4100").name());
    assertEquals("UTF-8", testDetection("EFBBBF41").name());

    // Invalid UCS-4 encodings: these we're certain we cannot handle.
    assertThrows(IllegalArgumentException.class, () -> testDetection("0000FFFE"));
    assertThrows(IllegalArgumentException.class, () -> testDetection("FEFF0000"));
  }

  @Test
  void validateCharsetDetectionWithoutBOM() {
    assertEquals("UTF-32BE", testDetection("00000041").name());
    assertEquals("UTF-32LE", testDetection("41000000").name());
    assertEquals("UTF-16BE", testDetection("00410042").name());
    assertEquals("UTF-16LE", testDetection("41004200").name());
    assertEquals("UTF-8", testDetection("41424344").name());

    assertEquals("UTF-8", testDetection("414243").name());

    assertEquals("UTF-16BE", testDetection("0041").name());
    assertEquals("UTF-16LE", testDetection("4100").name());
    assertEquals("UTF-8", testDetection("4142").name());

    assertEquals("UTF-8", testDetection("41").name());

    assertEquals("UTF-8", testDetection("").name());

    // Invalid UCS-4 encodings: these we're fairly certain we cannot handle.
    assertThrows(IllegalArgumentException.class, () -> testDetection("00004100"));
    assertThrows(IllegalArgumentException.class, () -> testDetection("00410000"));
  }

  private Charset testDetection(String hexBytes) {
    return UtfTextUtils.detectUtfCharset(hexBytes(hexBytes));
  }

  private static byte[] hexBytes(String hexBytes) {
    byte[] bytes = new byte[hexBytes.length() / 2];
    for (int i = 0; i < bytes.length; i++) {
      int index = i * 2;
      bytes[i] = (byte) Integer.parseUnsignedInt(hexBytes.substring(index, index + 2), 16);
    }
    return bytes;
  }

  @Test
  void validateTextConversionFromBytes() {
    assertEquals("A", UtfTextUtils.asString(hexBytes("EFBBBF41"), StandardCharsets.UTF_8));
    assertEquals("A", UtfTextUtils.asString(hexBytes("EFBBBF41"), null));

    assertEquals("A", UtfTextUtils.asString(hexBytes("41"), StandardCharsets.UTF_8));
    assertEquals("A", UtfTextUtils.asString(hexBytes("41"), null));
  }

  @Test
  void validateTextConversionFromStreams() throws IOException {
    assertEquals("A",
        UtfTextUtils.readAllBytes(new ByteArrayInputStream(hexBytes("EFBBBF41")), StandardCharsets.UTF_8));
    assertEquals("A", UtfTextUtils.readAllBytes(new ByteArrayInputStream(hexBytes("EFBBBF41")), null));

    assertEquals("A", UtfTextUtils.readAllBytes(new ByteArrayInputStream(hexBytes("41")), StandardCharsets.UTF_8));
    assertEquals("A", UtfTextUtils.readAllBytes(new ByteArrayInputStream(hexBytes("41")), null));

    // Invalid UCS-4 encoding should throw an IOException instead of an
    // IllegalArgumentException.
    assertThrows(IOException.class,
        () -> UtfTextUtils.readAllBytes(new ByteArrayInputStream(hexBytes("0000FFFE")), null));
  }

  @Test
  void validateSupportForUnmarkableStreams() throws IOException {
    assertEquals("ABCD",
        UtfTextUtils.readAllBytes(new UnmarkableInputStream(new ByteArrayInputStream(hexBytes("41424344"))), null));
  }

  private static class UnmarkableInputStream extends FilterInputStream {
    public UnmarkableInputStream(InputStream input) {
      super(input);
    }

    @Override
    public synchronized void mark(int ignored) {
    }

    @Override
    public synchronized void reset() throws IOException {
      throw new IOException("mark/reset not supported");
    }

    @Override
    public boolean markSupported() {
      return false;
    }
  }
}
