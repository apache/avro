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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Regression tests for AVRO-4303: a {@code bytes}/{@code string} length prefix
 * read from a non-seekable stream must not drive a single up-front allocation
 * of the (attacker-declared) length. A truncated or hostile stream declaring a
 * huge length must fail with a bounded {@link EOFException} after only a
 * bounded allocation, rather than an {@link OutOfMemoryError} or a huge
 * allocation.
 */
public class TestBinaryDecoderBoundedRead {

  /**
   * Wraps the bytes in a stream whose class is neither
   * {@link ByteArrayInputStream} nor {@code ByteBufferInputStream}, so the
   * decoder reports an unknown number of remaining bytes (the non-seekable case).
   */
  private static InputStream nonSeekable(byte[] data) {
    return new FilterInputStream(new ByteArrayInputStream(data)) {
    };
  }

  /**
   * Encodes a bytes/string length prefix followed by {@code payload} raw bytes.
   */
  private static byte[] lengthPrefixed(long declaredLength, byte[] payload) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder e = EncoderFactory.get().directBinaryEncoder(out, null);
    e.writeLong(declaredLength);
    e.flush();
    out.write(payload);
    return out.toByteArray();
  }

  /**
   * A near-2GB declared length that a single {@code new byte[len]} could not
   * satisfy on a normal test heap, so reaching an {@link EOFException} proves the
   * decoder never attempted the full up-front allocation.
   */
  private static final long HUGE_LENGTH = Integer.MAX_VALUE - 8L;

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void bufferedDecoderRejectsHugeBytesLengthOnStreamWithoutHugeAllocation() throws IOException {
    byte[] data = lengthPrefixed(HUGE_LENGTH, new byte[] { 1, 2, 3, 4, 5 });
    BinaryDecoder d = DecoderFactory.get().binaryDecoder(nonSeekable(data), null);
    assertThrows(EOFException.class, () -> d.readBytes(null));
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void bufferedDecoderRejectsHugeStringLengthOnStreamWithoutHugeAllocation() throws IOException {
    byte[] data = lengthPrefixed(HUGE_LENGTH, new byte[] { 'a', 'b', 'c' });
    BinaryDecoder d = DecoderFactory.get().binaryDecoder(nonSeekable(data), null);
    assertThrows(EOFException.class, () -> d.readString(null));
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void directDecoderRejectsHugeBytesLengthOnStreamWithoutHugeAllocation() throws IOException {
    byte[] data = lengthPrefixed(HUGE_LENGTH, new byte[] { 1, 2, 3, 4, 5 });
    BinaryDecoder d = DecoderFactory.get().directBinaryDecoder(nonSeekable(data), null);
    assertThrows(EOFException.class, () -> d.readBytes(null));
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void directDecoderRejectsHugeStringLengthOnStreamWithoutHugeAllocation() throws IOException {
    byte[] data = lengthPrefixed(HUGE_LENGTH, new byte[] { 'a', 'b', 'c' });
    BinaryDecoder d = DecoderFactory.get().directBinaryDecoder(nonSeekable(data), null);
    assertThrows(EOFException.class, () -> d.readString(null));
  }

  @Test
  void legitimateLargeBytesStillRoundTripsOnNonSeekableStream() throws IOException {
    // Larger than the bounded-read threshold so it exercises the growing-buffer
    // path, but a genuinely present payload must still decode intact.
    byte[] payload = new byte[1_000_000];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) i;
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder e = EncoderFactory.get().directBinaryEncoder(out, null);
    e.writeBytes(payload);
    e.flush();

    BinaryDecoder d = DecoderFactory.get().binaryDecoder(nonSeekable(out.toByteArray()), null);
    ByteBuffer result = d.readBytes(null);
    byte[] decoded = new byte[result.remaining()];
    result.get(decoded);
    assertArrayEquals(payload, decoded);
  }

  @Test
  void reusableBufferWithSufficientCapacityIsHonoredOnNonSeekableStream() throws IOException {
    // A large value on a non-seekable stream, supplied with a reusable buffer that
    // is already big enough, must reuse it (Decoder#readBytes reuse contract)
    // rather than take the bounded-read path and allocate a new array.
    byte[] payload = new byte[1_000_000];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) i;
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder e = EncoderFactory.get().directBinaryEncoder(out, null);
    e.writeBytes(payload);
    e.flush();

    ByteBuffer reusable = ByteBuffer.allocate(payload.length + 100);
    BinaryDecoder d = DecoderFactory.get().binaryDecoder(nonSeekable(out.toByteArray()), null);
    ByteBuffer result = d.readBytes(reusable);
    assertSame(reusable.array(), result.array(), "supplied buffer with sufficient capacity should be reused");
    byte[] decoded = new byte[result.remaining()];
    result.get(decoded);
    assertArrayEquals(payload, decoded);
  }

  @Test
  void legitimateLargeStringStillRoundTripsOnNonSeekableStream() throws IOException {
    StringBuilder sb = new StringBuilder();
    while (sb.length() < 100_000) {
      sb.append("avro-4303-");
    }
    String expected = sb.toString();
    byte[] utf8 = expected.getBytes(StandardCharsets.UTF_8);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder e = EncoderFactory.get().directBinaryEncoder(out, null);
    e.writeString(expected);
    e.flush();

    BinaryDecoder d = DecoderFactory.get().directBinaryDecoder(nonSeekable(out.toByteArray()), null);
    String decoded = d.readString();
    assertEquals(expected, decoded);
    // Sanity: the bounded path produced exactly the encoded bytes.
    assertArrayEquals(utf8, decoded.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void smallValuesAndSeekableSourcesKeepDirectAllocationPath() throws IOException {
    // Small value: below the bounded-read threshold, read directly.
    byte[] small = new byte[100];
    Arrays.fill(small, (byte) 7);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder e = EncoderFactory.get().directBinaryEncoder(out, null);
    e.writeBytes(small);
    e.flush();
    byte[] encoded = out.toByteArray();

    // Seekable (byte-array) source: existing available-bytes guard rejects an
    // over-long declared length up front, unchanged by AVRO-4303.
    byte[] truncated = lengthPrefixed(HUGE_LENGTH, new byte[] { 1, 2, 3 });
    BinaryDecoder seekable = DecoderFactory.get().binaryDecoder(truncated, null);
    assertThrows(EOFException.class, () -> seekable.readBytes(null));

    // Small value still decodes on a non-seekable stream via the direct path.
    BinaryDecoder d = DecoderFactory.get().binaryDecoder(nonSeekable(encoded), null);
    ByteBuffer result = d.readBytes(null);
    byte[] decoded = new byte[result.remaining()];
    result.get(decoded);
    assertArrayEquals(small, decoded);
  }
}
