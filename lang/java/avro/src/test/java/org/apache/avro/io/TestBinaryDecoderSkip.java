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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.Test;

/**
 * Regression tests for AVRO-4253's sibling AVRO-4048: robust handling of
 * {@link InputStream#skip(long)} in the binary decoders.
 * <p>
 * {@code skip()} is allowed to return {@code 0} without being at end of stream,
 * and negative returns are not part of its contract. The decoders must not treat
 * a transient {@code 0} as EOF, and must never skip more bytes than requested.
 */
public class TestBinaryDecoderSkip {

  /** An InputStream whose skip() always returns 0, forcing the read() fallback. */
  private static final class ZeroSkipInputStream extends InputStream {
    private final ByteArrayInputStream delegate;

    ZeroSkipInputStream(byte[] data) {
      this.delegate = new ByteArrayInputStream(data);
    }

    @Override
    public int read() throws IOException {
      return delegate.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return delegate.read(b, off, len);
    }

    @Override
    public long skip(long n) {
      // Simulate a stream that never manages to skip, even when not at EOF.
      return 0;
    }
  }

  /** An InputStream whose skip() advances by at most {@code chunk} bytes per call. */
  private static final class PartialSkipInputStream extends InputStream {
    private final ByteArrayInputStream delegate;
    private final long chunk;

    PartialSkipInputStream(byte[] data, long chunk) {
      this.delegate = new ByteArrayInputStream(data);
      this.chunk = chunk;
    }

    @Override
    public int read() throws IOException {
      return delegate.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return delegate.read(b, off, len);
    }

    @Override
    public long skip(long n) {
      return delegate.skip(Math.min(n, chunk));
    }
  }

  private static byte[] ramp(int len) {
    byte[] data = new byte[len];
    for (int i = 0; i < len; i++) {
      data[i] = (byte) i;
    }
    return data;
  }

  /**
   * A skip() that returns 0 without EOF must not cause a spurious EOFException,
   * and the decoder must be positioned exactly after the skipped bytes.
   */
  @Test
  public void bufferedSkipFixedWithZeroSkipStream() throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new ZeroSkipInputStream(ramp(10)), null);
    decoder.skipFixed(4);
    byte[] rest = new byte[6];
    decoder.readFixed(rest, 0, 6);
    assertArrayEquals(new byte[] { 4, 5, 6, 7, 8, 9 }, rest);
  }

  @Test
  public void directSkipFixedWithZeroSkipStream() throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(new ZeroSkipInputStream(ramp(10)), null);
    decoder.skipFixed(4);
    byte[] rest = new byte[6];
    decoder.readFixed(rest, 0, 6);
    assertArrayEquals(new byte[] { 4, 5, 6, 7, 8, 9 }, rest);
  }

  /** Skipping past the real end of stream must still raise EOFException. */
  @Test
  public void bufferedSkipFixedPastEndThrows() {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new ZeroSkipInputStream(ramp(3)), null);
    assertThrows(EOFException.class, () -> decoder.skipFixed(10));
  }

  @Test
  public void directSkipFixedPastEndThrows() {
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(new ZeroSkipInputStream(ramp(3)), null);
    assertThrows(EOFException.class, () -> decoder.skipFixed(10));
  }

  /**
   * When skip() returns fewer bytes than requested, the decoder must request
   * only the remaining count on subsequent calls; otherwise it over-skips and
   * corrupts the stream position (AVRO-4048).
   */
  @Test
  public void inputStreamSkipDoesNotOverSkip() throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new PartialSkipInputStream(ramp(10), 3), null);
    InputStream is = decoder.inputStream();

    long skipped = is.skip(5);
    assertEquals(5, skipped, "must skip exactly the requested number of bytes");
    // The next byte must be data[5]; an over-skip would surface data[6].
    assertEquals(5, is.read(), "stream must be positioned exactly after the skipped bytes");
  }
}
