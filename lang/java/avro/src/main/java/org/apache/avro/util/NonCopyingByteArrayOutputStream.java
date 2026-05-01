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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.SystemLimitException;

/**
 * Utility to make data written to an {@link ByteArrayOutputStream} directly
 * available as a {@link ByteBuffer}.Supports limits to the amount of data which
 * may be written. All decompressors MUST create capacity restricted streams to
 * prevent maliciously compressed data to trigger memory problems across
 * threads.
 *
 */
public class NonCopyingByteArrayOutputStream extends ByteArrayOutputStream {

  /**
   * Size limit, -1 for no limits.
   */
  private final long limit;

  /**
   * Creates a new byte array output stream, with no size limit.
   *
   * @param size the initial size
   * @throws IllegalArgumentException if size is negative
   */
  public NonCopyingByteArrayOutputStream(int size) {
    this(size, -1);
  }

  /**
   * Creates a new byte array output stream, with a buffer capacity of the
   * specified size, in bytes, capacity limit as specified in {@code limit}.
   *
   * @param size  buffer capacity
   * @param limit size limit or -1 for no limit.
   */
  private NonCopyingByteArrayOutputStream(final int size, final long limit) {
    super(size);
    this.limit = limit;
  }

  /**
   * Get the contents of this ByteArrayOutputStream wrapped as a ByteBuffer. This
   * is a shallow copy. Changes to this ByteArrayOutputstream "write through" to
   * the ByteBuffer.
   *
   * @return The contents of this ByteArrayOutputstream wrapped as a ByteBuffer
   */
  public ByteBuffer asByteBuffer() {
    return ByteBuffer.wrap(super.buf, 0, super.count);
  }

  /**
   * Check there is capacity to write data. Throws SystemLimitException if the
   * limit is exceeded.
   *
   * @param bytes bytes to add
   */
  private void checkCapacity(int bytes) {
    if (limit > 0) {
      SystemLimitException.checkMaxDecompressCapacity(limit, size(), bytes);
    }
  }

  @Override
  public synchronized void write(final int b) {
    checkCapacity(1);
    super.write(b);
  }

  @Override
  public synchronized void write(final byte[] b, final int off, final int len) {
    checkCapacity(len);
    super.write(b, off, len);
  }

  @Override
  public void writeBytes(final byte[] b) {
    checkCapacity(b.length);
    super.writeBytes(b);
  }

  /**
   * Creates a new byte array output stream, with a buffer capacity of the
   * specified size, in bytes. The amount of data which can be written to any
   * output stream is limited by the system property
   * {@link SystemLimitException#MAX_DECOMPRESS_LENGTH_PROPERTY}
   *
   * @param size buffer capacity
   * @return the output stream
   */
  public static NonCopyingByteArrayOutputStream restrictedCapacityOutputStream(final int size) {
    final long limit = SystemLimitException.MAX_DECOMPRESS_LENGTH;
    return new NonCopyingByteArrayOutputStream((int) Math.min(size, limit), limit);
  }

  /**
   * Creates a new byte array output stream, with a buffer capacity of the
   * specified size, in bytes, capacity limit as specified in {@code limit}.
   *
   * @param size  buffer capacity
   * @param limit max size of output buffer
   * @return the output stream
   */
  public static NonCopyingByteArrayOutputStream restrictedCapacityOutputStream(final int size, long limit) {
    return new NonCopyingByteArrayOutputStream(size, limit);
  }

}
