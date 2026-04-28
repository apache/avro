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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.SystemLimitException;

/**
 * Utility to make data written to an {@link ByteArrayOutputStream} directly
 * available as a {@link ByteBuffer}. Optional limit to amount of data which may
 * be written.
 */
public class NonCopyingByteArrayOutputStream extends ByteArrayOutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(NonCopyingByteArrayOutputStream.class);

  /**
   * System property declaring max size of any decompression stream: {@value}.
   */
  private static final String MAX_DECOMPRESS_LENGTH_PROPERTY = "org.apache.avro.limits.decompress.maxLength";

  /**
   * Default limit: {@value}.
   */
  private static final long DEFAULT_MAX_DECOMPRESS_LENGTH = 200L * 1024 * 1024;

  private static final long MAX_DECOMPRESS_LENGTH;

  static {
    String prop = System.getProperty(MAX_DECOMPRESS_LENGTH_PROPERTY);
    long limit = DEFAULT_MAX_DECOMPRESS_LENGTH;
    if (prop != null) {
      try {
        long parsed = Long.parseLong(prop);
        if (parsed <= 0) {
          LOG.warn("Invalid value '{}' for property '{}': must be positive. Using default: {}", prop,
              MAX_DECOMPRESS_LENGTH_PROPERTY, DEFAULT_MAX_DECOMPRESS_LENGTH);
        } else {
          limit = parsed;
        }
      } catch (NumberFormatException e) {
        LOG.warn("Could not parse property '{}' value '{}'. Using default: {}", MAX_DECOMPRESS_LENGTH_PROPERTY, prop,
            DEFAULT_MAX_DECOMPRESS_LENGTH);
      }
    }
    MAX_DECOMPRESS_LENGTH = limit;
  }

  /**
   * Size limit, -1 for no limits.
   */
  private final long limit;

  /**
   * Creates a new byte array output stream, with a buffer capacity of the
   * specified size, in bytes, size limit {@link #MAX_DECOMPRESS_LENGTH}
   *
   * @param size the initial size
   * @throws IllegalArgumentException if size is negative
   */
  public NonCopyingByteArrayOutputStream(int size) {
    this(size, MAX_DECOMPRESS_LENGTH);
  }

  /**
   * Creates a new byte array output stream, with a buffer capacity of the
   * specified size, in bytes, size limit as specified.
   * 
   * @param size  buffer capacity
   * @param limit size limit or -1 for no limit.
   */
  public NonCopyingByteArrayOutputStream(final int size, final long limit) {
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
   * Check there is capacity to write data.
   * 
   * @param bytes bytes to add
   * @throws SystemLimitException if the limit is exceeded.
   */
  private void checkCapacity(int bytes) {
    if (limit >= 0 && (size() + bytes) >= limit) {
      throw new SystemLimitException(
          String.format("Buffer size %,3d (bytes) exceeds maximum allowed size %,3d.", (size() + bytes), limit));

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

}
