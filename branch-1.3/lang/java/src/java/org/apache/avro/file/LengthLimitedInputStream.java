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
package org.apache.avro.file;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/** Represents a substream of certain length. */
class LengthLimitedInputStream extends FilterInputStream {

  /** Bytes remaining. */
  private long remaining;

  protected LengthLimitedInputStream(InputStream in, long maxLength) {
    super(in);
    remaining = maxLength;
  }

  @Override
  public int read() throws IOException {
    if (remaining > 0) {
      int v = super.read();
      if (v != -1) {
        remaining--;
      }
      return v;
    }
    return -1;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  /**
   * Returns at most Integer.MAX_VALUE.
   */
  private int remainingInt() {
    return (int)Math.min(remaining, Integer.MAX_VALUE);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (remaining == 0) {
      return -1;
    }
    if (len > remaining) {
      len = remainingInt();
    }
    int v = super.read(b, off, len);
    if (v != -1) {
      remaining -= v;
    }
    return v;
  }

  @Override
  public int available() throws IOException {
    return Math.min(super.available(), remainingInt());
  }

  @Override
  public long skip(long n) throws IOException {
    long v = super.skip(Math.min(remaining, n));
    remaining -= v;
    return v;
  }
}
