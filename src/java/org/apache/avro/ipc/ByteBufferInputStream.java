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

package org.apache.avro.ipc;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Utility to present {@link ByteBuffer} data as an {@link InputStream}.*/
class ByteBufferInputStream extends InputStream {
  private static final int BUFFER_SIZE = 8192;

  private List<ByteBuffer> buffers;
  private int current;

  public ByteBufferInputStream(List<ByteBuffer> buffers) {
    this.buffers = buffers;
  }

  public int read() {
    ByteBuffer buffer = buffers.get(current);
    while (!buffer.hasRemaining())                // skip empty
      buffer = buffers.get(++current);
    return buffer.get();
  }

  public int read(byte b[], int off, int len) {
    ByteBuffer buffer = buffers.get(current);
    int remaining = buffer.remaining();
    if (len > remaining) {
      buffer.get(b, off, remaining);
      return remaining;
    } else {
      buffer.get(b, off, len);
      return len;
    }
  }

  /** Read a buffer from the input without copying, if possible. */
  public ByteBuffer readBuffer(int length) throws IOException {
    ByteBuffer buffer = buffers.get(current);
    while (!buffer.hasRemaining())                // skip empty
      buffer = buffers.get(++current);
    if (buffer.remaining() == length) {           // can return current as-is?
      current++;
      return buffer;                              // return w/o copying
    }
    // punt: allocate a new buffer & copy into it
    ByteBuffer result = ByteBuffer.allocate(length);
    read(result.array(), 0, length);
    return result;
  }
}
