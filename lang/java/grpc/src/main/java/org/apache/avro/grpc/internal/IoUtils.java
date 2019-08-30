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

package org.apache.avro.grpc.internal;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

// Copied from io.grpc.internal, got deprecated in 1.23.0
public final class IoUtils {
  /**
   * maximum buffer to be read is 16 KB.
   */
  private static final int MAX_BUFFER_LENGTH = 16384;

  /**
   * Copies the data from input stream to output stream.
   */
  public static long copy(InputStream from, OutputStream to) throws IOException {
    // Copied from guava com.google.common.io.ByteStreams because its API is
    // unstable (beta)
    Preconditions.checkNotNull(from);
    Preconditions.checkNotNull(to);
    byte[] buf = new byte[MAX_BUFFER_LENGTH];
    long total = 0;
    while (true) {
      int r = from.read(buf);
      if (r == -1) {
        break;
      }
      to.write(buf, 0, r);
      total += r;
    }
    return total;
  }
}
