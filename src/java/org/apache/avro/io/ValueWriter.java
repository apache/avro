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
package org.apache.avro.io;

import java.io.*;
import java.nio.ByteBuffer;
import org.apache.avro.util.Utf8;

/** Write leaf values.
 * <p>Has no state except that of the OutputStream it wraps.
 * <p>Used by {@link DatumWriter} implementations to write datum leaf values.
 * @see ValueReader
 */
public class ValueWriter extends FilterOutputStream {
  public ValueWriter(OutputStream out) {
    super(out);
  }
  /** Write a string as {@link #writeLong(long)}-prefixed UTF-8. */
  public void writeUtf8(Utf8 utf8) throws IOException {
    writeLong(utf8.getLength());
    out.write(utf8.getBytes(), 0, utf8.getLength());
  }
  /** Write a buffer of bytes. */
  public void writeBuffer(ByteBuffer bytes) throws IOException {
    writeLong(bytes.remaining());
    out.write(bytes.array(), bytes.position(), bytes.remaining());
  }
  /** Write an int using 1-5 bytes.  The sign is moved to the low-order bit,
   * and then the value is written so that the high-order bit of each byte
   * indicates whether more bytes remain. */
  public void writeInt(int n) throws IOException { writeLong(n); }

  /** Write a long using 1-10 bytes.  The sign is moved to the low-order bit,
   * and then the value is written so that the high-order bit of each byte
   * indicates whether more bytes remain. */
  public void writeLong(long n) throws IOException {
    n = (n << 1) ^ (n >> 63);                     // move sign to low-order bit
    while ((n & ~0x7F) != 0) {
      out.write((byte)((n & 0x7f) | 0x80));
      n >>>= 7;
    }
    out.write((byte)n);
  }

  /** Writes a float as eight bytes. */
  public void writeFloat(float n) throws IOException {
    int bits = Float.floatToRawIntBits(n);
    out.write((int)(bits      ) & 0xFF);
    out.write((int)(bits >>  8) & 0xFF);
    out.write((int)(bits >> 16) & 0xFF);
    out.write((int)(bits >> 24) & 0xFF);
  }

  /** Writes a double as eight bytes. */
  public void writeDouble(double n) throws IOException {
    long bits = Double.doubleToRawLongBits(n);
    out.write((int)(bits      ) & 0xFF);
    out.write((int)(bits >>  8) & 0xFF);
    out.write((int)(bits >> 16) & 0xFF);
    out.write((int)(bits >> 24) & 0xFF);
    out.write((int)(bits >> 32) & 0xFF);
    out.write((int)(bits >> 40) & 0xFF);
    out.write((int)(bits >> 48) & 0xFF);
    out.write((int)(bits >> 56) & 0xFF);
  }
 
 /** Writes a boolean as a single byte. */
  public void writeBoolean(boolean b) throws IOException {
    out.write(b ? 1 : 0);
  }

  public void write(byte b[], int off, int len) throws IOException {
    out.write(b, off, len);
  }

}
