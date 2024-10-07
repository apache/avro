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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.avro.SystemLimitException;

/**
 * A Utf8 string. Unlike {@link String}, instances are mutable. This is more
 * efficient than {@link String} when reading or writing a sequence of values,
 * as a single instance may be reused.
 */
public class Utf8 implements Comparable<Utf8>, CharSequence, Externalizable {

  private static final byte[] EMPTY = new byte[0];

  private byte[] bytes;
  private int hash;
  private int length;
  private String string;

  public Utf8() {
    this.bytes = EMPTY;
    this.hash = 1;
  }

  public Utf8(String string) {
    byte[] bytes = getBytesFor(string);
    int length = bytes.length;
    SystemLimitException.checkMaxStringLength(length);
    this.bytes = bytes;
    this.length = length;
    this.string = string;
  }

  public Utf8(Utf8 other) {
    this.length = other.length;
    this.bytes = Arrays.copyOf(other.bytes, other.length);
    this.string = other.string;
    this.hash = other.hash;
  }

  public Utf8(byte[] bytes) {
    int length = bytes.length;
    SystemLimitException.checkMaxStringLength(length);
    this.bytes = bytes;
    this.length = length;
  }

  /**
   * Return UTF-8 encoded bytes. Only valid through {@link #getByteLength()}
   * assuming the bytes have been fully copied into the underlying buffer from the
   * source.
   *
   * @see #setByteLength(int)
   * @return a reference to the underlying byte array
   */
  public byte[] getBytes() {
    return bytes;
  }

  /** Return length in bytes. */
  public int getByteLength() {
    return length;
  }

  /**
   * Set length in bytes. When calling this method, even if the new length is the
   * same as the current length, the cached contents of this Utf8 object will be
   * wiped out. After calling this method, no assumptions should be made about the
   * internal state (e.g., contents, hashcode, equality, etc.) of this Utf8 String
   * other than the internal buffer being large enough to accommodate a String of
   * the new length. This should be called immediately before reading a String
   * from the underlying data source.
   *
   * @param newLength the new length of the underlying buffer
   * @return a reference to this object.
   * @see org.apache.avro.io.BinaryDecoder#readString(Utf8)
   */
  public Utf8 setByteLength(int newLength) {
    SystemLimitException.checkMaxStringLength(newLength);

    // Note that if the buffer size increases, the internal buffer is zero-ed out.
    // If the buffer is large enough, just the length pointer moves and the old
    // contents remain. For consistency's sake, we could zero-out the buffer in
    // both cases, but would be a perf hit.
    if (this.bytes.length < newLength) {
      this.bytes = new byte[newLength];
    }
    this.length = newLength;
    this.string = null;
    this.hash = 0;
    return this;
  }

  /** Set to the contents of a String. */
  public Utf8 set(String string) {
    byte[] bytes = getBytesFor(string);
    int length = bytes.length;
    SystemLimitException.checkMaxStringLength(length);
    this.bytes = bytes;
    this.length = length;
    this.string = string;
    this.hash = 0;
    return this;
  }

  public Utf8 set(Utf8 other) {
    if (this.bytes.length < other.length) {
      this.bytes = new byte[other.length];
    }
    this.length = other.length;
    System.arraycopy(other.bytes, 0, bytes, 0, length);
    this.string = other.string;
    this.hash = other.hash;
    return this;
  }

  @Override
  public String toString() {
    if (this.length == 0)
      return "";
    if (this.string == null) {
      this.string = new String(bytes, 0, length, StandardCharsets.UTF_8);
    }
    return this.string;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (!(o instanceof Utf8))
      return false;
    Utf8 that = (Utf8) o;
    if (!(this.length == that.length))
      return false;
    // For longer strings, leverage vectorization (JDK 9+) to determine equality
    // For shorter strings, the overhead of this method defeats the value
    if (this.length > 7)
      return Arrays.equals(this.bytes, 0, this.length, that.bytes, 0, that.length);
    byte[] thatBytes = that.bytes;
    for (int i = 0; i < this.length; i++)
      if (bytes[i] != thatBytes[i])
        return false;
    return true;
  }

  @Override
  public int hashCode() {
    int h = hash;
    if (h == 0) {
      byte[] bytes = this.bytes;
      int length = this.length;
      h = 1;
      for (int i = 0; i < length; i++) {
        h = h * 31 + bytes[i];
      }
      this.hash = h;
    }
    return h;
  }

  @Override
  public int compareTo(Utf8 that) {
    return Arrays.compare(this.bytes, 0, this.length, that.bytes, 0, that.length);
  }

  // CharSequence implementation
  @Override
  public char charAt(int index) {
    return toString().charAt(index);
  }

  @Override
  public int length() {
    return toString().length();
  }

  @Override
  public CharSequence subSequence(int start, int end) {
    return toString().subSequence(start, end);
  }

  /** Gets the UTF-8 bytes for a String */
  public static byte[] getBytesFor(String str) {
    return str.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    setByteLength(in.readInt());
    in.readFully(bytes);
  }
}
