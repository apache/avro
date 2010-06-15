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
package org.apache.avro.util;

import java.io.UnsupportedEncodingException;

import org.apache.avro.io.BinaryData;
import org.apache.avro.reflect.Stringable;

/** A Utf8 string. */
@Stringable
public class Utf8 implements Comparable<Utf8> {
  private static final byte[] EMPTY = new byte[0];

  byte[] bytes = EMPTY;
  int length;

  public Utf8() {}

  public Utf8(String string) {
    try {
      this.bytes = string.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    this.length = bytes.length;
  }

  public Utf8(byte[] bytes) {
    this.bytes = bytes;
    this.length = bytes.length;
  }

  public byte[] getBytes() { return bytes; }
  public int getLength() { return length; }

  public Utf8 setLength(int newLength) {
    if (this.length < newLength) {
      byte[] newBytes = new byte[newLength];
      System.arraycopy(bytes, 0, newBytes, 0, this.length);
      this.bytes = newBytes;
    }
    this.length = newLength;
    return this;
  }

  public String toString() {
    try {
      return new String(bytes, 0, length, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Utf8)) return false;
    Utf8 that = (Utf8)o;
    if (!(this.length == that.length)) return false;
    byte[] thatBytes = that.bytes;
    for (int i = 0; i < this.length; i++)
      if (bytes[i] != thatBytes[i])
        return false;
    return true;
  }

  public int hashCode() {
    int hash = 0;
    for (int i = 0; i < this.length; i++)
      hash = hash*31 + bytes[i];
    return hash;
  }

  public int compareTo(Utf8 that) {
    return BinaryData.compareBytes(this.bytes, 0, this.length,
                                   that.bytes, 0, that.length);
  }

}
