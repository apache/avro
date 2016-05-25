/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.util;

import java.lang.ref.SoftReference;

/**
 *
 * @author zfarkas
 */
public final class Arrays {

  private Arrays() {
  }

  private static final ThreadLocal<SoftReference<byte[]>> BYTES_TMP = new ThreadLocal<SoftReference<byte[]>>();

  /**
   * returns a thread local byte array of at least the size requested. use only for temporary purpose. This method needs
   * to be carefully used!
   *
   * @param size
   * @return
   */
  public static byte[] getBytesTmp(final int size) {
    SoftReference<byte[]> sr = BYTES_TMP.get();
    byte[] result;
    if (sr == null) {
      result = new byte[size];
      BYTES_TMP.set(new SoftReference<byte[]>(result));
    } else {
      result = sr.get();
      if (result == null || result.length < size) {
        result = new byte[size];
        BYTES_TMP.set(new SoftReference<byte[]>(result));
      }
    }
    return result;
  }

  private static final ThreadLocal<SoftReference<char[]>> CHARS_TMP = new ThreadLocal<SoftReference<char[]>>();

  /**
   * returns a thread local char array of at least the requested size. Use only for temporary purpose.
   *
   * @param size
   * @return
   */
  public static char[] getCharsTmp(final int size) {
    SoftReference<char[]> sr = CHARS_TMP.get();
    char[] result;
    if (sr == null) {
      result = new char[size];
      CHARS_TMP.set(new SoftReference<char []>(result));
    } else {
      result = sr.get();
      if (result == null || result.length < size) {
        result = new char[size];
        CHARS_TMP.set(new SoftReference<char []>(result));
      }
    }
    return result;
  }

  public static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};

}
