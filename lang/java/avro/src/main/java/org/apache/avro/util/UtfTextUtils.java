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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Text utilities especially suited for UTF encoded bytes.
 *
 * <p>
 * When the character set is unknown, methods in this class assume UTF encoded
 * text and try to detect the UTF variant (8/16/32 bits, big/little endian),
 * using the BOM (if present) or an educated guess assuming the first character
 * is in the range U+0000-U+00FF. This heuristic works for all latin text based
 * formats, which includes Avro IDL, JSON, YAML, XML, etc. If the heuristic
 * fails, UTF-8 is assumed.
 * </p>
 *
 * @see <a href="https://www.w3.org/TR/xml/#sec-guessing">XML specification,
 *      appendix F: Autodetection of Character Encodings (Non-Normative)</a>
 */
public class UtfTextUtils {
  private static final int TRANSFER_BUFFER_SIZE = 4096;
  /**
   * JVM standard character set (but that doesn't have a constant in
   * {@link StandardCharsets}) for UTF-32.
   */
  private static final Charset UTF_32 = Charset.forName("UTF-32");
  /**
   * JVM standard character set (but that doesn't have a constant in
   * {@link StandardCharsets}) for UTF-32BE.
   */
  private static final Charset UTF_32BE = Charset.forName("UTF-32BE");
  /**
   * JVM standard character set (but that doesn't have a constant in
   * {@link StandardCharsets}) for UTF-32LE.
   */
  private static final Charset UTF_32LE = Charset.forName("UTF-32LE");

  public static String asString(byte[] bytes, Charset charset) {
    if (charset == null) {
      charset = detectUtfCharset(bytes);
    }
    return skipBOM(new String(bytes, charset));
  }

  /**
   * Reads the specified input stream as text. If {@code charset} is {@code null},
   * the method will assume UTF encoded text and attempt to detect the appropriate
   * charset.
   *
   * @param input   the input to read
   * @param charset the character set of the input, if known
   * @return all bytes, read into a string
   * @throws IOException when reading the input fails for some reason
   */
  public static String readAllBytes(InputStream input, Charset charset) throws IOException {
    input = ensureMarkSupport(input);
    input.mark(4);
    byte[] buffer = new byte[4];
    int bytesRead = fillBuffer(input, buffer);
    input.reset();

    if (charset == null) {
      charset = detectUtfCharset0(buffer, bytesRead);
    }
    if (charset == null) {
      throw new IOException("Unsupported UCS-4 variant (neither UTF-32BE nor UTF32-LE)");
    }
    Reader reader = new InputStreamReader(input, charset);
    return readAllChars(reader);
  }

  static InputStream ensureMarkSupport(InputStream input) {
    if (input.markSupported()) {
      return input;
    } else {
      return new BufferedInputStream(input);
    }
  }

  static int fillBuffer(InputStream in, byte[] buf) throws IOException {
    int remaining = buf.length;
    int offset = 0;
    while (remaining > 0) {
      int bytesRead = in.read(buf, offset, remaining);
      // As remaining > 0, bytesRead is either -1 or positive
      if (bytesRead == -1) {
        break;
      }
      offset += bytesRead;
      remaining -= bytesRead;
    }
    return offset;
  }

  public static String readAllChars(Reader input) throws IOException {
    StringBuilder buffer = new StringBuilder();
    char[] charBuffer = new char[TRANSFER_BUFFER_SIZE];
    int charsRead;
    while ((charsRead = input.read(charBuffer, 0, TRANSFER_BUFFER_SIZE)) >= 0) {
      buffer.append(charBuffer, 0, charsRead);
    }
    return skipBOM(buffer);
  }

  private static String skipBOM(CharSequence buffer) {
    if (buffer.charAt(0) == '\uFEFF') {
      return buffer.subSequence(1, buffer.length()).toString();
    }
    return buffer.toString();
  }

  /**
   * Assuming UTF encoded bytes, detect the UTF variant (8/16/32 bits, big/little
   * endian).
   *
   * <p>
   * Detection is certain when a byte order mark (BOM) is used. Otherwise a
   * heuristic is used, which works when the first character is from the first 256
   * characters from the BMP (U+0000-U+00FF). This works for all latin-based
   * textual formats, like Avro IDL, JSON, YAML, XML, etc.
   * </p>
   *
   * @param firstFewBytes the first few bytes of the text to detect the character
   *                      set of
   * @return the character set to use
   */
  public static Charset detectUtfCharset(byte[] firstFewBytes) {
    return detectUtfCharset(firstFewBytes, firstFewBytes.length);
  }

  /**
   * Assuming UTF encoded bytes, detect the UTF variant (8/16/32 bits, big/little
   * endian).
   *
   * <p>
   * Detection is certain when a byte order mark (BOM) is used. Otherwise a
   * heuristic is used, which works when the first character is from the first 256
   * characters from the BMP (U+0000-U+00FF). This works for all latin-based
   * textual formats, like Avro IDL, JSON, YAML, XML, etc.
   * </p>
   *
   * @param firstFewBytes the first few bytes of the text to detect the character
   *                      set of
   * @param numBytes      the number of bytes in {@code firstFewBytes} that are
   *                      valid (starting at offset 0)
   * @return the character set to use
   */
  public static Charset detectUtfCharset(byte[] firstFewBytes, int numBytes) {
    Charset detectedCharset = detectUtfCharset0(firstFewBytes, numBytes);
    if (detectedCharset == null) {
      throw new IllegalArgumentException("Unsupported UCS-4 variant (neither UTF-32BE nor UTF32-LE)");
    }
    return detectedCharset;
  }

  private static Charset detectUtfCharset0(byte[] firstFewBytes, int numBytes) {
    /*
     * Lookup table, adapted from https://www.w3.org/TR/xml/#sec-guessing Note: the
     * order (with respect to UTF-32 & UTF-16) is important!
     *
     * ||BOM||Magic bytes||Result || | yes| 00 00 FE FF| UTF-32 (be) | | yes| FF FE
     * 00 00| UTF-32 (le) | | yes| 00 00 FF FE| unsupported UCS-4 (byte order 2143)
     * | | yes| FE FF 00 00| unsupported UCS-4 (byte order 3412) | | yes| FE FF __
     * __| UTF-16 (be) | | yes| FF FE __ __| UTF-16 (le) | | yes| EF BB BF __| UTF-8
     * | | no | 00 00 00 __| UTF-32BE | | no | __ 00 00 00| UTF-32LE | | no | 00 00
     * __ 00| unsupported UCS-4 (byte order 2143) | | no | 00 __ 00 00| unsupported
     * UCS-4 (byte order 3412) | | no | 00 __ __ __| UTF-16BE | | no | __ 00 __ __|
     * UTF-16LE | | no | __ __ __ __| UTF-8 (fallback) |
     */
    int quad = quad(firstFewBytes, numBytes);
    int word = quad >>> 16;
    if (numBytes > 3 && (quad == 0x0000FEFF || quad == 0xFFFE0000)) {
      // With BOM: UTF-32 (Charset handles BOM & endianness)
      return UTF_32;
    } else if (numBytes > 3 && (quad == 0x0000FFFE || quad == 0xFEFF0000)) {
      // With BOM: unsupported UCS-4 encoding (byte order 2143 resp. 3412)
      return null;
    } else if (numBytes > 1 && (word == 0xFEFF || word == 0xFFFE)) {
      // With BOM: UTF-16 (Charset handles BOM & endianness)
      return StandardCharsets.UTF_16;
    } else if (numBytes > 2 && quad >>> 8 == 0xEFBBBF) {
      // With BOM: UTF-8 (Charset does not handle a BOM, so our caller must skip it)
      return StandardCharsets.UTF_8;
    } else if (numBytes > 3 && (quad & 0xFFFFFF00) == 0) {
      // Without BOM (i.e., a guess)
      return UTF_32BE;
    } else if (numBytes > 3 && (quad & 0x00FFFFFF) == 0) {
      // Without BOM (i.e., a guess)
      return UTF_32LE;
    } else if (numBytes > 3 && (quad & 0xFFFF00FF) == 0 || (quad & 0xFF00FFFF) == 0) {
      // Without BOM (i.e., a guess): unsupported UCS-4 encoding (byte order 2143
      // resp. 3412)
      return null;
    } else if (numBytes > 1 && (word & 0xFF00) == 0) {
      // Without BOM (i.e., a guess)
      return StandardCharsets.UTF_16BE;
    } else if (numBytes > 1 && (word & 0x00FF) == 0) {
      // Without BOM (i.e., a guess)
      return StandardCharsets.UTF_16LE;
    } else {
      // Fallback
      return StandardCharsets.UTF_8;
    }
  }

  private static int quad(byte[] bytes, int length) {
    int quad = 0xFFFFFFFF;
    switch (length) {
    default:
      quad = (quad & 0xFFFFFF00) | (bytes[3] & 0xFF);
      // Fallthrough
    case 3:
      quad = (quad & 0xFFFF00FF) | (bytes[2] & 0xFF) << 8;
      // Fallthrough
    case 2:
      quad = (quad & 0xFF00FFFF) | (bytes[1] & 0xFF) << 16;
      // Fallthrough
    case 1:
      quad = (quad & 0x00FFFFFF) | (bytes[0] & 0xFF) << 24;
      // Fallthrough
    case 0:
      break;
    }
    return quad;
  }
}
