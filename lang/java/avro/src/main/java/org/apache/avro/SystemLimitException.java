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

package org.apache.avro;

import org.slf4j.LoggerFactory;

/**
 * Thrown to prevent making large allocations when reading potentially
 * pathological input data from an untrusted source.
 * <p/>
 * The following system properties can be set to limit the size of bytes,
 * strings and collection types to be allocated:
 * <ul>
 * <li><tt>org.apache.avro.limits.byte.maxLength</tt></li> limits the maximum
 * size of <tt>byte</tt> types.</li>
 * <li><tt>org.apache.avro.limits.collectionItems.maxLength</tt></li> limits the
 * maximum number of <tt>map</tt> and <tt>list</tt> items that can be read at
 * once single sequence.</li>
 * <li><tt>org.apache.avro.limits.string.maxLength</tt></li> limits the maximum
 * size of <tt>string</tt> types.</li>
 * </ul>
 *
 * The default is to permit sizes up to {@link #MAX_ARRAY_VM_LIMIT}.
 */
public class SystemLimitException extends AvroRuntimeException {

  /**
   * The maximum length of array to allocate (unless necessary). Some VMs reserve
   * some header words in an array. Attempts to allocate larger arrays may result
   * in {@code OutOfMemoryError: Requested array size exceeds VM limit}
   *
   * @see <a href="https://bugs.openjdk.org/browse/JDK-8246725">JDK-8246725</a>
   */
  // VisibleForTesting
  static final int MAX_ARRAY_VM_LIMIT = Integer.MAX_VALUE - 8;

  public static final String MAX_BYTES_LENGTH_PROPERTY = "org.apache.avro.limits.bytes.maxLength";
  public static final String MAX_COLLECTION_LENGTH_PROPERTY = "org.apache.avro.limits.collectionItems.maxLength";
  public static final String MAX_STRING_LENGTH_PROPERTY = "org.apache.avro.limits.string.maxLength";

  private static int maxBytesLength = MAX_ARRAY_VM_LIMIT;
  private static int maxCollectionLength = MAX_ARRAY_VM_LIMIT;
  private static int maxStringLength = MAX_ARRAY_VM_LIMIT;

  static {
    resetLimits();
  }

  public SystemLimitException(String message) {
    super(message);
  }

  /**
   * Get an integer value stored in a system property, used to configure the
   * system behaviour of decoders
   *
   * @param property     The system property to fetch
   * @param defaultValue The value to use if the system property is not present or
   *                     parsable as an int
   * @return The value from the system property
   */
  private static int getLimitFromProperty(String property, int defaultValue) {
    String o = System.getProperty(property);
    int i = defaultValue;
    if (o != null) {
      try {
        i = Integer.parseUnsignedInt(o);
      } catch (NumberFormatException nfe) {
        LoggerFactory.getLogger(SystemLimitException.class).warn("Could not parse property " + property + ": " + o,
            nfe);
      }
    }
    return i;
  }

  /**
   * Check to ensure that reading the bytes is within the specified limits.
   *
   * @param length The proposed size of the bytes to read
   * @return The size of the bytes if and only if it is within the limit and
   *         non-negative.
   * @throws UnsupportedOperationException if reading the datum would allocate a
   *                                       collection that the Java VM would be
   *                                       unable to handle
   * @throws SystemLimitException          if the decoding should fail because it
   *                                       would otherwise result in an allocation
   *                                       exceeding the set limit
   * @throws AvroRuntimeException          if the length is negative
   */
  public static int checkMaxBytesLength(long length) {
    if (length < 0) {
      throw new AvroRuntimeException("Malformed data. Length is negative: " + length);
    }
    if (length > MAX_ARRAY_VM_LIMIT) {
      throw new UnsupportedOperationException(
          "Cannot read arrays longer than " + MAX_ARRAY_VM_LIMIT + " bytes in Java library");
    }
    if (length > maxBytesLength) {
      throw new SystemLimitException("Bytes length " + length + " exceeds maximum allowed");
    }
    return (int) length;
  }

  /**
   * Check to ensure that reading the specified number of items remains within the
   * specified limits.
   *
   * @param existing The number of elements items read in the collection
   * @param items    The next number of items to read. In normal usage, this is
   *                 always a positive, permitted value. Negative and zero values
   *                 have a special meaning in Avro decoding.
   * @return The total number of items in the collection if and only if it is
   *         within the limit and non-negative.
   * @throws UnsupportedOperationException if reading the items would allocate a
   *                                       collection that the Java VM would be
   *                                       unable to handle
   * @throws SystemLimitException          if the decoding should fail because it
   *                                       would otherwise result in an allocation
   *                                       exceeding the set limit
   * @throws AvroRuntimeException          if the length is negative
   */
  public static int checkMaxCollectionLength(long existing, long items) {
    long length = existing + items;
    if (existing < 0) {
      throw new AvroRuntimeException("Malformed data. Length is negative: " + existing);
    }
    if (items < 0) {
      throw new AvroRuntimeException("Malformed data. Length is negative: " + items);
    }
    if (length > MAX_ARRAY_VM_LIMIT || length < existing) {
      throw new UnsupportedOperationException(
          "Cannot read collections larger than " + MAX_ARRAY_VM_LIMIT + " items in Java library");
    }
    if (length > maxCollectionLength) {
      throw new SystemLimitException("Collection length " + length + " exceeds maximum allowed");
    }
    return (int) length;
  }

  /**
   * Check to ensure that reading the string size is within the specified limits.
   *
   * @param length The proposed size of the string to read
   * @return The size of the string if and only if it is within the limit and
   *         non-negative.
   * @throws UnsupportedOperationException if reading the items would allocate a
   *                                       collection that the Java VM would be
   *                                       unable to handle
   * @throws SystemLimitException          if the decoding should fail because it
   *                                       would otherwise result in an allocation
   *                                       exceeding the set limit
   * @throws AvroRuntimeException          if the length is negative
   */
  public static int checkMaxStringLength(long length) {
    if (length < 0) {
      throw new AvroRuntimeException("Malformed data. Length is negative: " + length);
    }
    if (length > MAX_ARRAY_VM_LIMIT) {
      throw new UnsupportedOperationException("Cannot read strings longer than " + MAX_ARRAY_VM_LIMIT + " bytes");
    }
    if (length > maxStringLength) {
      throw new SystemLimitException("String length " + length + " exceeds maximum allowed");
    }
    return (int) length;
  }

  /** Reread the limits from the system properties. */
  // VisibleForTesting
  static void resetLimits() {
    maxBytesLength = getLimitFromProperty(MAX_BYTES_LENGTH_PROPERTY, MAX_ARRAY_VM_LIMIT);
    maxCollectionLength = getLimitFromProperty(MAX_COLLECTION_LENGTH_PROPERTY, MAX_ARRAY_VM_LIMIT);
    maxStringLength = getLimitFromProperty(MAX_STRING_LENGTH_PROPERTY, MAX_ARRAY_VM_LIMIT);
  }
}
