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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thrown to prevent making large allocations when reading potentially
 * pathological input data from an untrusted source.
 * <p/>
 * The following system properties can be set to limit the size of bytes,
 * strings and collection types to be allocated:
 * <ul>
 * <li><tt>org.apache.avro.limits.bytes.maxLength</tt> limits the maximum size
 * of <tt>bytes</tt> types.</li>
 * <li><tt>org.apache.avro.limits.collectionItems.maxLength</tt> limits the
 * maximum number of <tt>map</tt> and <tt>list</tt> items that can be read in a
 * single sequence.</li>
 * <li><tt>org.apache.avro.limits.string.maxLength</tt> limits the maximum size
 * of <tt>string</tt> types.</li>
 * <li><tt>org.apache.avro.limits.collectionItems.maxAllocation</tt> limits the
 * number of <tt>array</tt> elements whose schema encodes to zero bytes (such as
 * <tt>null</tt> or a self-referencing record) that may be allocated at once.
 * Unlike other element types, these cannot be bounded by the number of bytes
 * remaining in the stream, so the limit defaults to a fraction of the maximum
 * heap.</li>
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

  private static final Logger LOG = LoggerFactory.getLogger(SystemLimitException.class);

  /**
   * System property declaring max size of any decompression stream: {@value}.
   */
  public static final String MAX_DECOMPRESS_LENGTH_PROPERTY = "org.apache.avro.limits.decompress.maxLength";

  /**
   * Default limit when it is lower than the heap-aware limit: {@value}.
   */
  private static final long DEFAULT_MAX_DECOMPRESS_LENGTH = 200L * 1024 * 1024;

  /**
   * Keep the default decompression limit below the maximum heap to avoid allowing
   * a single block to exhaust constrained JVMs: {@value}.
   */
  private static final long DEFAULT_MAX_DECOMPRESS_HEAP_FRACTION = 4;

  /**
   * Calculated max decompress length.
   */
  public static final long MAX_DECOMPRESS_LENGTH = getLongLimitFromProperty(MAX_DECOMPRESS_LENGTH_PROPERTY,
      defaultMaxDecompressLength());

  /**
   * System property declaring the maximum number of zero-byte-encoded array
   * elements (e.g. {@code null} or a self-referencing record) to allocate at
   * once: {@value}.
   */
  public static final String MAX_COLLECTION_ALLOCATION_PROPERTY = "org.apache.avro.limits.collectionItems.maxAllocation";

  /**
   * Fraction of the maximum heap a single decoded collection of zero-byte
   * elements may occupy by default. Keeps the backing allocation below the heap
   * so a small payload declaring a huge block count cannot exhaust the JVM.
   */
  private static final long DEFAULT_MAX_COLLECTION_ALLOCATION_HEAP_FRACTION = 4;

  /**
   * Estimated bytes retained per pre-allocated collection slot (a single object
   * reference), used to translate the heap budget into an element count.
   */
  private static final long BYTES_PER_COLLECTION_SLOT = 8;

  /**
   * Maximum number of zero-byte-encoded array elements to allocate at once.
   * Recomputed from the system property (or the heap) by {@link #resetLimits()}.
   */
  private static long maxCollectionAllocation = defaultMaxCollectionAllocation();

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
   * Get a long value stored in a system property, used to configure the system
   * behaviour of output.
   *
   * @param property     The system property to fetch
   * @param defaultValue The value to use if the system property is not present or
   *                     parsable as a long
   * @return The value from the system property
   */
  private static long getLongLimitFromProperty(String property, long defaultValue) {
    String prop = System.getProperty(property);
    long limit = defaultValue;
    if (prop != null) {
      try {
        long parsed = Long.parseLong(prop);
        if (parsed <= 0) {
          LOG.warn("Invalid value '{}' for property '{}': must be positive. Using default: {}", prop, property,
              defaultValue);
        } else {
          limit = parsed;
        }
      } catch (NumberFormatException e) {
        LOG.warn("Could not parse property '{}' value '{}'. Using default: {}", property, prop, defaultValue);
      }
    }
    return limit;
  }

  /**
   * Calculate a max decompression length as a fraction of the maximum memory of
   * the runtime.
   * 
   * @return the calculated max default decompression length.
   */
  private static long defaultMaxDecompressLength() {
    return Math.min(DEFAULT_MAX_DECOMPRESS_LENGTH,
        Math.max(1L, Runtime.getRuntime().maxMemory() / DEFAULT_MAX_DECOMPRESS_HEAP_FRACTION));
  }

  /**
   * Calculate the default maximum number of zero-byte-encoded array elements to
   * allocate at once, as a fraction of the maximum heap. Such elements consume no
   * input bytes, so the usual "bytes remaining" bound does not apply and the
   * allocation must instead be capped relative to the available memory.
   *
   * @return the calculated default max zero-byte element count.
   */
  private static long defaultMaxCollectionAllocation() {
    long heapBudget = Math.max(1L, Runtime.getRuntime().maxMemory() / DEFAULT_MAX_COLLECTION_ALLOCATION_HEAP_FRACTION);
    return Math.max(1L, heapBudget / BYTES_PER_COLLECTION_SLOT);
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
   * Check to ensure that reading the specified number of items remains within the
   * specified limits.
   *
   * @param items The next number of items to read. In normal usage, this is
   *              always a positive, permitted value. Negative and zero values
   *              have a special meaning in Avro decoding.
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
  public static int checkMaxCollectionLength(long items) {
    if (items > MAX_ARRAY_VM_LIMIT) {
      throw new UnsupportedOperationException(
          "Cannot read collections larger than " + MAX_ARRAY_VM_LIMIT + " items in Java library");
    }
    if (items > maxCollectionLength) {
      throw new SystemLimitException(
          "Collection length " + items + " exceeds the maximum allowed of " + maxCollectionLength);
    }
    return (int) items;
  }

  /**
   * Check to ensure that allocating storage for the specified number of
   * zero-byte-encoded array elements remains within the heap-aware limit.
   * <p>
   * Elements whose schema encodes to zero bytes (e.g. {@code null} or a
   * self-referencing record) consume no input bytes, so the number that may be
   * declared is not bounded by the bytes remaining in the stream. Without a cap,
   * a tiny payload can declare an enormous block count and drive an unbounded
   * backing-array allocation. This limit is derived from the maximum heap (see
   * {@link #MAX_COLLECTION_ALLOCATION_PROPERTY}).
   *
   * @param existing The number of elements already allocated for the collection.
   * @param items    The next number of elements to allocate.
   * @return The cumulative element count if and only if it is within the limit.
   * @throws SystemLimitException if the cumulative allocation would exceed the
   *                              limit.
   * @throws AvroRuntimeException if either argument is negative.
   */
  public static long checkMaxCollectionAllocation(long existing, long items) {
    if (existing < 0) {
      throw new AvroRuntimeException("Malformed data. Length is negative: " + existing);
    }
    if (items < 0) {
      throw new AvroRuntimeException("Malformed data. Length is negative: " + items);
    }
    long total = existing + items;
    if (total < existing || total > maxCollectionAllocation) {
      throw new SystemLimitException("Cannot allocate " + (total < existing ? "more than Long.MAX_VALUE" : total)
          + " zero-byte collection elements: exceeds the maximum allowed of " + maxCollectionAllocation
          + " (configure with the system property " + MAX_COLLECTION_ALLOCATION_PROPERTY + ")");
    }
    return total;
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

  /**
   * Check there is capacity to write data to a stream.
   *
   * @param limit        total capacity limit
   * @param streamLength current stream size
   * @param bytes        bytes to add to the stream
   * @throws SystemLimitException if the limit is exceeded.
   */
  public static void checkMaxDecompressCapacity(long limit, long streamLength, int bytes) {
    if (streamLength + bytes > limit) {
      throw new SystemLimitException(
          String.format("Buffer size %,d (bytes) exceeds maximum allowed size %,d.", (streamLength + bytes), limit));
    }
  }

  /** Reread the limits from the system properties. */
  // VisibleForTesting
  static void resetLimits() {
    maxBytesLength = getLimitFromProperty(MAX_BYTES_LENGTH_PROPERTY, MAX_ARRAY_VM_LIMIT);
    maxCollectionLength = getLimitFromProperty(MAX_COLLECTION_LENGTH_PROPERTY, MAX_ARRAY_VM_LIMIT);
    maxStringLength = getLimitFromProperty(MAX_STRING_LENGTH_PROPERTY, MAX_ARRAY_VM_LIMIT);
    maxCollectionAllocation = getLongLimitFromProperty(MAX_COLLECTION_ALLOCATION_PROPERTY,
        defaultMaxCollectionAllocation());
    // A collection cannot hold more than MAX_ARRAY_VM_LIMIT elements, so keep the
    // zero-byte allocation cap consistent with the other collection limits even
    // when it is configured (or derived from a very large heap) above that.
    maxCollectionAllocation = Math.min(maxCollectionAllocation, MAX_ARRAY_VM_LIMIT);
  }
}
