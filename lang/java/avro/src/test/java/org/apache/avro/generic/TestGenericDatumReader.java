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
package org.apache.avro.generic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.Schema;
import org.apache.avro.SystemLimitException;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

public class TestGenericDatumReader {

  private static final Random r = new Random(System.currentTimeMillis());

  @Test
  void readerCache() {
    final GenericDatumReader.ReaderCache cache = new GenericDatumReader.ReaderCache(this::findStringClass);
    List<Thread> threads = IntStream.rangeClosed(1, 200).mapToObj((int index) -> {
      final Schema schema = TestGenericDatumReader.this.build(index);
      final WithSchema s = new WithSchema(schema, cache);
      return (Runnable) () -> s.test();
    }).map(Thread::new).collect(Collectors.toList());
    threads.forEach(Thread::start);
    threads.forEach((Thread t) -> {
      try {
        t.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  void newInstanceFromString() {
    final GenericDatumReader.ReaderCache cache = new GenericDatumReader.ReaderCache(this::findStringClass);

    Object object = cache.newInstanceFromString(StringBuilder.class, "Hello");
    assertEquals(StringBuilder.class, object.getClass());
    StringBuilder builder = (StringBuilder) object;
    assertEquals("Hello", builder.toString());

  }

  static class WithSchema {
    private final Schema schema;

    private final GenericDatumReader.ReaderCache cache;

    public WithSchema(Schema schema, GenericDatumReader.ReaderCache cache) {
      this.schema = schema;
      this.cache = cache;
    }

    public void test() {
      this.cache.getStringClass(schema);
    }
  }

  private List<Schema> list = new ArrayList<>();

  private Schema build(int index) {
    int schemaNum = (index - 1) % 50;
    if (index <= 50) {
      Schema schema = Schema.createRecord("record_" + schemaNum, "doc", "namespace", false,
          Arrays.asList(new Schema.Field("field" + schemaNum, Schema.create(Schema.Type.STRING))));
      list.add(schema);
    }

    return list.get(schemaNum);
  }

  private Class findStringClass(Schema schema) {
    this.sleep();
    if (schema.getType() == Schema.Type.INT) {
      return Integer.class;
    }
    if (schema.getType() == Schema.Type.STRING) {
      return String.class;
    }
    if (schema.getType() == Schema.Type.LONG) {
      return Long.class;
    }
    if (schema.getType() == Schema.Type.FLOAT) {
      return Float.class;
    }
    return String.class;
  }

  private void sleep() {
    long timeToSleep = r.nextInt(30) + 10L;
    if (timeToSleep > 25) {
      try {
        Thread.sleep(timeToSleep);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // --- minBytesPerElement tests ---

  @Test
  void testMinBytesPerElementPrimitives() {
    assertEquals(0, GenericDatumReader.minBytesPerElement(Schema.create(Schema.Type.NULL)));
    assertEquals(1, GenericDatumReader.minBytesPerElement(Schema.create(Schema.Type.BOOLEAN)));
    assertEquals(1, GenericDatumReader.minBytesPerElement(Schema.create(Schema.Type.INT)));
    assertEquals(1, GenericDatumReader.minBytesPerElement(Schema.create(Schema.Type.LONG)));
    assertEquals(4, GenericDatumReader.minBytesPerElement(Schema.create(Schema.Type.FLOAT)));
    assertEquals(8, GenericDatumReader.minBytesPerElement(Schema.create(Schema.Type.DOUBLE)));
    assertEquals(1, GenericDatumReader.minBytesPerElement(Schema.create(Schema.Type.STRING)));
    assertEquals(1, GenericDatumReader.minBytesPerElement(Schema.create(Schema.Type.BYTES)));
  }

  @Test
  void testMinBytesPerElementFixed() {
    assertEquals(0, GenericDatumReader.minBytesPerElement(Schema.createFixed("ZeroFixed", null, "test", 0)));
    assertEquals(5, GenericDatumReader.minBytesPerElement(Schema.createFixed("FiveFixed", null, "test", 5)));
    assertEquals(16, GenericDatumReader.minBytesPerElement(Schema.createFixed("SixteenFixed", null, "test", 16)));
  }

  @Test
  void testMinBytesPerElementUnion() {
    // Union always >= 1 byte (branch index varint)
    Schema nullableInt = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
    assertEquals(1, GenericDatumReader.minBytesPerElement(nullableInt));
  }

  @Test
  void testMinBytesPerElementRecord() {
    // Empty record = 0 bytes
    Schema emptyRecord = Schema.createRecord("Empty", null, "test", false);
    emptyRecord.setFields(Collections.emptyList());
    assertEquals(0, GenericDatumReader.minBytesPerElement(emptyRecord));

    // Record with a single non-null field >= 1 byte
    Schema recWithInt = Schema.createRecord("WithInt", null, "test", false);
    recWithInt.setFields(Collections.singletonList(new Schema.Field("x", Schema.create(Schema.Type.INT))));
    assertEquals(1, GenericDatumReader.minBytesPerElement(recWithInt));

    // Record with only null fields = 0 bytes
    Schema recWithNull = Schema.createRecord("WithNull", null, "test", false);
    recWithNull.setFields(Collections.singletonList(new Schema.Field("n", Schema.create(Schema.Type.NULL))));
    assertEquals(0, GenericDatumReader.minBytesPerElement(recWithNull));

    Schema recWithMultipleFields = Schema.createRecord("WithMultipleFields", null, "test", false);
    recWithMultipleFields.setFields(Arrays.asList(new Schema.Field("f", Schema.create(Schema.Type.FLOAT)),
        new Schema.Field("d", Schema.create(Schema.Type.DOUBLE))));
    assertEquals(12, GenericDatumReader.minBytesPerElement(recWithMultipleFields));
  }

  @Test
  void testMinBytesPerElementNestedCollections() {
    // Array and map types are >= 1 byte (count varint)
    assertEquals(1, GenericDatumReader.minBytesPerElement(Schema.createArray(Schema.create(Schema.Type.INT))));
    assertEquals(1, GenericDatumReader.minBytesPerElement(Schema.createMap(Schema.create(Schema.Type.INT))));
  }

  // --- Collection byte validation end-to-end tests ---

  /**
   * Encodes the given longs as Avro varints into a byte array.
   */
  private static byte[] encodeVarints(long... values) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BinaryEncoder enc = EncoderFactory.get().directBinaryEncoder(baos, null);
    for (long v : values) {
      enc.writeLong(v);
    }
    enc.flush();
    return baos.toByteArray();
  }

  /**
   * Verify that reading an array of ints with a huge count but no element data
   * throws EOFException from the schema-aware byte check.
   */
  @Test
  void arrayOfIntsRejectsHugeCount() throws Exception {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
    GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);

    // Binary: varint(10_000_000) for block count, varint(0) for terminator.
    // No actual element data -- the reader should reject before allocating.
    byte[] data = encodeVarints(10_000_000L, 0L);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    assertThrows(EOFException.class, () -> reader.read(null, decoder));
  }

  /**
   * Verify that reading an array of nulls with a large count SUCCEEDS because
   * null elements are 0 bytes each, so the byte check is correctly skipped.
   */
  @Test
  void arrayOfNullsAcceptsLargeCount() throws Exception {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.NULL));
    GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);

    // Binary: varint(1000) for block count, varint(0) for terminator.
    // 1000 null elements = 0 bytes of element data.
    byte[] data = encodeVarints(1000L, 0L);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    GenericData.Array<?> result = (GenericData.Array<?>) reader.read(null, decoder);
    assertEquals(1000, result.size());
  }

  /**
   * Verify that reading a map of string->int with a huge count throws
   * EOFException. Each map entry needs at least 2 bytes (1 for key length varint
   * + 1 for int value).
   */
  @Test
  void mapOfStringToIntRejectsHugeCount() throws Exception {
    Schema schema = Schema.createMap(Schema.create(Schema.Type.INT));
    GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);

    byte[] data = encodeVarints(10_000_000L, 0L);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    assertThrows(EOFException.class, () -> reader.read(null, decoder));
  }

  /**
   * Verify that reading a map of string->null with a huge count also throws
   * EOFException because map keys are always strings (at least 1 byte each).
   */
  @Test
  void mapOfStringToNullRejectsHugeCount() throws Exception {
    Schema schema = Schema.createMap(Schema.create(Schema.Type.NULL));
    GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);

    byte[] data = encodeVarints(10_000_000L, 0L);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    assertThrows(EOFException.class, () -> reader.read(null, decoder));
  }

  /**
   * Verify that reading an array of zero-length fixed elements with a large count
   * SUCCEEDS because zero-length fixed elements are 0 bytes each.
   */
  @Test
  void arrayOfZeroLengthFixedAcceptsLargeCount() throws Exception {
    Schema fixedSchema = Schema.createFixed("Empty", null, "test", 0);
    Schema schema = Schema.createArray(fixedSchema);
    GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);

    byte[] data = encodeVarints(500L, 0L);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    GenericData.Array<?> result = (GenericData.Array<?>) reader.read(null, decoder);
    assertEquals(500, result.size());
  }

  @Test
  void arrayOfRecordsRejectsHugeCountUsingFullRecordSize() throws Exception {
    Schema recordSchema = Schema.createRecord("Element", null, "test", false);
    recordSchema.setFields(Arrays.asList(new Schema.Field("f", Schema.create(Schema.Type.FLOAT)),
        new Schema.Field("d", Schema.create(Schema.Type.DOUBLE))));
    Schema schema = Schema.createArray(recordSchema);
    GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);

    byte[] data = encodeVarints(2L, 0L);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    assertThrows(EOFException.class, () -> reader.read(null, decoder));
  }

  @Test
  void mapOfRecordsRejectsHugeCountUsingFullRecordSize() throws Exception {
    Schema recordSchema = Schema.createRecord("MapValue", null, "test", false);
    recordSchema.setFields(Arrays.asList(new Schema.Field("f", Schema.create(Schema.Type.FLOAT)),
        new Schema.Field("d", Schema.create(Schema.Type.DOUBLE))));
    Schema schema = Schema.createMap(recordSchema);
    GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);

    byte[] data = encodeVarints(1L, 0L);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    assertThrows(EOFException.class, () -> reader.read(null, decoder));
  }

  // --- Zero-byte element collection allocation limit (AVRO-4300) ---

  /**
   * An array of {@code null} elements encodes each element as 0 bytes, so the
   * bytes-remaining check cannot bound the block count. A huge count must be
   * rejected by the heap-aware allocation limit rather than driving an unbounded
   * backing-array allocation.
   */
  @Test
  void arrayOfNullsRejectsCountAboveAllocationLimit() throws Exception {
    System.setProperty(SystemLimitException.MAX_COLLECTION_ALLOCATION_PROPERTY, "1000");
    org.apache.avro.TestSystemLimitException.resetLimits();
    try {
      Schema schema = Schema.createArray(Schema.create(Schema.Type.NULL));
      GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);

      // A single block declaring 200,000 null elements: only ~4 payload bytes,
      // but would allocate a 200,000-slot backing array. Exceeds the limit of
      // 1000, so it must be rejected before allocating.
      byte[] data = encodeVarints(200_000L, 0L);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
      assertThrows(SystemLimitException.class, () -> reader.read(null, decoder));

      // Cumulative growth across multiple blocks is also rejected: two blocks of
      // 600 nulls each (1200 > 1000) must throw on the second block.
      byte[] cumulative = encodeVarints(600L, 600L, 0L);
      BinaryDecoder decoder2 = DecoderFactory.get().binaryDecoder(cumulative, null);
      assertThrows(SystemLimitException.class, () -> reader.read(null, decoder2));
    } finally {
      System.clearProperty(SystemLimitException.MAX_COLLECTION_ALLOCATION_PROPERTY);
      org.apache.avro.TestSystemLimitException.resetLimits();
    }
  }

  /**
   * A legitimate array of {@code null} elements within the allocation limit still
   * decodes correctly, so the guard does not reject valid data.
   */
  @Test
  void arrayOfNullsWithinAllocationLimitStillDecodes() throws Exception {
    System.setProperty(SystemLimitException.MAX_COLLECTION_ALLOCATION_PROPERTY, "1000");
    org.apache.avro.TestSystemLimitException.resetLimits();
    try {
      Schema schema = Schema.createArray(Schema.create(Schema.Type.NULL));
      GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);

      byte[] data = encodeVarints(1000L, 0L);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
      GenericData.Array<?> result = (GenericData.Array<?>) reader.read(null, decoder);
      assertEquals(1000, result.size());
    } finally {
      System.clearProperty(SystemLimitException.MAX_COLLECTION_ALLOCATION_PROPERTY);
      org.apache.avro.TestSystemLimitException.resetLimits();
    }
  }

  /**
   * An array whose element is a record with only {@code null} fields also encodes
   * to 0 bytes per element and must be bounded by the allocation limit.
   */
  @Test
  void arrayOfAllNullRecordsRejectsCountAboveAllocationLimit() throws Exception {
    System.setProperty(SystemLimitException.MAX_COLLECTION_ALLOCATION_PROPERTY, "1000");
    org.apache.avro.TestSystemLimitException.resetLimits();
    try {
      Schema recWithNull = Schema.createRecord("AllNull", null, "test", false);
      recWithNull.setFields(Collections.singletonList(new Schema.Field("n", Schema.create(Schema.Type.NULL))));
      Schema schema = Schema.createArray(recWithNull);
      GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);

      byte[] data = encodeVarints(200_000L, 0L);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
      assertThrows(SystemLimitException.class, () -> reader.read(null, decoder));
    } finally {
      System.clearProperty(SystemLimitException.MAX_COLLECTION_ALLOCATION_PROPERTY);
      org.apache.avro.TestSystemLimitException.resetLimits();
    }
  }

  private static GenericDatumReader<Object> arrayReader(Schema elementType, boolean fastReader) {
    return readerFor(Schema.createArray(elementType), fastReader);
  }

  private static GenericDatumReader<Object> readerFor(Schema schema, boolean fastReader) {
    GenericData data = new GenericData();
    data.setFastReaderEnabled(fastReader);
    return new GenericDatumReader<>(schema, schema, data);
  }

  /**
   * Full matrix: every collection kind must be rejected (never OOM) with a huge
   * declared block count and no element data, on both the fast (default) and the
   * classic reader. Zero-byte-element arrays are bounded by the heap-aware
   * allocation cap (SystemLimitException); every other kind is bounded by the
   * bytes-remaining check (EOFException). Maps always carry a >=1-byte key so
   * they fall in the latter group regardless of the value type.
   */
  @Test
  void hugeCollectionsRejectedOnBothReaderPaths() throws Exception {
    // element/value type -> expected exception when the count is huge and no data
    Schema nullType = Schema.create(Schema.Type.NULL);
    Schema longType = Schema.create(Schema.Type.LONG);
    Schema intType = Schema.create(Schema.Type.INT);

    // (schema, expected exception). array<null> is the only zero-byte case here.
    Object[][] cases = { { Schema.createArray(nullType), SystemLimitException.class },
        { Schema.createArray(longType), EOFException.class }, { Schema.createArray(intType), EOFException.class },
        { Schema.createMap(nullType), EOFException.class }, { Schema.createMap(longType), EOFException.class }, };

    // Small allocation limit so the zero-byte array<null> case is rejected
    // deterministically regardless of the test JVM heap size.
    System.setProperty(SystemLimitException.MAX_COLLECTION_ALLOCATION_PROPERTY, "1000");
    org.apache.avro.TestSystemLimitException.resetLimits();
    try {
      for (Object[] c : cases) {
        Schema schema = (Schema) c[0];
        @SuppressWarnings("unchecked")
        Class<? extends Throwable> expected = (Class<? extends Throwable>) c[1];
        for (boolean fast : new boolean[] { true, false }) {
          GenericDatumReader<Object> reader = readerFor(schema, fast);
          byte[] data = encodeVarints(200_000_000L, 0L);
          BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
          assertThrows(expected, () -> reader.read(null, decoder), () -> schema + " fast=" + fast);
        }
      }
    } finally {
      System.clearProperty(SystemLimitException.MAX_COLLECTION_ALLOCATION_PROPERTY);
      org.apache.avro.TestSystemLimitException.resetLimits();
    }
  }

  /**
   * The fast reader (the default decode path) must apply the same guards as the
   * classic reader. A non-zero-byte element array with a huge count and no data
   * must be rejected by the bytes-remaining check rather than pre-allocating a
   * huge backing array. Also verifies the cumulative allocation cap across
   * multiple blocks for the zero-byte case, which the single-block matrix does
   * not exercise.
   */
  @Test
  void fastAndClassicReaderRejectCumulativeNullBlocks() throws Exception {
    System.setProperty(SystemLimitException.MAX_COLLECTION_ALLOCATION_PROPERTY, "1000");
    org.apache.avro.TestSystemLimitException.resetLimits();
    try {
      for (boolean fast : new boolean[] { true, false }) {
        GenericDatumReader<Object> reader = arrayReader(Schema.create(Schema.Type.NULL), fast);
        // Two blocks of 600 nulls each (1200 > 1000) must throw on the second.
        byte[] data = encodeVarints(600L, 600L, 0L);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        assertThrows(SystemLimitException.class, () -> reader.read(null, decoder), "fastReader=" + fast);
      }
    } finally {
      System.clearProperty(SystemLimitException.MAX_COLLECTION_ALLOCATION_PROPERTY);
      org.apache.avro.TestSystemLimitException.resetLimits();
    }
  }

  /**
   * A negative block count encodes {@code abs(count)} zero-byte elements preceded
   * by a block byte-size; the decoder normalizes it to a positive count, which
   * must still be bounded by the allocation cap on both reader paths (matching
   * the negative-block-count coverage of the C and Python SDKs).
   */
  @Test
  void fastAndClassicReaderRejectNegativeNullBlockCount() throws Exception {
    System.setProperty(SystemLimitException.MAX_COLLECTION_ALLOCATION_PROPERTY, "1000");
    org.apache.avro.TestSystemLimitException.resetLimits();
    try {
      for (boolean fast : new boolean[] { true, false }) {
        GenericDatumReader<Object> reader = arrayReader(Schema.create(Schema.Type.NULL), fast);
        // -200000 items (zigzag negative), followed by a block byte-size of 0,
        // then the end-of-array terminator. Normalized to 200000 > 1000.
        byte[] data = encodeVarints(-200_000L, 0L, 0L);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        assertThrows(SystemLimitException.class, () -> reader.read(null, decoder), "fastReader=" + fast);
      }
    } finally {
      System.clearProperty(SystemLimitException.MAX_COLLECTION_ALLOCATION_PROPERTY);
      org.apache.avro.TestSystemLimitException.resetLimits();
    }
  }

  /**
   * {@code Long.MIN_VALUE} as a block count is the pathological overflow case:
   * negating it overflows back to a negative value. It must be handled safely
   * without allocating -- the decoder normalizes it to an empty collection rather
   * than a huge one -- on both reader paths. (The C SDK rejects it outright; this
   * normalization is equally non-exploitable.)
   */
  @Test
  void fastAndClassicReaderHandleMinValueBlockCountSafely() throws Exception {
    for (boolean fast : new boolean[] { true, false }) {
      GenericDatumReader<Object> reader = arrayReader(Schema.create(Schema.Type.NULL), fast);
      // Long.MIN_VALUE items (zigzag), a block byte-size of 0, then the
      // end-of-array terminator. Negating Long.MIN_VALUE overflows, so this must
      // not drive an allocation.
      byte[] data = encodeVarints(Long.MIN_VALUE, 0L, 0L);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
      Collection<?> result = (Collection<?>) reader.read(null, decoder);
      assertEquals(0, result.size(), "fastReader=" + fast);
    }
  }

  /**
   * A legitimate array of nulls within the limit still decodes on both reader
   * paths.
   */
  @Test
  void fastAndClassicReaderDecodeSmallNullArray() throws Exception {
    for (boolean fast : new boolean[] { true, false }) {
      GenericDatumReader<Object> reader = arrayReader(Schema.create(Schema.Type.NULL), fast);
      byte[] data = encodeVarints(1000L, 0L);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
      Collection<?> result = (Collection<?>) reader.read(null, decoder);
      assertEquals(1000, result.size(), "fastReader=" + fast);
    }
  }
}
