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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.Schema;
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
}
