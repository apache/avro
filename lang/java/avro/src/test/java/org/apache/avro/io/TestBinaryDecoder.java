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
package org.apache.avro.io;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SystemLimitException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.avro.util.RandomData;
import org.apache.avro.util.Utf8;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.avro.TestSystemLimitException.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestBinaryDecoder {
  // prime number buffer size so that looping tests hit the buffer edge
  // at different points in the loop.
  DecoderFactory factory = new DecoderFactory().configureDecoderBufferSize(521);

  static EncoderFactory e_factory = EncoderFactory.get();

  private Decoder newDecoderWithNoData(boolean useDirect) {
    return newDecoder(new byte[0], useDirect);
  }

  private BinaryDecoder newDecoder(byte[] bytes, int start, int len, boolean useDirect) {
    return this.newDecoder(bytes, start, len, null, useDirect);
  }

  private BinaryDecoder newDecoder(byte[] bytes, int start, int len, BinaryDecoder reuse, boolean useDirect) {
    if (useDirect) {
      final ByteArrayInputStream input = new ByteArrayInputStream(bytes, start, len);
      return factory.directBinaryDecoder(input, reuse);
    } else {
      return factory.binaryDecoder(bytes, start, len, reuse);
    }
  }

  private BinaryDecoder newDecoder(InputStream in, boolean useDirect) {
    return this.newDecoder(in, null, useDirect);
  }

  private BinaryDecoder newDecoder(InputStream in, BinaryDecoder reuse, boolean useDirect) {
    if (useDirect) {
      return factory.directBinaryDecoder(in, reuse);
    } else {
      return factory.binaryDecoder(in, reuse);
    }
  }

  private BinaryDecoder newDecoder(byte[] bytes, BinaryDecoder reuse, boolean useDirect) {
    if (useDirect) {
      return this.factory.directBinaryDecoder(new ByteArrayInputStream(bytes), reuse);
    } else {
      return factory.binaryDecoder(bytes, reuse);
    }
  }

  private BinaryDecoder newDecoder(byte[] bytes, boolean useDirect) {
    return this.newDecoder(bytes, null, useDirect);
  }

  /**
   * Create a decoder for simulating reading corrupt, unexpected or out-of-bounds
   * data.
   *
   * @return a {@link org.apache.avro.io.BinaryDecoder that has been initialized
   *         on a byte array containing the sequence of encoded longs in order.
   */
  private BinaryDecoder newDecoder(boolean useDirect, long... values) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      for (long v : values)
        encoder.writeLong(v);
      encoder.flush();
      return newDecoder(baos.toByteArray(), useDirect);
    }
  }

  /** Verify EOFException throw at EOF */

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void eofBoolean(boolean useDirect) {
    Assertions.assertThrows(EOFException.class, () -> newDecoderWithNoData(useDirect).readBoolean());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void eofInt(boolean useDirect) {
    Assertions.assertThrows(EOFException.class, () -> newDecoderWithNoData(useDirect).readInt());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void eofLong(boolean useDirect) {
    Assertions.assertThrows(EOFException.class, () -> newDecoderWithNoData(useDirect).readLong());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void eofFloat(boolean useDirect) {
    Assertions.assertThrows(EOFException.class, () -> newDecoderWithNoData(useDirect).readFloat());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void eofDouble(boolean useDirect) {
    Assertions.assertThrows(EOFException.class, () -> newDecoderWithNoData(useDirect).readDouble());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void eofBytes(boolean useDirect) {
    Assertions.assertThrows(EOFException.class, () -> newDecoderWithNoData(useDirect).readBytes(null));
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void eofString(boolean useDirect) {
    Assertions.assertThrows(EOFException.class, () -> newDecoderWithNoData(useDirect).readString(new Utf8("a")));
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void eofFixed(boolean useDirect) {
    Assertions.assertThrows(EOFException.class, () -> newDecoderWithNoData(useDirect).readFixed(new byte[1]));
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void eofEnum(boolean useDirect) {
    Assertions.assertThrows(EOFException.class, () -> newDecoderWithNoData(useDirect).readEnum());
  }

  @Test
  void reuse() throws IOException {
    ByteBufferOutputStream bbo1 = new ByteBufferOutputStream();
    ByteBufferOutputStream bbo2 = new ByteBufferOutputStream();
    byte[] b1 = new byte[] { 1, 2 };

    BinaryEncoder e1 = e_factory.binaryEncoder(bbo1, null);
    e1.writeBytes(b1);
    e1.flush();

    BinaryEncoder e2 = e_factory.binaryEncoder(bbo2, null);
    e2.writeBytes(b1);
    e2.flush();

    DirectBinaryDecoder d = new DirectBinaryDecoder(new ByteBufferInputStream(bbo1.getBufferList()));
    ByteBuffer bb1 = d.readBytes(null);
    Assertions.assertEquals(b1.length, bb1.limit() - bb1.position());

    d.configure(new ByteBufferInputStream(bbo2.getBufferList()));
    ByteBuffer bb2 = d.readBytes(null);
    Assertions.assertEquals(b1.length, bb2.limit() - bb2.position());

  }

  private static byte[] data = null;
  private static Schema schema = null;
  private static final int count = 200;
  private static final ArrayList<Object> records = new ArrayList<>(count);

  @BeforeAll
  public static void generateData() throws IOException {
    int seed = (int) System.currentTimeMillis();
    // note some tests (testSkipping) rely on this explicitly
    String jsonSchema = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": ["
        + "{\"name\":\"intField\", \"type\":\"int\"}," + "{\"name\":\"bytesField\", \"type\":\"bytes\"},"
        + "{\"name\":\"booleanField\", \"type\":\"boolean\"}," + "{\"name\":\"stringField\", \"type\":\"string\"},"
        + "{\"name\":\"floatField\", \"type\":\"float\"}," + "{\"name\":\"doubleField\", \"type\":\"double\"},"
        + "{\"name\":\"arrayField\", \"type\": " + "{\"type\":\"array\", \"items\":\"boolean\"}},"
        + "{\"name\":\"longField\", \"type\":\"long\"}]}";
    schema = new Schema.Parser().parse(jsonSchema);
    GenericDatumWriter<Object> writer = new GenericDatumWriter<>();
    writer.setSchema(schema);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
    BinaryEncoder encoder = e_factory.binaryEncoder(baos, null);

    for (Object datum : new RandomData(schema, count, seed)) {
      writer.write(datum, encoder);
      records.add(datum);
    }
    encoder.flush();
    data = baos.toByteArray();
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void decodeFromSources(boolean useDirect) throws IOException {
    GenericDatumReader<Object> reader = new GenericDatumReader<>();
    reader.setSchema(schema);

    ByteArrayInputStream is = new ByteArrayInputStream(data);
    ByteArrayInputStream is2 = new ByteArrayInputStream(data);
    ByteArrayInputStream is3 = new ByteArrayInputStream(data);

    Decoder fromInputStream = newDecoder(is, useDirect);
    Decoder fromArray = newDecoder(data, useDirect);

    byte[] data2 = new byte[data.length + 30];
    Arrays.fill(data2, (byte) 0xff);
    System.arraycopy(data, 0, data2, 15, data.length);

    Decoder fromOffsetArray = newDecoder(data2, 15, data.length, useDirect);

    BinaryDecoder initOnInputStream = newDecoder(new byte[50], 0, 30, useDirect);
    initOnInputStream = newDecoder(is2, initOnInputStream, useDirect);
    BinaryDecoder initOnArray = this.newDecoder(is3, null, useDirect);
    initOnArray = this.newDecoder(data, initOnArray, useDirect);

    for (Object datum : records) {
      Assertions.assertEquals(datum, reader.read(null, fromInputStream),
          "InputStream based BinaryDecoder result does not match");
      Assertions.assertEquals(datum, reader.read(null, fromArray), "Array based BinaryDecoder result does not match");
      Assertions.assertEquals(datum, reader.read(null, fromOffsetArray),
          "offset Array based BinaryDecoder result does not match");
      Assertions.assertEquals(datum, reader.read(null, initOnInputStream),
          "InputStream initialized BinaryDecoder result does not match");
      Assertions.assertEquals(datum, reader.read(null, initOnArray),
          "Array initialized BinaryDecoder result does not match");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void inputStreamProxy(boolean useDirect) throws IOException {
    BinaryDecoder d = newDecoder(data, useDirect);
    if (d != null) {
      BinaryDecoder bd = d;
      InputStream test = bd.inputStream();
      InputStream check = new ByteArrayInputStream(data);
      validateInputStreamReads(test, check);
      bd = this.newDecoder(data, bd, useDirect);
      test = bd.inputStream();
      check = new ByteArrayInputStream(data);
      validateInputStreamSkips(test, check);
      // with input stream sources
      bd = newDecoder(new ByteArrayInputStream(data), bd, useDirect);
      test = bd.inputStream();
      check = new ByteArrayInputStream(data);
      validateInputStreamReads(test, check);
      bd = newDecoder(new ByteArrayInputStream(data), bd, useDirect);
      test = bd.inputStream();
      check = new ByteArrayInputStream(data);
      validateInputStreamSkips(test, check);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void inputStreamProxyDetached(boolean useDirect) throws IOException {
    BinaryDecoder bd = newDecoder(data, useDirect);

    InputStream test = bd.inputStream();
    InputStream check = new ByteArrayInputStream(data);
    // detach input stream and decoder from old source
    this.newDecoder(new byte[56], useDirect);
    try (InputStream bad = bd.inputStream(); InputStream check2 = new ByteArrayInputStream(data)) {
      validateInputStreamReads(test, check);
      Assertions.assertNotEquals(bad.read(), check2.read());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void inputStreamPartiallyUsed(boolean useDirect) throws IOException {
    BinaryDecoder bd = this.newDecoder(new ByteArrayInputStream(data), useDirect);
    InputStream test = bd.inputStream();
    InputStream check = new ByteArrayInputStream(data);
    // triggers buffer fill if unused and tests isEnd()
    try {
      Assertions.assertFalse(bd.isEnd());
    } catch (UnsupportedOperationException e) {
      // this is ok if its a DirectBinaryDecoder.
      if (bd.getClass() != DirectBinaryDecoder.class) {
        throw e;
      }
    }
    bd.readFloat(); // use data, and otherwise trigger buffer fill
    check.skip(4); // skip the same # of bytes here
    validateInputStreamReads(test, check);
  }

  private void validateInputStreamReads(InputStream test, InputStream check) throws IOException {
    byte[] bt = new byte[7];
    byte[] bc = new byte[7];
    while (true) {
      int t = test.read();
      int c = check.read();
      Assertions.assertEquals(c, t);
      if (-1 == t) {
        break;
      }
      t = test.read(bt);
      c = check.read(bc);
      Assertions.assertEquals(c, t);
      Assertions.assertArrayEquals(bt, bc);
      if (-1 == t) {
        break;
      }
      t = test.read(bt, 1, 4);
      c = check.read(bc, 1, 4);
      Assertions.assertEquals(c, t);
      Assertions.assertArrayEquals(bt, bc);
      if (-1 == t) {
        break;
      }
    }
    Assertions.assertEquals(0, test.skip(5));
    Assertions.assertEquals(0, test.available());
    Assertions.assertFalse(test.getClass() != ByteArrayInputStream.class && test.markSupported());
    test.close();
  }

  private void validateInputStreamSkips(InputStream test, InputStream check) throws IOException {
    while (true) {
      long t2 = test.skip(19);
      long c2 = check.skip(19);
      Assertions.assertEquals(c2, t2);
      if (0 == t2) {
        break;
      }
    }
    Assertions.assertEquals(-1, test.read());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void badIntEncoding(boolean useDirect) throws IOException {
    byte[] badint = new byte[5];
    Arrays.fill(badint, (byte) 0xff);
    Decoder bd = this.newDecoder(badint, useDirect);
    String message = "";
    try {
      bd.readInt();
    } catch (IOException ioe) {
      message = ioe.getMessage();
    }
    Assertions.assertEquals("Invalid int encoding", message);
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void badLongEncoding(boolean useDirect) throws IOException {
    byte[] badint = new byte[10];
    Arrays.fill(badint, (byte) 0xff);
    Decoder bd = this.newDecoder(badint, useDirect);
    String message = "";
    try {
      bd.readLong();
    } catch (IOException ioe) {
      message = ioe.getMessage();
    }
    Assertions.assertEquals("Invalid long encoding", message);
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  public void testStringNegativeLength(boolean useDirect) throws IOException {
    Exception ex = Assertions.assertThrows(AvroRuntimeException.class, this.newDecoder(useDirect, -1L)::readString);
    Assertions.assertEquals(ERROR_NEGATIVE, ex.getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  public void testStringVmMaxSize(boolean useDirect) throws IOException {
    Exception ex = Assertions.assertThrows(UnsupportedOperationException.class,
        newDecoder(useDirect, MAX_ARRAY_VM_LIMIT + 1L)::readString);
    Assertions.assertEquals(ERROR_VM_LIMIT_STRING, ex.getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  public void testStringMaxCustom(boolean useDirect) throws IOException {
    try {
      System.setProperty(SystemLimitException.MAX_STRING_LENGTH_PROPERTY, Long.toString(128));
      resetLimits();
      Exception ex = Assertions.assertThrows(SystemLimitException.class, newDecoder(useDirect, 129)::readString);
      Assertions.assertEquals("String length 129 exceeds maximum allowed", ex.getMessage());
    } finally {
      System.clearProperty(SystemLimitException.MAX_STRING_LENGTH_PROPERTY);
      resetLimits();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  public void testBytesNegativeLength(boolean useDirect) throws IOException {
    Exception ex = Assertions.assertThrows(AvroRuntimeException.class,
        () -> this.newDecoder(useDirect, -1).readBytes(null));
    Assertions.assertEquals(ERROR_NEGATIVE, ex.getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  public void testBytesVmMaxSize(boolean useDirect) throws IOException {
    Exception ex = Assertions.assertThrows(UnsupportedOperationException.class,
        () -> this.newDecoder(useDirect, MAX_ARRAY_VM_LIMIT + 1).readBytes(null));
    Assertions.assertEquals(ERROR_VM_LIMIT_BYTES, ex.getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  public void testBytesMaxCustom(boolean useDirect) throws IOException {
    try {
      System.setProperty(SystemLimitException.MAX_BYTES_LENGTH_PROPERTY, Long.toString(128));
      resetLimits();
      Exception ex = Assertions.assertThrows(SystemLimitException.class,
          () -> newDecoder(useDirect, 129).readBytes(null));
      Assertions.assertEquals("Bytes length 129 exceeds maximum allowed", ex.getMessage());
    } finally {
      System.clearProperty(SystemLimitException.MAX_BYTES_LENGTH_PROPERTY);
      resetLimits();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  public void testArrayVmMaxSize(boolean useDirect) throws IOException {
    // At start
    Exception ex = Assertions.assertThrows(UnsupportedOperationException.class,
        () -> this.newDecoder(useDirect, MAX_ARRAY_VM_LIMIT + 1).readArrayStart());
    Assertions.assertEquals(ERROR_VM_LIMIT_COLLECTION, ex.getMessage());

    // Next
    ex = Assertions.assertThrows(UnsupportedOperationException.class,
        () -> this.newDecoder(useDirect, MAX_ARRAY_VM_LIMIT + 1).arrayNext());
    Assertions.assertEquals(ERROR_VM_LIMIT_COLLECTION, ex.getMessage());

    // An OK reads followed by an overflow
    Decoder bd = newDecoder(useDirect, MAX_ARRAY_VM_LIMIT - 100, Long.MAX_VALUE);
    Assertions.assertEquals(MAX_ARRAY_VM_LIMIT - 100, bd.readArrayStart());
    ex = Assertions.assertThrows(UnsupportedOperationException.class, bd::arrayNext);
    Assertions.assertEquals(ERROR_VM_LIMIT_COLLECTION, ex.getMessage());

    // Two OK reads followed by going over the VM limit.
    bd = newDecoder(useDirect, MAX_ARRAY_VM_LIMIT - 100, 100, 1);
    Assertions.assertEquals(MAX_ARRAY_VM_LIMIT - 100, bd.readArrayStart());
    Assertions.assertEquals(100, bd.arrayNext());
    ex = Assertions.assertThrows(UnsupportedOperationException.class, bd::arrayNext);
    Assertions.assertEquals(ERROR_VM_LIMIT_COLLECTION, ex.getMessage());

    // Two OK reads followed by going over the VM limit, where negative numbers are
    // followed by the byte length of the items. For testing, the 999 values are
    // read but ignored.
    bd = newDecoder(useDirect, 100 - MAX_ARRAY_VM_LIMIT, 999, -100, 999, 1);
    Assertions.assertEquals(MAX_ARRAY_VM_LIMIT - 100, bd.readArrayStart());
    Assertions.assertEquals(100, bd.arrayNext());
    ex = Assertions.assertThrows(UnsupportedOperationException.class, bd::arrayNext);
    Assertions.assertEquals(ERROR_VM_LIMIT_COLLECTION, ex.getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  public void testArrayMaxCustom(boolean useDirect) throws IOException {
    try {
      System.setProperty(SystemLimitException.MAX_COLLECTION_LENGTH_PROPERTY, Long.toString(128));
      resetLimits();
      Exception ex = Assertions.assertThrows(UnsupportedOperationException.class,
          () -> newDecoder(useDirect, MAX_ARRAY_VM_LIMIT + 1).readArrayStart());
      Assertions.assertEquals(ERROR_VM_LIMIT_COLLECTION, ex.getMessage());

      // Two OK reads followed by going over the custom limit.
      Decoder bd = newDecoder(useDirect, 118, 10, 1);
      Assertions.assertEquals(118, bd.readArrayStart());
      Assertions.assertEquals(10, bd.arrayNext());
      ex = Assertions.assertThrows(SystemLimitException.class, bd::arrayNext);
      Assertions.assertEquals("Collection length 129 exceeds maximum allowed", ex.getMessage());

      // Two OK reads followed by going over the VM limit, where negative numbers are
      // followed by the byte length of the items. For testing, the 999 values are
      // read but ignored.
      bd = newDecoder(useDirect, -118, 999, -10, 999, 1);
      Assertions.assertEquals(118, bd.readArrayStart());
      Assertions.assertEquals(10, bd.arrayNext());
      ex = Assertions.assertThrows(SystemLimitException.class, bd::arrayNext);
      Assertions.assertEquals("Collection length 129 exceeds maximum allowed", ex.getMessage());

    } finally {
      System.clearProperty(SystemLimitException.MAX_COLLECTION_LENGTH_PROPERTY);
      resetLimits();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  public void testMapVmMaxSize(boolean useDirect) throws IOException {
    // At start
    Exception ex = Assertions.assertThrows(UnsupportedOperationException.class,
        () -> this.newDecoder(useDirect, MAX_ARRAY_VM_LIMIT + 1).readMapStart());
    Assertions.assertEquals(ERROR_VM_LIMIT_COLLECTION, ex.getMessage());

    // Next
    ex = Assertions.assertThrows(UnsupportedOperationException.class,
        () -> this.newDecoder(useDirect, MAX_ARRAY_VM_LIMIT + 1).mapNext());
    Assertions.assertEquals(ERROR_VM_LIMIT_COLLECTION, ex.getMessage());

    // Two OK reads followed by going over the VM limit.
    Decoder bd = newDecoder(useDirect, MAX_ARRAY_VM_LIMIT - 100, 100, 1);
    Assertions.assertEquals(MAX_ARRAY_VM_LIMIT - 100, bd.readMapStart());
    Assertions.assertEquals(100, bd.mapNext());
    ex = Assertions.assertThrows(UnsupportedOperationException.class, bd::mapNext);
    Assertions.assertEquals(ERROR_VM_LIMIT_COLLECTION, ex.getMessage());

    // Two OK reads followed by going over the VM limit, where negative numbers are
    // followed by the byte length of the items. For testing, the 999 values are
    // read but ignored.
    bd = newDecoder(useDirect, 100 - MAX_ARRAY_VM_LIMIT, 999, -100, 999, 1);
    Assertions.assertEquals(MAX_ARRAY_VM_LIMIT - 100, bd.readMapStart());
    Assertions.assertEquals(100, bd.mapNext());
    ex = Assertions.assertThrows(UnsupportedOperationException.class, bd::mapNext);
    Assertions.assertEquals(ERROR_VM_LIMIT_COLLECTION, ex.getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  public void testMapMaxCustom(boolean useDirect) throws IOException {
    try {
      System.setProperty(SystemLimitException.MAX_COLLECTION_LENGTH_PROPERTY, Long.toString(128));
      resetLimits();
      Exception ex = Assertions.assertThrows(UnsupportedOperationException.class,
          () -> newDecoder(useDirect, MAX_ARRAY_VM_LIMIT + 1).readMapStart());
      Assertions.assertEquals(ERROR_VM_LIMIT_COLLECTION, ex.getMessage());

      // Two OK reads followed by going over the custom limit.
      Decoder bd = newDecoder(useDirect, 118, 10, 1);
      Assertions.assertEquals(118, bd.readMapStart());
      Assertions.assertEquals(10, bd.mapNext());
      ex = Assertions.assertThrows(SystemLimitException.class, bd::mapNext);
      Assertions.assertEquals("Collection length 129 exceeds maximum allowed", ex.getMessage());

      // Two OK reads followed by going over the VM limit, where negative numbers are
      // followed by the byte length of the items. For testing, the 999 values are
      // read but ignored.
      bd = newDecoder(useDirect, -118, 999, -10, 999, 1);
      Assertions.assertEquals(118, bd.readMapStart());
      Assertions.assertEquals(10, bd.mapNext());
      ex = Assertions.assertThrows(SystemLimitException.class, bd::mapNext);
      Assertions.assertEquals("Collection length 129 exceeds maximum allowed", ex.getMessage());

    } finally {
      System.clearProperty(SystemLimitException.MAX_COLLECTION_LENGTH_PROPERTY);
      resetLimits();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void longLengthEncoding(boolean useDirect) {
    // Size equivalent to Integer.MAX_VALUE + 1
    byte[] bad = new byte[] { (byte) -128, (byte) -128, (byte) -128, (byte) -128, (byte) 16 };
    Decoder bd = this.newDecoder(bad, useDirect);
    Assertions.assertThrows(UnsupportedOperationException.class, bd::readString);
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void intTooShort(boolean useDirect) {
    byte[] badint = new byte[4];
    Arrays.fill(badint, (byte) 0xff);
    Assertions.assertThrows(EOFException.class, () -> newDecoder(badint, useDirect).readInt());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void longTooShort(boolean useDirect) {
    byte[] badint = new byte[9];
    Arrays.fill(badint, (byte) 0xff);
    Assertions.assertThrows(EOFException.class, () -> newDecoder(badint, useDirect).readLong());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void floatTooShort(boolean useDirect) {
    byte[] badint = new byte[3];
    Arrays.fill(badint, (byte) 0xff);
    Assertions.assertThrows(EOFException.class, () -> newDecoder(badint, useDirect).readInt());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void doubleTooShort(boolean useDirect) {
    byte[] badint = new byte[7];
    Arrays.fill(badint, (byte) 0xff);
    Assertions.assertThrows(EOFException.class, () -> newDecoder(badint, useDirect).readLong());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void skipping(boolean useDirect) throws IOException {
    BinaryDecoder bd = newDecoder(data, useDirect);
    skipGenerated(bd);

    try {
      Assertions.assertTrue(bd.isEnd());
    } catch (UnsupportedOperationException e) {
      // this is ok if its a DirectBinaryDecoder.
      if (bd.getClass() != DirectBinaryDecoder.class) {
        throw e;
      }
    }
    bd = this.newDecoder(new ByteArrayInputStream(data), bd, useDirect);
    skipGenerated(bd);
    try {
      Assertions.assertTrue(bd.isEnd());
    } catch (UnsupportedOperationException e) {
      // this is ok if its a DirectBinaryDecoder.
      if (bd.getClass() != DirectBinaryDecoder.class) {
        throw e;
      }
    }

  }

  private void skipGenerated(Decoder bd) throws IOException {
    for (int i = 0; i < records.size(); i++) {
      bd.readInt();
      bd.skipBytes();
      bd.skipFixed(1);
      bd.skipString();
      bd.skipFixed(4);
      bd.skipFixed(8);
      long leftover = bd.skipArray();
      // booleans are one byte, array trailer is one byte
      bd.skipFixed((int) leftover + 1);
      bd.skipFixed(0);
      bd.readLong();
    }
    EOFException eof = null;
    try {
      bd.skipFixed(4);
    } catch (EOFException e) {
      eof = e;
    }
    Assertions.assertNotNull(eof);
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void eof(boolean useDirect) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder e = EncoderFactory.get().binaryEncoder(baos, null);
    e.writeLong(0x10000000000000L);
    e.flush();

    Decoder d = newDecoder(new ByteArrayInputStream(baos.toByteArray()), useDirect);
    Assertions.assertEquals(0x10000000000000L, d.readLong());
    Assertions.assertThrows(EOFException.class, () -> d.readInt());
  }

  @Test
  void testFloatPrecision() throws Exception {
    String def = "{\"type\":\"record\",\"name\":\"X\",\"fields\":" + "[{\"type\":\"float\",\"name\":\"n\"}]}";
    Schema schema = new Schema.Parser().parse(def);
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

    float value = 33.33000183105469f;

    GenericData.Record record = new GenericData.Record(schema);
    record.put(0, value);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);

    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();

    Decoder decoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(out.toByteArray()), null);
    GenericRecord r = reader.read(null, decoder);
    assertEquals(value + 0d, ((float) r.get("n")) + 0d);
  }

}
