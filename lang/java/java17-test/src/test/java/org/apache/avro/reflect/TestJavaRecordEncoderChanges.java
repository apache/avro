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

package org.apache.avro.reflect;

import org.apache.avro.AvroMissingFieldException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

import static org.junit.Assert.*;

public class TestJavaRecordEncoderChanges {

  @Test
  public void testBase() throws IOException {
    Base in = new Base("hello world", 42);
    byte[] encoded = write(in, Base.class);
    Base decoded = read(encoded);

    assertNotNull(decoded);
    assertEquals("hello world", decoded.field());
    assertEquals(42, decoded.field2());

  }

  @ParameterizedTest
  @ValueSource(classes = { BaseTypeChangeCompatible.class, BaseTypeChangeCompatibleClass.class })
  public void testCompatibleTypeChange(Class<?> type) throws IOException {
    Base in = new Base("hello world", 42);
    byte[] encoded = write(in, type);
    Compatible decoded = read(encoded);

    assertNotNull(decoded);
    assertEquals("hello world", decoded.field());
    assertEquals(42, decoded.field2(), 0);

  }

  @ParameterizedTest
  @ValueSource(classes = { BaseTypeChangeCastable.class, BaseTypeChangeCastableClass.class })
  public void testCastableTypeChange(Class<?> type) {
    Base in = new Base("hello world", 42);
    byte[] encoded = write(in, type);
    assertThrows(IllegalArgumentException.class, () -> read(encoded));
  }

  @ParameterizedTest
  @ValueSource(classes = { BaseWithDefault.class, BaseWithDefaultClass.class })
  public void testWithDefault(Class<?> type) throws IOException {
    Base in = new Base("hello world", 42);
    byte[] encoded = write(in, type);
    Default decoded = read(encoded);

    assertNotNull(decoded);
    assertEquals("hello world", decoded.field());
    assertEquals(42, decoded.field2());
    assertEquals(0, decoded.field3(), 0); // avro default annotation seems to get ignored, matching class behaviour
    assertNull(decoded.field4()); // avro default annotation seems to get ignored, matching class behaviour

  }

  @Test
  public void testWithRemovedField() {
    Base in = new Base("hello world", 42);
    byte[] encoded = write(in, BaseWithFieldRemoved.class);
    assertThrows(AvroMissingFieldException.class, () -> read(encoded));
  }

  @Test
  public void testWithRemovedFieldClass() {
    Base in = new Base("hello world", 42);
    byte[] encoded = write(in, BaseWithFieldRemovedClass.class);
    assertThrows(NullPointerException.class, () -> read(encoded)); // exceptions don't match
  }

  private <T> T read(byte[] toDecode) throws IOException {
    DatumReader<T> datumReader = new ReflectDatumReader<>();
    try (DataFileStream<T> dataFileReader = new DataFileStream<>(new ByteArrayInputStream(toDecode, 0, toDecode.length),
        datumReader);) {
      dataFileReader.hasNext();
      return dataFileReader.next();
    }
  }

  private <T> byte[] write(T custom, Class<?> asName) {
    var schema = ReflectData.get().getSchema(custom.getClass());

    var schemaAs = ReflectData.get().getSchema(asName);

    var fields = schema.getFields().stream()
        .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order()))
        .toList();

    schemaAs = Schema.createRecord(schemaAs.getName(), schemaAs.getDoc(), schemaAs.getNamespace(), schemaAs.isError(),
        fields);

    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>();

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataFileWriter<T> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(schemaAs, baos);
      datumWriter.setSchema(schema);
      writer.append(custom);
      writer.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public record Base(String field, long field2) {
  }

  public record BaseTypeChangeCastable(String field, int field2) {
  }

  interface Default {
    String field();

    long field2();

    double field3();

    String field4();

  }

  public record BaseWithDefault(String field, long field2, @AvroDefault("7.6") double field3,
      @AvroDefault("\"Not a primitive\"") String field4) implements Default {
  }

  public static class BaseWithDefaultClass implements Default {
    String field;
    long field2;
    @AvroDefault("7.6")
    double field3;
    @AvroDefault("\"Not a primitive\"")
    String field4;

    @Override
    public String field() {
      return field;
    }

    @Override
    public long field2() {
      return field2;
    }

    @Override
    public double field3() {
      return field3;
    }

    @Override
    public String field4() {
      return field4;
    }
  }

  public record BaseWithFieldRemoved(String field) {
  }

  interface Compatible {
    String field();

    double field2();
  }

  public record BaseTypeChangeCompatible(String field, double field2) implements Compatible {
  }

  // to compare to class behaviour
  public static class BaseTypeChangeCompatibleClass implements Compatible {
    String field;
    double field2;

    @Override
    public String field() {
      return field;
    }

    @Override
    public double field2() {
      return field2;
    }
  }

  public static class BaseTypeChangeCastableClass {
    String field;
    int field2;
  }

  public static class BaseWithFieldRemovedClass {
    String field;
  }

}
