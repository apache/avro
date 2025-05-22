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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.TestReadingWritingDataInEvolvedSchemas;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.Schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

  @ParameterizedTest
  @ValueSource(strings = { "true", "false" })
  public void simple(String fastReaderProp) throws IOException {
    final String current = System.getProperty(GenericData.FAST_READER_PROP);
    try {
      System.setProperty(GenericData.FAST_READER_PROP, fastReaderProp);
      Schema schema = Schema.createRecord("Person", "doc", "foo", false,
          Arrays.asList(new Schema.Field("lastname", Schema.create(Schema.Type.STRING), "doc", "Bond")));

      String data = "{}";
      byte[] toAvro = this.convertJsonToAvro(data.getBytes(), schema);

      final IndexedRecord record = this.readOneRecord(schema, toAvro);
      Assertions.assertEquals("Bond", record.get(schema.getField("lastname").pos()).toString());
    } finally {
      if (current == null) {
        System.clearProperty(GenericData.FAST_READER_PROP);
      } else {
        System.setProperty(GenericData.FAST_READER_PROP, current);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "true", "false" })
  void wrong(String fastReaderProp) {
    final String current = System.getProperty(GenericData.FAST_READER_PROP);
    try {
      System.setProperty(GenericData.FAST_READER_PROP, fastReaderProp);
      Schema schema = Schema.createRecord("Person", "doc", "foo", false,
          Arrays.asList(new Schema.Field("lastname", Schema.create(Schema.Type.STRING), "doc", "last")));

      String data = "{ \"lastname\": 123 }";
      AvroTypeException exception = Assertions.assertThrows(AvroTypeException.class,
          () -> this.convertJsonToAvro(data.getBytes(), schema));

    } finally {
      if (current == null) {
        System.clearProperty(GenericData.FAST_READER_PROP);
      } else {
        System.setProperty(GenericData.FAST_READER_PROP, current);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "true", "false" })
  public void multiField(String fastReaderProp) throws IOException {
    final String current = System.getProperty(GenericData.FAST_READER_PROP);
    try {
      System.setProperty(GenericData.FAST_READER_PROP, fastReaderProp);
      Schema schema = Schema.createRecord("Person", "doc", "foo", false,
          Arrays.asList(new Schema.Field("lastname", Schema.create(Schema.Type.STRING), "doc", "Bond"),
              new Schema.Field("firstname", Schema.create(Schema.Type.STRING), "doc", "James")));

      String data = "{\"lastname\": \"Doe\" }";
      byte[] toAvro = this.convertJsonToAvro(data.getBytes(), schema);

      final IndexedRecord record = this.readOneRecord(schema, toAvro);
      assertEquals("James", record.get(schema.getField("firstname").pos()).toString());
      assertEquals("Doe", record.get(schema.getField("lastname").pos()).toString());
    } finally {
      if (current == null) {
        System.clearProperty(GenericData.FAST_READER_PROP);
      } else {
        System.setProperty(GenericData.FAST_READER_PROP, current);
      }
    }
  }

  private IndexedRecord readOneRecord(Schema schema, byte[] avroBinaryData) throws IOException {
    GenericDatumReader<Object> reader = new GenericDatumReader<>();
    reader.setExpected(schema);
    reader.setSchema(schema);
    DataInputStream din = new DataInputStream(new ByteArrayInputStream(avroBinaryData));
    Decoder decoder = DecoderFactory.get().binaryDecoder(din, null);

    final Object record = reader.read(null, decoder);
    Assertions.assertNotNull(record);
    Assertions.assertTrue(record instanceof IndexedRecord);
    return (IndexedRecord) record;
  }

  public byte[] convertJsonToAvro(byte[] data, Schema schema) {

    DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    try (InputStream input = new ByteArrayInputStream(data); DataInputStream din = new DataInputStream(input)) {

      ByteArrayOutputStream output = new ByteArrayOutputStream();
      GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
      Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);

      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
      GenericRecord datum = null;
      while (true) {
        try {
          datum = reader.read(null, decoder);
        } catch (EOFException eofe) {
          break;
        }
        writer.write(datum, encoder);
      }
      encoder.flush();

      output.flush();
      return output.toByteArray();
    } catch (IOException e1) {
      throw new RuntimeException("Error decoding Json " + e1.getMessage(), e1);
    }
  }
}
