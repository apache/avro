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

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestEncoders {
  private static final int ENCODER_BUFFER_SIZE = 32;
  private static final int EXAMPLE_DATA_SIZE = 17;

  private static final EncoderFactory FACTORY = EncoderFactory.get();

  @TempDir
  public Path dataDir;

  @Test
  void binaryEncoderInit() throws IOException {
    OutputStream out = new ByteArrayOutputStream();
    BinaryEncoder enc = FACTORY.binaryEncoder(out, null);
    assertSame(enc, FACTORY.binaryEncoder(out, enc));
  }

  @Test
  void badBinaryEncoderInit() {
    assertThrows(NullPointerException.class, () -> {
      FACTORY.binaryEncoder(null, null);
    });
  }

  @Test
  void blockingBinaryEncoderInit() throws IOException {
    OutputStream out = new ByteArrayOutputStream();
    BinaryEncoder reuse = null;
    reuse = FACTORY.blockingBinaryEncoder(out, reuse);
    assertSame(reuse, FACTORY.blockingBinaryEncoder(out, reuse));
    // comparison
  }

  @Test
  void badBlockintBinaryEncoderInit() {
    assertThrows(NullPointerException.class, () -> {
      FACTORY.binaryEncoder(null, null);
    });
  }

  @Test
  void directBinaryEncoderInit() throws IOException {
    OutputStream out = new ByteArrayOutputStream();
    BinaryEncoder enc = FACTORY.directBinaryEncoder(out, null);
    assertSame(enc, FACTORY.directBinaryEncoder(out, enc));
  }

  @Test
  void badDirectBinaryEncoderInit() {
    assertThrows(NullPointerException.class, () -> {
      FACTORY.directBinaryEncoder(null, null);
    });
  }

  @Test
  void blockingDirectBinaryEncoderInit() throws IOException {
    OutputStream out = new ByteArrayOutputStream();
    BinaryEncoder enc = FACTORY.blockingDirectBinaryEncoder(out, null);
    assertSame(enc, FACTORY.blockingDirectBinaryEncoder(out, enc));
  }

  @Test
  void badBlockingDirectBinaryEncoderInit() {
    assertThrows(NullPointerException.class, () -> {
      FACTORY.blockingDirectBinaryEncoder(null, null);
    });
  }

  @Test
  void jsonEncoderInit() throws IOException {
    Schema s = new Schema.Parser().parse("\"int\"");
    OutputStream out = new ByteArrayOutputStream();
    FACTORY.jsonEncoder(s, out);
    JsonEncoder enc = FACTORY.jsonEncoder(s, new JsonFactory().createGenerator(out, JsonEncoding.UTF8));
    enc.configure(out);
  }

  @Test
  void badJsonEncoderInitOS() throws IOException {
    assertThrows(NullPointerException.class, () -> {
      FACTORY.jsonEncoder(Schema.create(Type.INT), (OutputStream) null);
    });
  }

  @Test
  void badJsonEncoderInit() throws IOException {
    assertThrows(NullPointerException.class, () -> {
      FACTORY.jsonEncoder(Schema.create(Type.INT), (JsonGenerator) null);
    });
  }

  @Test
  void jsonEncoderNewlineDelimited() throws IOException {
    OutputStream out = new ByteArrayOutputStream();
    Schema ints = Schema.create(Type.INT);
    Encoder e = FACTORY.jsonEncoder(ints, out);
    String separator = System.getProperty("line.separator");
    GenericDatumWriter<Integer> writer = new GenericDatumWriter<>(ints);
    writer.write(1, e);
    writer.write(2, e);
    e.flush();
    assertEquals("1" + separator + "2", out.toString());
  }

  @Test
  void jsonEncoderWhenIncludeNamespaceOptionIsFalse() throws IOException {
    String value = "{\"b\": {\"string\":\"myVal\"}, \"a\": 1}";
    String schemaStr = "{\"type\": \"record\", \"name\": \"ab\", \"fields\": ["
        + "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": [\"null\", \"string\"]}" + "]}";
    Schema schema = new Schema.Parser().parse(schemaStr);
    byte[] avroBytes = fromJsonToAvro(value, schema);
    ObjectMapper mapper = new ObjectMapper();

    assertEquals(mapper.readTree("{\"b\":\"myVal\",\"a\":1}"),
        mapper.readTree(fromAvroToJson(avroBytes, schema, false)));
  }

  @Test
  void jsonEncoderWhenIncludeNamespaceOptionIsTrue() throws IOException {
    String value = "{\"b\": {\"string\":\"myVal\"}, \"a\": 1}";
    String schemaStr = "{\"type\": \"record\", \"name\": \"ab\", \"fields\": ["
        + "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": [\"null\", \"string\"]}" + "]}";
    Schema schema = new Schema.Parser().parse(schemaStr);
    byte[] avroBytes = fromJsonToAvro(value, schema);
    ObjectMapper mapper = new ObjectMapper();

    assertEquals(mapper.readTree("{\"b\":{\"string\":\"myVal\"},\"a\":1}"),
        mapper.readTree(fromAvroToJson(avroBytes, schema, true)));
  }

  @Test
  void validatingEncoderInit() throws IOException {
    Schema s = new Schema.Parser().parse("\"int\"");
    OutputStream out = new ByteArrayOutputStream();
    Encoder e = FACTORY.directBinaryEncoder(out, null);
    FACTORY.validatingEncoder(s, e).configure(e);
  }

  @Test
  void jsonRecordOrdering() throws IOException {
    String value = "{\"b\": 2, \"a\": 1}";
    Schema schema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": ["
        + "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": \"int\"}" + "]}");
    GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, value);
    Object o = reader.read(null, decoder);
    assertEquals("{\"a\": 1, \"b\": 2}", o.toString());
  }

  @Test
  void jsonExcessFields() throws IOException {
    assertThrows(AvroTypeException.class, () -> {
      String value = "{\"b\": { \"b3\": 1.4, \"b2\": 3.14, \"b1\": \"h\"}, \"a\": {\"a0\": 45, \"a2\":true, \"a1\": null}}";
      Schema schema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n"
          + "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n"
          + "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}},\n"
          + "{\"name\": \"b\", \"type\": {\"type\":\"record\",\"name\":\"B\",\"fields\":\n"
          + "[{\"name\":\"b1\", \"type\":\"string\"}, {\"name\":\"b2\", \"type\":\"float\"}, {\"name\":\"b3\", \"type\":\"double\"}]}}\n"
          + "]}");
      GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, value);
      reader.read(null, decoder);
    });
  }

  @Test
  void jsonRecordOrdering2() throws IOException {
    String value = "{\"b\": { \"b3\": 1.4, \"b2\": 3.14, \"b1\": \"h\"}, \"a\": {\"a2\":true, \"a1\": null}}";
    Schema schema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n"
        + "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n"
        + "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}},\n"
        + "{\"name\": \"b\", \"type\": {\"type\":\"record\",\"name\":\"B\",\"fields\":\n"
        + "[{\"name\":\"b1\", \"type\":\"string\"}, {\"name\":\"b2\", \"type\":\"float\"}, {\"name\":\"b3\", \"type\":\"double\"}]}}\n"
        + "]}");
    GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, value);
    Object o = reader.read(null, decoder);
    assertEquals("{\"a\": {\"a1\": null, \"a2\": true}, \"b\": {\"b1\": \"h\", \"b2\": 3.14, \"b3\": 1.4}}",
        o.toString());
  }

  @Test
  void jsonRecordOrderingWithProjection() throws IOException {
    String value = "{\"b\": { \"b3\": 1.4, \"b2\": 3.14, \"b1\": \"h\"}, \"a\": {\"a2\":true, \"a1\": null}}";
    Schema writerSchema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n"
        + "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n"
        + "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}},\n"
        + "{\"name\": \"b\", \"type\": {\"type\":\"record\",\"name\":\"B\",\"fields\":\n"
        + "[{\"name\":\"b1\", \"type\":\"string\"}, {\"name\":\"b2\", \"type\":\"float\"}, {\"name\":\"b3\", \"type\":\"double\"}]}}\n"
        + "]}");
    Schema readerSchema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n"
        + "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n"
        + "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}}\n" + "]}");
    GenericDatumReader<Object> reader = new GenericDatumReader<>(writerSchema, readerSchema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(writerSchema, value);
    Object o = reader.read(null, decoder);
    assertEquals("{\"a\": {\"a1\": null, \"a2\": true}}", o.toString());
  }

  @Test
  void jsonRecordOrderingWithProjection2() throws IOException {
    String value = "{\"b\": { \"b1\": \"h\", \"b2\": [3.14, 3.56], \"b3\": 1.4}, \"a\": {\"a2\":true, \"a1\": null}}";
    Schema writerSchema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n"
        + "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n"
        + "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}},\n"
        + "{\"name\": \"b\", \"type\": {\"type\":\"record\",\"name\":\"B\",\"fields\":\n"
        + "[{\"name\":\"b1\", \"type\":\"string\"}, {\"name\":\"b2\", \"type\":{\"type\":\"array\", \"items\":\"float\"}}, {\"name\":\"b3\", \"type\":\"double\"}]}}\n"
        + "]}");
    Schema readerSchema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n"
        + "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n"
        + "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}}\n" + "]}");
    GenericDatumReader<Object> reader = new GenericDatumReader<>(writerSchema, readerSchema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(writerSchema, value);
    Object o = reader.read(null, decoder);
    assertEquals("{\"a\": {\"a1\": null, \"a2\": true}}", o.toString());
  }

  @Test
  void arrayBackedByteBuffer() throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(someBytes(EXAMPLE_DATA_SIZE));

    testWithBuffer(buffer);
  }

  @Test
  void mappedByteBuffer() throws IOException {
    Path file = dataDir.resolve("testMappedByteBuffer.avro");
    Files.write(file, someBytes(EXAMPLE_DATA_SIZE));
    MappedByteBuffer buffer = FileChannel.open(file, StandardOpenOption.READ).map(FileChannel.MapMode.READ_ONLY, 0,
        EXAMPLE_DATA_SIZE);

    testWithBuffer(buffer);
  }

  private void testWithBuffer(ByteBuffer buffer) throws IOException {
    assertThat(asList(buffer.position(), buffer.remaining()), is(asList(0, EXAMPLE_DATA_SIZE)));

    ByteArrayOutputStream output = new ByteArrayOutputStream(EXAMPLE_DATA_SIZE * 2);
    EncoderFactory encoderFactory = new EncoderFactory();
    encoderFactory.configureBufferSize(ENCODER_BUFFER_SIZE);

    Encoder encoder = encoderFactory.binaryEncoder(output, null);
    new GenericDatumWriter<ByteBuffer>(Schema.create(Schema.Type.BYTES)).write(buffer, encoder);
    encoder.flush();

    assertThat(output.toByteArray(), equalTo(avroEncoded(someBytes(EXAMPLE_DATA_SIZE))));
    assertThat(asList(buffer.position(), buffer.remaining()), is(asList(0, EXAMPLE_DATA_SIZE))); // fails if buffer is
    // not array-backed and
    // buffer overflow
    // occurs
  }

  private byte[] someBytes(int size) {
    byte[] result = new byte[size];
    for (int i = 0; i < size; i++) {
      result[i] = (byte) i;
    }
    return result;
  }

  private byte[] avroEncoded(byte[] bytes) {
    assert bytes.length < 64;
    byte[] result = new byte[1 + bytes.length];
    result[0] = (byte) (bytes.length * 2); // zig-zag encoding
    System.arraycopy(bytes, 0, result, 1, bytes.length);
    return result;
  }

  private byte[] fromJsonToAvro(String json, Schema schema) throws IOException {
    DatumReader<Object> reader = new GenericDatumReader<>(schema);
    GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
    Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);

    Object datum = reader.read(null, decoder);

    writer.write(datum, encoder);
    encoder.flush();

    return output.toByteArray();
  }

  private String fromAvroToJson(byte[] avroBytes, Schema schema, boolean includeNamespace) throws IOException {
    GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
    DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    JsonEncoder encoder = FACTORY.jsonEncoder(schema, output);
    encoder.setIncludeNamespace(includeNamespace);
    Decoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);
    Object datum = reader.read(null, decoder);
    writer.write(datum, encoder);
    encoder.flush();
    output.flush();

    return new String(output.toByteArray(), StandardCharsets.UTF_8.name());
  }

  @Test
  public void testJsonEncoderInitAutoFlush() throws IOException {
    Schema s = new Schema.Parser().parse("\"int\"");
    OutputStream baos = new ByteArrayOutputStream();
    OutputStream out = new BufferedOutputStream(baos);
    JsonEncoder enc = FACTORY.jsonEncoder(s, out, false);
    enc.configure(out, false);
    enc.writeInt(24);
    enc.flush();
    assertEquals("", baos.toString());
    out.flush();
    assertEquals("24", baos.toString());
  }

  @Test
  public void testJsonEncoderInitAutoFlushDisabled() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStream out = new BufferedOutputStream(baos);
    Schema ints = Schema.create(Type.INT);
    Encoder e = FACTORY.jsonEncoder(ints, out, false, false);
    String separator = System.getProperty("line.separator");
    GenericDatumWriter<Integer> writer = new GenericDatumWriter<Integer>(ints);
    writer.write(1, e);
    writer.write(2, e);
    e.flush();
    assertEquals("", baos.toString());
    out.flush();
    assertEquals("1" + separator + "2", baos.toString());
    out.close();
  }
}
