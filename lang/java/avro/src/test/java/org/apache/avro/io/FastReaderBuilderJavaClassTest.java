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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/**
 * Tests for FastReaderBuilder behavior with schemas containing "java-class"
 * attributes.
 */
public class FastReaderBuilderJavaClassTest {

  /**
   * Tests that GenericDatumReader can deserialize records with string fields that
   * have a "java-class" attribute (e.g., BigDecimal).
   *
   * This test reproduces a bug where
   * FastReaderBuilder.getTransformingStringReader() casts the result of
   * stringReader.read() directly to String, but in GenericData mode the reader
   * returns Utf8, causing a ClassCastException.
   */
  @Test
  void genericDatumReaderWithJavaClassAttribute() throws IOException {
    // Schema with a string field that has "java-class": "java.math.BigDecimal"
    // This is a common pattern for representing decimal values as strings
    String schemaJson = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestRecord\",\n" + "  \"fields\": [\n"
        + "    {\"name\": \"id\", \"type\": \"string\"},\n" + "    {\"name\": \"price\", \"type\": [\"null\", {\n"
        + "      \"type\": \"string\",\n" + "      \"java-class\": \"java.math.BigDecimal\"\n" + "    }]}\n" + "  ]\n"
        + "}";

    Schema schema = new Schema.Parser().parse(schemaJson);

    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "123");
    record.put("price", "-0.0002");

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(record, encoder);
    encoder.flush();

    byte[] serialized = out.toByteArray();

    // Deserialize using GenericDatumReader (which uses FastReaderBuilder by
    // default)
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(serialized, null);

    // AVRO-4225 this should not throw ClassCastException: Utf8 cannot be cast
    // to String
    GenericRecord result = reader.read(null, decoder);

    assertNotNull(result);
    assertEquals("123", result.get("id").toString());
    assertEquals("-0.0002", result.get("price").toString());
  }

  /**
   * Tests that GenericDatumReader can deserialize records with a direct string
   * field (not in a union) that has a "java-class" attribute.
   */
  @Test
  void genericDatumReaderWithDirectJavaClassString() throws IOException {
    String schemaJson = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestRecord\",\n" + "  \"fields\": [\n"
        + "    {\"name\": \"amount\", \"type\": {\n" + "      \"type\": \"string\",\n"
        + "      \"java-class\": \"java.math.BigDecimal\"\n" + "    }}\n" + "  ]\n" + "}";

    Schema schema = new Schema.Parser().parse(schemaJson);

    GenericRecord record = new GenericData.Record(schema);
    record.put("amount", "123.45");

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(record, encoder);
    encoder.flush();

    byte[] serialized = out.toByteArray();

    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(serialized, null);

    GenericRecord result = reader.read(null, decoder);

    assertNotNull(result);
    assertEquals("123.45", result.get("amount").toString());
  }
}
