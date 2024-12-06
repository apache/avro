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

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class TestJsonDecoder {

  @Test
  void testInt() throws Exception {
    checkNumeric("int", 1);
  }

  @Test
  void testLong() throws Exception {
    checkNumeric("long", 1L);
  }

  @Test
  void testFloat() throws Exception {
    checkNumeric("float", 1.0F);
  }

  @Test
  void testDouble() throws Exception {
    checkNumeric("double", 1.0);
  }

  private void checkNumeric(String type, Object value) throws Exception {
    String def = "{\"type\":\"record\",\"name\":\"X\",\"fields\":" + "[{\"type\":\"" + type + "\",\"name\":\"n\"}]}";
    Schema schema = new Schema.Parser().parse(def);
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

    String[] records = { "{\"n\":1}", "{\"n\":1.0}" };

    for (String record : records) {
      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, record);
      GenericRecord r = reader.read(null, decoder);
      assertEquals(value, r.get("n"));
    }
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
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out);

    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();
    // check the whole float precision is kept.
    assertEquals("{\"n\":33.33000183105469}", out.toString());

    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, out.toString());
    GenericRecord r = reader.read(null, decoder);
    assertEquals(value + 0d, ((float) r.get("n")) + 0d);
  }

  // Ensure that even if the order of fields in JSON is different from the order
  // in schema,
  // it works.
  @Test
  void reorderFields() throws Exception {
    String w = "{\"type\":\"record\",\"name\":\"R\",\"fields\":" + "[{\"type\":\"long\",\"name\":\"l\"},"
        + "{\"type\":{\"type\":\"array\",\"items\":\"int\"},\"name\":\"a\"}" + "]}";
    Schema ws = new Schema.Parser().parse(w);
    DecoderFactory df = DecoderFactory.get();
    String data = "{\"a\":[1,2],\"l\":100}{\"l\": 200, \"a\":[1,2]}";
    JsonDecoder in = df.jsonDecoder(ws, data);
    assertEquals(100, in.readLong());
    in.skipArray();
    assertEquals(200, in.readLong());
    in.skipArray();
  }

  @Test
  void testIntWithError() throws IOException {
    Schema schema = SchemaBuilder.builder("test").record("example").fields().requiredInt("id").endRecord();
    String record = "{ \"id\": -1.2 }";

    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema, schema);
    JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, record);
    Assertions.assertThrows(AvroTypeException.class, () -> reader.read(null, decoder));
  }
}
