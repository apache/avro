/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.io;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static java.util.Collections.singletonList;
import static org.apache.avro.Schema.create;
import static org.apache.avro.Schema.createRecord;
import static org.apache.avro.Schema.createUnion;
import static org.junit.Assert.assertEquals;

public class TestJsonDecoder {

  @Test public void testInt() throws Exception {
    checkNumeric("int", 1);
  }

  @Test public void testLong() throws Exception {
    checkNumeric("long", 1L);
  }

  @Test public void testFloat() throws Exception {
    checkNumeric("float", 1.0F);
  }

  @Test public void testDouble() throws Exception {
    checkNumeric("double", 1.0);
  }

  private void checkNumeric(String type, Object value) throws Exception {
    String def =
      "{\"type\":\"record\",\"name\":\"X\",\"fields\":"
      +"[{\"type\":\""+type+"\",\"name\":\"n\"}]}";
    Schema schema = Schema.parse(def);
    DatumReader<GenericRecord> reader =
      new GenericDatumReader<>(schema);

    String[] records = {"{\"n\":1}", "{\"n\":1.0}"};

    for (String record : records) {
      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, record);
      GenericRecord r = reader.read(null, decoder);
      Assert.assertEquals(value, r.get("n"));
    }
  }

  // Ensure that even if the order of fields in JSON is different from the order in schema,
  // it works.
  @Test public void testReorderFields() throws Exception {
    String w =
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":"
      +"[{\"type\":\"long\",\"name\":\"l\"},"
      +"{\"type\":{\"type\":\"array\",\"items\":\"int\"},\"name\":\"a\"}"
      +"]}";
    Schema ws = Schema.parse(w);
    DecoderFactory df = DecoderFactory.get();
    String data = "{\"a\":[1,2],\"l\":100}{\"l\": 200, \"a\":[1,2]}";
    JsonDecoder in = df.jsonDecoder(ws, data);
    Assert.assertEquals(100, in.readLong());
    in.skipArray();
    Assert.assertEquals(200, in.readLong());
    in.skipArray();
  }


  /**
   * AVRO-2152
   * JsonDecoder fails when reading record with aliases inside union
   */
  @Test public void testJsonDecoderWithAliasesInUnion() throws Exception {
    Schema writerItem = createRecord("WItem", "writer item", "writer.ns", false,
      singletonList(new Schema.Field("value",
        create(Schema.Type.STRING),
        "value", (Object) null)));
    Schema writerSchema = createRecord("WWrapper", "writer", "writer.ns", false,
      singletonList(new Schema.Field("item",
        createUnion(create(Schema.Type.NULL), writerItem),
        "value", (Object) null)));
    System.out.println(writerSchema.toString(true));

    Schema readerItem = createRecord("RItem", "reader item", "reader.ns", false,
      singletonList(new Schema.Field("value", create(Schema.Type.STRING),
        "value", (Object) null)));
    Schema readerSchema = createRecord("RWrapper", "reader", "reader.ns", false,
      singletonList(new Schema.Field("item",
        createUnion(create(Schema.Type.NULL), readerItem),
        "value", (Object) null)));
    readerSchema.addAlias("WWrapper", "writer.ns");
    readerItem.addAlias("WItem", "writer.ns");

    System.out.println(readerSchema.toString(true));

    assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
      SchemaCompatibility.checkReaderWriterCompatibility(readerSchema, writerSchema).getType());

    // Create an instance for testing
    GenericData.Record instance = new GenericRecordBuilder(writerSchema)
      .set("item",
        new GenericRecordBuilder(writerItem)
          .set("value", "12345")
          .build()
      ).build();

    // Serialize using JSON Encoder
    final GenericDatumWriter<Object> writer = new GenericDatumWriter<>(instance.getSchema());
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(instance.getSchema(), out);
    writer.write(instance, encoder);
    encoder.flush();

    // Deserialize using JSON Decoder
    final GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(readerSchema,
      new String(out.toByteArray(), StandardCharsets.UTF_8));

    GenericRecord deserialized = reader.read(null, decoder);
    assertEquals(deserialized.toString(), instance.toString());
  }

}
