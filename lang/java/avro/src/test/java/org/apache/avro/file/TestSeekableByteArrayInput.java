/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.file;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSeekableByteArrayInput {

  private byte[] getSerializedMessage(IndexedRecord message, Schema schema) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    SpecificDatumWriter<IndexedRecord> writer = new SpecificDatumWriter<>();
    try (DataFileWriter<IndexedRecord> dfw = new DataFileWriter<>(writer).create(schema, baos)) {
      dfw.append(message);
    }
    return baos.toByteArray();
  }

  private Schema getTestSchema() throws Exception {
    Schema schema = Schema.createRecord("TestRecord", "this is a test record", "org.apache.avro.file", false);
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("name", Schema.create(Type.STRING), "this is a test field"));
    schema.setFields(fields);
    return schema;
  }

  @Test
  void serialization() throws Exception {
    Schema testSchema = getTestSchema();
    GenericRecord message = new Record(testSchema);
    message.put("name", "testValue");

    byte[] data = getSerializedMessage(message, testSchema);

    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(testSchema);
    final IndexedRecord result;
    try (SeekableInput in = new SeekableByteArrayInput(data);
        FileReader<IndexedRecord> dfr = DataFileReader.openReader(in, reader)) {
      result = dfr.next();
    }
    assertNotNull(result);
    assertInstanceOf(GenericRecord.class, result);
    assertEquals(new Utf8("testValue"), ((GenericRecord) result).get("name"));
  }

  @Test
  void readingData() throws IOException {
    byte[] data = "0123456789ABCD".getBytes(StandardCharsets.UTF_8);
    byte[] result = new byte[16];
    try (SeekableInput in = new SeekableByteArrayInput(data)) {
      in.read(result, 0, 8);
      in.seek(4);
      in.read(result, 8, 8);
      assertEquals(12, in.tell());
      assertEquals(data.length, in.length());
      assertEquals("01234567456789AB", new String(result, StandardCharsets.UTF_8));
    }
  }

  @Test
  void illegalSeeks() throws IOException {
    byte[] data = "0123456789ABCD".getBytes(StandardCharsets.UTF_8);
    try (SeekableInput in = new SeekableByteArrayInput(data)) {
      byte[] buf = new byte[2];
      in.read(buf, 0, buf.length);
      in.seek(-4);
      assertEquals(2, in.tell());

      assertThrows(EOFException.class, () -> in.seek(64));
    }
  }
}
