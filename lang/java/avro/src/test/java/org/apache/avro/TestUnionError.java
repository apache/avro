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
package org.apache.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestUnionError {

  @Test
  void unionErrorMessage() throws IOException {
    String writerSchemaJson = "            {\n" + "              \"type\" : \"record\",\n"
        + "              \"name\" : \"C\",\n" + "              \"fields\" : [ {\n"
        + "                \"name\" : \"c\",\n" + "                \"type\" : [ {\n"
        + "                  \"type\" : \"record\",\n" + "                  \"name\" : \"A\",\n"
        + "                  \"fields\" : [ {\n" + "                    \"name\" : \"amount\",\n"
        + "                    \"type\" : \"int\"\n" + "                  } ]\n" + "                }, {\n"
        + "                  \"type\" : \"record\",\n" + "                  \"name\" : \"B\",\n"
        + "                  \"fields\" : [ {\n" + "                    \"name\" : \"amount1\",\n"
        + "                    \"type\" : \"int\"\n" + "                  } ]\n" + "                } ]\n"
        + "              } ]\n" + "            }";
    Schema writerSchema = new Schema.Parser().parse(writerSchemaJson);

    String readerSchemaJson = " {\n" + "              \"type\" : \"record\",\n" + "              \"name\" : \"C1\",\n"
        + "              \"fields\" : [ {\n" + "                \"name\" : \"c\",\n"
        + "                \"type\" : [ {\n" + "                  \"type\" : \"record\",\n"
        + "                  \"name\" : \"A\",\n" + "                  \"fields\" : [ {\n"
        + "                    \"name\" : \"amount\",\n" + "                    \"type\" : \"int\"\n"
        + "                  } ]\n" + "                }, \"float\" ]\n" + "              } ]\n" + "            }";
    Schema readerSchema = new Schema.Parser().parse(readerSchemaJson);

    List<Schema> unionSchemas = writerSchema.getField("c").schema().getTypes();

    GenericRecord r = new GenericData.Record(writerSchema);
    GenericRecord b = new GenericData.Record(unionSchemas.get(1));
    b.put("amount1", 12);
    r.put("c", b);

    ByteArrayOutputStream outs = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(writerSchema);
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outs, null);
    datumWriter.write(r, encoder);
    encoder.flush();

    InputStream ins = new ByteArrayInputStream(outs.toByteArray());
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(ins, null);

    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
    AvroTypeException avroException = assertThrows(AvroTypeException.class, () -> datumReader.read(null, decoder));
    assertEquals("Field \"c\" content mismatch: Found B, expecting union[A, float]", avroException.getMessage());
  }
}
