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
package org.apache.avro.io.parsing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.avro.TestSchemas.ENUM1_AB_SCHEMA_NAMESPACE_1;
import static org.apache.avro.TestSchemas.ENUM1_AB_SCHEMA_NAMESPACE_2;

public class TestResolvingGrammarGenerator {

  @ParameterizedTest
  @MethodSource("data")
  void test(Schema schema, JsonNode data) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EncoderFactory factory = EncoderFactory.get();
    Encoder e = factory.validatingEncoder(schema, factory.binaryEncoder(baos, null));

    ResolvingGrammarGenerator.encode(e, schema, data);
    e.flush();
  }

  @Test
  void recordMissingRequiredFieldError() throws Exception {
    Schema schemaWithoutField = SchemaBuilder.record("MyRecord").namespace("ns").fields().name("field1").type()
        .stringType().noDefault().endRecord();
    Schema schemaWithField = SchemaBuilder.record("MyRecord").namespace("ns").fields().name("field1").type()
        .stringType().noDefault().name("field2").type().stringType().noDefault().endRecord();
    GenericData.Record record = new GenericRecordBuilder(schemaWithoutField).set("field1", "someValue").build();
    byte[] data = writeRecord(schemaWithoutField, record);
    try {
      readRecord(schemaWithField, data);
      Assertions.fail("Expected exception not thrown");
    } catch (AvroTypeException typeException) {
      Assertions.assertEquals("Found ns.MyRecord, expecting ns.MyRecord, missing required field field2",
          typeException.getMessage(), "Incorrect exception message");
    }
  }

  @Test
  void differingEnumNamespaces() throws Exception {
    Schema schema1 = SchemaBuilder.record("MyRecord").fields().name("field").type(ENUM1_AB_SCHEMA_NAMESPACE_1)
        .noDefault().endRecord();
    Schema schema2 = SchemaBuilder.record("MyRecord").fields().name("field").type(ENUM1_AB_SCHEMA_NAMESPACE_2)
        .noDefault().endRecord();
    GenericData.EnumSymbol genericEnumSymbol = new GenericData.EnumSymbol(ENUM1_AB_SCHEMA_NAMESPACE_1, "A");
    GenericData.Record record = new GenericRecordBuilder(schema1).set("field", genericEnumSymbol).build();
    byte[] data = writeRecord(schema1, record);
    Assertions.assertEquals(genericEnumSymbol, readRecord(schema1, data).get("field"));
    Assertions.assertEquals(genericEnumSymbol, readRecord(schema2, data).get("field"));
  }

  public static Stream<Arguments> data() {
    Collection<String[]> ret = Arrays.asList(new String[][] {
        { "{ \"type\": \"record\", \"name\": \"r\", \"fields\": [ " + " { \"name\" : \"f1\", \"type\": \"int\" }, "
            + " { \"name\" : \"f2\", \"type\": \"float\" } " + "] }", "{ \"f2\": 10.4, \"f1\": 10 } " },
        { "{ \"type\": \"enum\", \"name\": \"e\", \"symbols\": " + "[ \"s1\", \"s2\"] }", " \"s1\" " },
        { "{ \"type\": \"enum\", \"name\": \"e\", \"symbols\": " + "[ \"s1\", \"s2\"] }", " \"s2\" " },
        { "{ \"type\": \"fixed\", \"name\": \"f\", \"size\": 10 }", "\"hello\"" },
        { "{ \"type\": \"array\", \"items\": \"int\" }", "[ 10, 20, 30 ]" },
        { "{ \"type\": \"map\", \"values\": \"int\" }", "{ \"k1\": 10, \"k3\": 20, \"k3\": 30 }" },
        { "[ \"int\", \"long\" ]", "10" }, { "\"string\"", "\"hello\"" }, { "\"bytes\"", "\"hello\"" },
        { "\"int\"", "10" }, { "\"long\"", "10" }, { "\"float\"", "10.0" }, { "\"double\"", "10.0" },
        { "\"boolean\"", "true" }, { "\"boolean\"", "false" }, { "\"null\"", "null" }, });

    final JsonFactory factory = new JsonFactory();
    final ObjectMapper mapper = new ObjectMapper(factory);

    return ret.stream().map((String[] args) -> {
      Schema schema = new Schema.Parser().parse(args[0]);
      try {
        JsonNode data = mapper.readTree(new StringReader(args[1]));
        return Arguments.of(schema, data);
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    });
  }

  private byte[] writeRecord(Schema schema, GenericData.Record record) throws Exception {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    GenericDatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>(schema);
    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(schema, byteStream);
      writer.append(record);
    }
    return byteStream.toByteArray();
  }

  private GenericData.Record readRecord(Schema schema, byte[] data) throws Exception {
    ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
    GenericDatumReader<GenericData.Record> datumReader = new GenericDatumReader<>(schema);
    try (DataFileStream<GenericData.Record> reader = new DataFileStream<>(byteStream, datumReader)) {
      return reader.next();
    }
  }
}
