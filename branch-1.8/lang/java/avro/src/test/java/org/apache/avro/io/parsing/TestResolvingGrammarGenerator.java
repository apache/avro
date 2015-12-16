/**
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
package org.apache.avro.io.parsing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;

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
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestResolvingGrammarGenerator {
  private final Schema schema;
  private final JsonNode data;
  
  public TestResolvingGrammarGenerator(String jsonSchema, String jsonData)
    throws IOException {
    this.schema = Schema.parse(jsonSchema);
    JsonFactory factory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper(factory);

    this.data = mapper.readTree(new StringReader(jsonData));
  }

  @Test
  public void test() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EncoderFactory factory = EncoderFactory.get();
    Encoder e = factory.validatingEncoder(schema, 
        factory.binaryEncoder(baos, null));
    
    ResolvingGrammarGenerator.encode(e, schema, data);
    e.flush();
  }

  @Test
  public void testRecordMissingRequiredFieldError() throws Exception {
    Schema schemaWithoutField = SchemaBuilder
        .record("MyRecord").namespace("ns")
        .fields()
          .name("field1").type().stringType().noDefault()
        .endRecord();
    Schema schemaWithField = SchemaBuilder
        .record("MyRecord").namespace("ns")
        .fields()
          .name("field1").type().stringType().noDefault()
          .name("field2").type().stringType().noDefault()
        .endRecord();
    GenericData.Record record = new GenericRecordBuilder(schemaWithoutField).set("field1", "someValue").build();
    byte[] data = writeRecord(schemaWithoutField, record);
    try {
      readRecord(schemaWithField, data);
      Assert.fail("Expected exception not thrown");
    } catch (AvroTypeException typeException) {
      Assert.assertEquals("Incorrect exception message",
          "Found ns.MyRecord, expecting ns.MyRecord, missing required field field2", typeException.getMessage());
    }
  }
  
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> ret = Arrays.asList(
        new Object[][] {
            { "{ \"type\": \"record\", \"name\": \"r\", \"fields\": [ "
                + " { \"name\" : \"f1\", \"type\": \"int\" }, "
                + " { \"name\" : \"f2\", \"type\": \"float\" } "
                + "] } }",
              "{ \"f2\": 10.4, \"f1\": 10 } " },
            { "{ \"type\": \"enum\", \"name\": \"e\", \"symbols\": "
                + "[ \"s1\", \"s2\"] } }", " \"s1\" " },
            { "{ \"type\": \"enum\", \"name\": \"e\", \"symbols\": "
                + "[ \"s1\", \"s2\"] } }", " \"s2\" " },
            { "{ \"type\": \"fixed\", \"name\": \"f\", \"size\": 10 }",
              "\"hello\"" },
            { "{ \"type\": \"array\", \"items\": \"int\" }",
              "[ 10, 20, 30 ]" },
            { "{ \"type\": \"map\", \"values\": \"int\" }",
              "{ \"k1\": 10, \"k3\": 20, \"k3\": 30 }" },
            { "[ \"int\", \"long\" ]", "10" },
            { "\"string\"", "\"hello\"" },
            { "\"bytes\"", "\"hello\"" },
            { "\"int\"", "10" },
            { "\"long\"", "10" },
            { "\"float\"", "10.0" },
            { "\"double\"", "10.0" },
            { "\"boolean\"", "true" },
            { "\"boolean\"", "false" },
            { "\"null\"", "null" },
            }
        );
    return ret;
  }

  private byte[] writeRecord(Schema schema, GenericData.Record record) throws Exception {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    GenericDatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<GenericData.Record>(schema);
    DataFileWriter<GenericData.Record> writer = new DataFileWriter<GenericData.Record>(datumWriter);
    try {
      writer.create(schema, byteStream);
      writer.append(record);
    } finally {
      writer.close();
    }
    return byteStream.toByteArray();
  }

  private GenericData.Record readRecord(Schema schema, byte[] data) throws Exception {
    ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
    GenericDatumReader<GenericData.Record> datumReader = new GenericDatumReader<GenericData.Record>(schema);
    DataFileStream<GenericData.Record> reader = new DataFileStream<GenericData.Record>(byteStream, datumReader);
    try {
      return reader.next();
    } finally {
      reader.close();
    }
  }
}
