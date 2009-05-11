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
package org.apache.avro;

import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;
import org.codehaus.jackson.map.JsonNode;
import junit.framework.TestCase;

import org.apache.avro.io.*;
import org.apache.avro.util.Utf8;
import org.apache.avro.generic.*;
import org.apache.avro.Schema.*;

public class TestSchema extends TestCase {

  private static final int COUNT =
    Integer.parseInt(System.getProperty("test.count", "10"));

  public void testNull() throws Exception {
    check("\"null\"", "null", null);
  }

  public void testBoolean() throws Exception {
    check("\"boolean\"", "true", Boolean.TRUE);
  }

  public void testString() throws Exception {
    check("\"string\"", "\"foo\"", new Utf8("foo"));
  }

  public void testBytes() throws Exception {
    check("\"bytes\"", "\"\"", ByteBuffer.allocate(0));
  }

  public void testInt() throws Exception {
    check("\"int\"", "9", new Integer(9));
  }

  public void testLong() throws Exception {
    check("\"long\"", "11", new Long(11));
  }

  public void testFloat() throws Exception {
    check("\"float\"", "1.1", new Float(1.1));
  }

  public void testDouble() throws Exception {
    check("\"double\"", "1.2", new Double(1.2));
  }

  public void testArray() throws Exception {
    GenericArray<Long> array = new GenericData.Array<Long>(1);
    array.add(1L);
    check("{\"type\":\"array\", \"items\": \"long\"}", "[1]", array);
  }

  public void testMap() throws Exception {
    HashMap<Utf8,Long> map = new HashMap<Utf8,Long>();
    map.put(new Utf8("a"), 1L);
    check("{\"type\":\"map\", \"values\":\"long\"}", "{\"a\":1}", map);
  }

  public void testRecord() throws Exception {
    String recordJson = 
      "{\"type\":\"record\",\"fields\":[{\"name\":\"f\", \"type\":\"long\"}]}";
    Schema schema = Schema.parse(recordJson);
    GenericData.Record record = new GenericData.Record(schema);
    record.put("f", 11L);
    check(recordJson, "{\"f\":11}", record);
  }

  public void testRecursive() throws Exception {
    check("{\"type\": \"record\", \"name\": \"Node\", \"fields\": ["
          +"{\"name\":\"label\", \"type\":\"string\"},"
          +"{\"name\":\"children\", \"type\":"
          +"{\"type\": \"array\", \"items\": \"Node\" }}]}",
          false);
  }

  public void testLisp() throws Exception {
    check("{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": ["
          +"{\"name\":\"value\", \"type\":[\"null\", \"string\","
          +"{\"type\": \"record\", \"name\": \"Cons\", \"fields\": ["
          +"{\"name\":\"car\", \"type\":\"Lisp\"},"
          +"{\"name\":\"cdr\", \"type\":\"Lisp\"}]}]}]}",
          false);
  }

  public void testUnion() throws Exception {
    check("[\"string\", \"long\"]", false);
    checkDefault("[\"double\", \"long\"]", "1.1", new Double(1.1));
  }

  private static void check(String schemaJson, String defaultJson,
                            Object defaultValue) throws Exception {
    check(schemaJson, true);
    checkDefault(schemaJson, defaultJson, defaultValue);
  }

  private static void check(String jsonSchema) throws Exception {
    check(jsonSchema, true);
  }
  private static void check(String jsonSchema, boolean induce)
    throws Exception {
    Schema schema = Schema.parse(jsonSchema);
    //System.out.println(schema);
    for (Object datum : new RandomData(schema, COUNT)) {
      //System.out.println(GenericData.toString(datum));

      if (induce) {
        Schema induced = GenericData.induce(datum);
        assertEquals("Induced schema does not match.", schema, induced);
      }
        
      assertTrue("Datum does not validate against schema "+datum,
                 GenericData.validate(schema, datum));

      checkSerialization(schema, datum,
                         new GenericDatumWriter<Object>(), new GenericDatumReader<Object>());
    }
  }

  private static void checkSerialization(Schema schema, Object datum,
                                         DatumWriter<Object> writer,
                                         DatumReader<Object> reader)
    throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.setSchema(schema);
    writer.write(datum, new ValueWriter(out));
      
    byte[] data = out.toByteArray();
    // System.out.println("length = "+data.length);

    reader.setSchema(schema);
        
    Object decoded =
      reader.read(null, new ValueReader(new ByteArrayInputStream(data)));
      
    // System.out.println(GenericData.toString(datum));
    // System.out.println(GenericData.toString(decoded));
    assertEquals("Decoded data does not match.", datum, decoded);
  }

  private static final Schema ACTUAL =            // an empty record schema
    Schema.createRecord(new LinkedHashMap<String,Field>());

  @SuppressWarnings(value="unchecked")
  private static void checkDefault(String schemaJson, String defaultJson,
                                   Object defaultValue) throws Exception {
    String recordJson = "{\"type\":\"record\",\"fields\":[{\"name\":\"f\", "
    +"\"type\":"+schemaJson+", "
    +"\"default\":"+defaultJson+"}]}";
    Schema expected = Schema.parse(recordJson);
    DatumReader in = new GenericDatumReader(ACTUAL, expected);
    GenericData.Record record = (GenericData.Record)
      in.read(null, new ValueReader(new ByteArrayInputStream(new byte[0])));
    assertEquals("Wrong default.", defaultValue, record.get("f"));
  }



}
