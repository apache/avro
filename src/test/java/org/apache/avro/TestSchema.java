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
import org.codehaus.jackson.map.JsonNode;
import junit.framework.TestCase;

import org.apache.avro.io.*;
import org.apache.avro.generic.*;

public class TestSchema extends TestCase {

  private static final int COUNT =
    Integer.parseInt(System.getProperty("test.count", "10"));

  public void testNull() throws Exception {
    check("\"null\"");
  }

  public void testBoolean() throws Exception {
    check("\"boolean\"");
  }

  public void testString() throws Exception {
    check("\"string\"");
  }

  public void testBytes() throws Exception {
    check("\"bytes\"");
  }

  public void testInt() throws Exception {
    check("\"int\"");
  }

  public void testLong() throws Exception {
    check("\"long\"");
  }

  public void testFloat() throws Exception {
    check("\"float\"");
  }

  public void testDouble() throws Exception {
    check("\"double\"");
  }

  public void testArray() throws Exception {
    check("{\"type\":\"array\", \"items\": \"long\"}");
  }

  public void testMap() throws Exception {
    check("{\"type\":\"map\", \"keys\": \"long\", \"values\": \"string\"}");
  }

  public void testRecord() throws Exception {
    check("{\"type\":\"record\",\"fields\":{\"f\":\"string\"}}");
  }

  public void testRecursive() throws Exception {
    check("{\"type\": \"record\", \"name\": \"Node\", \"fields\": {"
          +"\"label\": \"string\","
          +"\"children\": {\"type\": \"array\", \"items\": \"Node\" }}}",
          false);
  }

  public void testLisp() throws Exception {
    check("{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": {"
          +"\"value\": [\"null\", \"string\","
          +"{\"type\": \"record\", \"name\": \"Cons\", \"fields\": {"
          +"\"car\": \"Lisp\", \"cdr\": \"Lisp\"}}]}}",
          false);
  }

  public void testUnion() throws Exception {
    check("[\"string\", \"long\"]", false);
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
                         new GenericDatumWriter(), new GenericDatumReader());
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

}
