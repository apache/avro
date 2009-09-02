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

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.io.BinaryData;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.util.Utf8;

import org.apache.avro.test.Simple;

public class TestCompare {

  @Test
  public void testNull() throws Exception {
    Schema schema = Schema.parse("\"null\"");
    byte[] b = render(null, schema, new GenericDatumWriter<Object>());
    assertEquals(0, BinaryData.compare(b, 0, b, 0, schema));
  }

  @Test
  public void testBoolean() throws Exception {
    check("\"boolean\"", Boolean.FALSE, Boolean.TRUE);
  }

  @Test
  public void testString() throws Exception {
    check("\"string\"", new Utf8("a"), new Utf8("b"));
    check("\"string\"", new Utf8("a"), new Utf8("ab"));
  }

  @Test
  public void testBytes() throws Exception {
    check("\"bytes\"",
          ByteBuffer.wrap(new byte[]{}),
          ByteBuffer.wrap(new byte[]{1}));
    check("\"bytes\"",
          ByteBuffer.wrap(new byte[]{1}),
          ByteBuffer.wrap(new byte[]{2}));
    check("\"bytes\"",
          ByteBuffer.wrap(new byte[]{1,2}),
          ByteBuffer.wrap(new byte[]{2}));
  }

  @Test
  public void testInt() throws Exception {
    check("\"int\"", new Integer(-1), new Integer(0));
    check("\"int\"", new Integer(0), new Integer(1));
  }

  @Test
  public void testLong() throws Exception {
    check("\"long\"", new Long(11), new Long(12));
    check("\"long\"", new Long(-1), new Long(1));
  }

  @Test
  public void testFloat() throws Exception {
    check("\"float\"", new Float(1.1), new Float(1.2));
    check("\"float\"", new Float(-1.1), new Float(1.0));
  }

  @Test
  public void testDouble() throws Exception {
    check("\"double\"", new Double(1.2), new Double(1.3));
    check("\"double\"", new Double(-1.2), new Double(1.3));
  }

  @Test
  public void testArray() throws Exception {
    String json = "{\"type\":\"array\", \"items\": \"long\"}";
    Schema schema = Schema.parse(json);
    GenericArray<Long> a1 = new GenericData.Array<Long>(1, schema);
    a1.add(1L);
    GenericArray<Long> a2 = new GenericData.Array<Long>(1, schema);
    a2.add(1L);
    a2.add(0L);
    check(json, a1, a2);
  }

  @Test
  public void testRecord() throws Exception {
    String recordJson = "{\"type\":\"record\", \"name\":\"Test\", \"fields\":"
      +"[{\"name\":\"f\",\"type\":\"int\"},{\"name\":\"g\",\"type\":\"int\"}]}";
    Schema schema = Schema.parse(recordJson);
    GenericData.Record r1 = new GenericData.Record(schema);
    r1.put("f", 11);
    r1.put("g", 12);
    GenericData.Record r2 = new GenericData.Record(schema);
    r2.put("f", 11);
    r2.put("g", 13);
    check(recordJson, r1, r2);
  }

  @Test
  public void testEnum() throws Exception {
    check("{\"type\":\"enum\", \"name\":\"Test\",\"symbols\": [\"A\", \"B\"]}",
          "A", "B");
  }

  @Test
  public void testFixed() throws Exception {
    check("{\"type\": \"fixed\", \"name\":\"Test\", \"size\": 1}",
          new GenericData.Fixed(new byte[]{(byte)'a'}),
          new GenericData.Fixed(new byte[]{(byte)'b'}));
  }

  @Test
  public void testUnion() throws Exception {
    check("[\"string\", \"long\"]", new Utf8("a"), new Utf8("b"), false);
    check("[\"string\", \"long\"]", new Long(1), new Long(2), false);
    check("[\"string\", \"long\"]", new Utf8("a"), new Long(1), false);
  }

  @Test
  public void testSpecificRecord() throws Exception {
    Simple.TestRecord s1 = new Simple.TestRecord();
    Simple.TestRecord s2 = new Simple.TestRecord();
    s1.name = new Utf8("foo");
    s2.name = new Utf8("foo");
    s1.kind = Simple.Kind.BAR;
    s2.kind = Simple.Kind.BAR;
    s1.hash = new Simple.MD5();
    s1.hash.bytes(new byte[] {0,1,2,3,4,5,6,7,8,9,0,1,2,3,4,5});
    s2.hash = new Simple.MD5();
    s2.hash.bytes(new byte[] {0,1,2,3,4,5,6,7,8,9,0,1,2,3,4,6});
    check(Simple.TestRecord._SCHEMA, s1, s2, true,
          new SpecificDatumWriter(Simple.TestRecord._SCHEMA),
          SpecificData.get());
  }  

  private static void check(String schemaJson, Object o1, Object o2)
    throws Exception {
    check(schemaJson, o1, o2, true);
  }

  private static void check(String schemaJson, Object o1, Object o2,
                            boolean comparable)
    throws Exception {
    check(Schema.parse(schemaJson), o1, o2, comparable,
          new GenericDatumWriter<Object>(), GenericData.get());
  }

  private static void check(Schema schema, Object o1, Object o2,
                            boolean comparable,
                            DatumWriter<Object> writer,
                            GenericData comparator)
    throws Exception {

    byte[] b1 = render(o1, schema, writer);
    byte[] b2 = render(o2, schema, writer);
    assertEquals(-1, BinaryData.compare(b1, 0, b2, 0, schema));
    assertEquals(1, BinaryData.compare(b2, 0, b1, 0, schema));
    assertEquals(0, BinaryData.compare(b1, 0, b1, 0, schema));
    assertEquals(0, BinaryData.compare(b2, 0, b2, 0, schema));

    assertEquals(-1, compare(o1, o2, schema, comparable, comparator));
    assertEquals(1, compare(o2, o1, schema, comparable, comparator));
    assertEquals(0, compare(o1, o1, schema, comparable, comparator));
    assertEquals(0, compare(o2, o2, schema, comparable, comparator));
  }

  @SuppressWarnings(value="unchecked")
  private static int compare(Object o1, Object o2, Schema schema,
                             boolean comparable, GenericData comparator) {
    return comparable
      ? ((Comparable)o1).compareTo(o2)
      : comparator.compare(o1, o2, schema);
  }

  private static byte[] render(Object datum, Schema schema,
                               DatumWriter<Object> writer)
    throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.setSchema(schema);
    writer.write(datum, new BinaryEncoder(out));
    return out.toByteArray();
  }
}
