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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.TestSpecificCompiler;
import org.apache.avro.util.Utf8;

public class TestSchema {

  public static final String BASIC_ENUM_SCHEMA = "{\"type\":\"enum\", \"name\":\"Test\","
            +"\"symbols\": [\"A\", \"B\"]}";

  public static final String SCHEMA_WITH_DOC_TAGS = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"outer_record\",\n"
      + "  \"doc\": \"This is not a world record.\",\n"
      + "  \"fields\": [\n"
      + "    { \"type\": { \"type\": \"fixed\", \"doc\": \"Very Inner Fixed\", "
      + "                  \"name\": \"very_inner_fixed\", \"size\": 1 },\n"
      + "      \"doc\": \"Inner Fixed\", \"name\": \"inner_fixed\" },\n"
      + "    { \"type\": \"string\",\n"
      + "      \"name\": \"inner_string\",\n"
      + "      \"doc\": \"Inner String\" },\n"
      + "    { \"type\": { \"type\": \"enum\", \"doc\": \"Very Inner Enum\", \n"
      + "                  \"name\": \"very_inner_enum\", \n"
      + "                  \"symbols\": [ \"A\", \"B\", \"C\" ] },\n"
      + "      \"doc\": \"Inner Enum\", \"name\": \"inner_enum\" },\n"
      + "    { \"type\": [\"string\", \"int\"], \"doc\": \"Inner Union\", \n"
      + "      \"name\": \"inner_union\" }\n" + "  ]\n" + "}\n";

  private static final int COUNT =
    Integer.parseInt(System.getProperty("test.count", "10"));

  @Test
  public void testNull() throws Exception {
    assertEquals(Schema.create(Type.NULL), Schema.parse("\"null\""));
    assertEquals(Schema.create(Type.NULL), Schema.parse("{\"type\":\"null\"}"));
    check("\"null\"", "null", null);
  }

  @Test
  public void testBoolean() throws Exception {
    assertEquals(Schema.create(Type.BOOLEAN), Schema.parse("\"boolean\""));
    assertEquals(Schema.create(Type.BOOLEAN),
                 Schema.parse("{\"type\":\"boolean\"}"));
    check("\"boolean\"", "true", Boolean.TRUE);
  }

  @Test
  public void testString() throws Exception {
    assertEquals(Schema.create(Type.STRING), Schema.parse("\"string\""));
    assertEquals(Schema.create(Type.STRING),
                 Schema.parse("{\"type\":\"string\"}"));
    check("\"string\"", "\"foo\"", new Utf8("foo"));
  }

  @Test
  public void testBytes() throws Exception {
    assertEquals(Schema.create(Type.BYTES), Schema.parse("\"bytes\""));
    assertEquals(Schema.create(Type.BYTES),
                 Schema.parse("{\"type\":\"bytes\"}"));
    check("\"bytes\"", "\"\\u0000ABC\\u00FF\"",
          ByteBuffer.wrap(new byte[]{0,65,66,67,-1}));
  }

  @Test
  public void testInt() throws Exception {
    assertEquals(Schema.create(Type.INT), Schema.parse("\"int\""));
    assertEquals(Schema.create(Type.INT), Schema.parse("{\"type\":\"int\"}"));
    check("\"int\"", "9", new Integer(9));
  }

  @Test
  public void testLong() throws Exception {
    assertEquals(Schema.create(Type.LONG), Schema.parse("\"long\""));
    assertEquals(Schema.create(Type.LONG), Schema.parse("{\"type\":\"long\"}"));
    check("\"long\"", "11", new Long(11));
  }

  @Test
  public void testFloat() throws Exception {
    assertEquals(Schema.create(Type.FLOAT), Schema.parse("\"float\""));
    assertEquals(Schema.create(Type.FLOAT),
                 Schema.parse("{\"type\":\"float\"}"));
    check("\"float\"", "1.1", new Float(1.1));
  }

  @Test
  public void testDouble() throws Exception {
    assertEquals(Schema.create(Type.DOUBLE), Schema.parse("\"double\""));
    assertEquals(Schema.create(Type.DOUBLE),
                 Schema.parse("{\"type\":\"double\"}"));
    check("\"double\"", "1.2", new Double(1.2));
  }

  @Test
  public void testArray() throws Exception {
    String json = "{\"type\":\"array\", \"items\": \"long\"}";
    Schema schema = Schema.parse(json);
    GenericArray<Long> array = new GenericData.Array<Long>(1, schema);
    array.add(1L);
    check(json, "[1]", array);
    checkParseError("{\"type\":\"array\"}");      // items required
  }

  @Test
  public void testMap() throws Exception {
    HashMap<Utf8,Long> map = new HashMap<Utf8,Long>();
    map.put(new Utf8("a"), 1L);
    check("{\"type\":\"map\", \"values\":\"long\"}", "{\"a\":1}", map);
    checkParseError("{\"type\":\"map\"}");        // values required
  }

  @Test
  public void testRecord() throws Exception {
    String recordJson = "{\"type\":\"record\", \"name\":\"Test\", \"fields\":"
      +"[{\"name\":\"f\", \"type\":\"long\", \"foo\":\"bar\"}]}";
    Schema schema = Schema.parse(recordJson);

    // test field props
    assertEquals("bar", schema.getField("f").getProp("foo"));
    assertEquals("bar", Schema.parse(schema.toString())
                 .getField("f").getProp("foo"));
    schema.getField("f").addProp("baz", "boo");
    assertEquals("boo", schema.getField("f").getProp("baz"));

    GenericData.Record record = new GenericData.Record(schema);
    record.put("f", 11L);
    check(recordJson, "{\"f\":11}", record, false);
    checkParseError("{\"type\":\"record\"}");
    checkParseError("{\"type\":\"record\",\"name\":\"X\"}");
    checkParseError("{\"type\":\"record\",\"name\":\"X\",\"fields\":\"Y\"}");
    checkParseError("{\"type\":\"record\",\"name\":\"X\",\"fields\":"
                    +"[{\"name\":\"f\"}]}");       // no type
    checkParseError("{\"type\":\"record\",\"name\":\"X\",\"fields\":"
                    +"[{\"type\":\"long\"}]}");    // no name
  }

  @Test
  public void testEnum() throws Exception {
    check(BASIC_ENUM_SCHEMA, "\"B\"", "B", false);
    checkParseError("{\"type\":\"enum\"}");        // symbols required
    checkParseError("{\"type\":\"enum\",\"symbols\": [\"X\",\"X\"]}");
  }

  @Test
  public void testFixed() throws Exception {
    check("{\"type\": \"fixed\", \"name\":\"Test\", \"size\": 1}", "\"a\"",
          new GenericData.Fixed(new byte[]{(byte)'a'}), false);
    checkParseError("{\"type\":\"fixed\"}");        // size required
  }

  @Test
  public void testRecursive() throws Exception {
    check("{\"type\": \"record\", \"name\": \"Node\", \"fields\": ["
          +"{\"name\":\"label\", \"type\":\"string\"},"
          +"{\"name\":\"children\", \"type\":"
          +"{\"type\": \"array\", \"items\": \"Node\" }}]}",
          false);
  }

  @Test
  public void testRecursiveEquals() throws Exception {
    String jsonSchema = "{\"type\":\"record\", \"name\":\"List\", \"fields\": ["
      +"{\"name\":\"next\", \"type\":\"List\"}]}";
    Schema s1 = Schema.parse(jsonSchema);
    Schema s2 = Schema.parse(jsonSchema);
    assertEquals(s1, s2);
    s1.hashCode();                                // test no stackoverflow
  }

  @Test
  public void testLisp() throws Exception {
    check("{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": ["
          +"{\"name\":\"value\", \"type\":[\"null\", \"string\","
          +"{\"type\": \"record\", \"name\": \"Cons\", \"fields\": ["
          +"{\"name\":\"car\", \"type\":\"Lisp\"},"
          +"{\"name\":\"cdr\", \"type\":\"Lisp\"}]}]}]}",
          false);
  }

  @Test
  public void testUnion() throws Exception {
    check("[\"string\", \"long\"]", false);
    checkDefault("[\"double\", \"long\"]", "1.1", new Double(1.1));

    // check union json
    String record = "{\"type\":\"record\",\"name\":\"Foo\",\"fields\":[]}";
    String fixed = "{\"type\":\"fixed\",\"name\":\"Bar\",\"size\": 1}";
    String enu = "{\"type\":\"enum\",\"name\":\"Baz\",\"symbols\": [\"X\"]}";
    Schema union = Schema.parse("[\"null\",\"string\","
                                +record+","+ enu+","+fixed+"]");
    checkJson(union, null, "null");
    checkJson(union, new Utf8("foo"), "{\"string\":\"foo\"}");
    checkJson(union,
              new GenericData.Record(Schema.parse(record)),
              "{\"Foo\":{}}");
    checkJson(union,
              new GenericData.Fixed(new byte[]{(byte)'a'}),
              "{\"Bar\":\"a\"}");
    checkJson(union, "X", "{\"Baz\":\"X\"}");
  }

  @Test
  public void testComplexUnions() throws Exception {
    // one of each unnamed type and two of named types
    String partial = "[\"int\", \"long\", \"float\", \"double\", \"boolean\", \"bytes\"," +
    " \"string\", {\"type\":\"array\", \"items\": \"long\"}," +
    " {\"type\":\"map\", \"values\":\"long\"}";
    String namedTypes = ", {\"type\":\"record\",\"name\":\"Foo\",\"fields\":[]}," +
    " {\"type\":\"fixed\",\"name\":\"Bar\",\"size\": 1}," +
    " {\"type\":\"enum\",\"name\":\"Baz\",\"symbols\": [\"X\"]}";
    
    String namedTypes2 = ", {\"type\":\"record\",\"name\":\"Foo2\",\"fields\":[]}," +
    " {\"type\":\"fixed\",\"name\":\"Bar2\",\"size\": 1}," +
    " {\"type\":\"enum\",\"name\":\"Baz2\",\"symbols\": [\"X\"]}";
    
    check(partial + namedTypes + "]", false);
    check(partial + namedTypes + namedTypes2 + "]", false); 
    checkParseError(partial + namedTypes + namedTypes + "]");
    
    // fail with two branches of the same unnamed type
    checkUnionError(new Schema[] {Schema.create(Type.INT), Schema.create(Type.INT)});
    checkUnionError(new Schema[] {Schema.create(Type.LONG), Schema.create(Type.LONG)});
    checkUnionError(new Schema[] {Schema.create(Type.FLOAT), Schema.create(Type.FLOAT)});
    checkUnionError(new Schema[] {Schema.create(Type.DOUBLE), Schema.create(Type.DOUBLE)});
    checkUnionError(new Schema[] {Schema.create(Type.BOOLEAN), Schema.create(Type.BOOLEAN)});
    checkUnionError(new Schema[] {Schema.create(Type.BYTES), Schema.create(Type.BYTES)});
    checkUnionError(new Schema[] {Schema.create(Type.STRING), Schema.create(Type.STRING)});
    checkUnionError(new Schema[] {Schema.createArray(Schema.create(Type.INT)), 
        Schema.createArray(Schema.create(Type.INT))});
    checkUnionError(new Schema[] {Schema.createMap(Schema.create(Type.INT)), 
        Schema.createMap(Schema.create(Type.INT))});
    
    List<String> symbols = new ArrayList<String>();
    symbols.add("NOTHING");
    
    // succeed with two branches of the same named type, if different names
    buildUnion(new Schema[] {Schema.createRecord("Foo", null, "org.test", false),
        Schema.createRecord("Foo2", null, "org.test", false)});
    buildUnion(new Schema[] {Schema.createEnum("Bar", null, "org.test", symbols),
        Schema.createEnum("Bar2", null, "org.test", symbols)});
    buildUnion(new Schema[] {Schema.createFixed("Baz", null, "org.test", 2),
        Schema.createFixed("Baz2", null, "org.test", 1)});

    // fail with two branches of the same named type, but same names
    checkUnionError(new Schema[] {Schema.createRecord("Foo", null, "org.test", false),
        Schema.createRecord("Foo", null, "org.test", false)});
    checkUnionError(new Schema[] {Schema.createEnum("Bar", null, "org.test", symbols),
        Schema.createEnum("Bar", null, "org.test", symbols)});
    checkUnionError(new Schema[] {Schema.createFixed("Baz", null, "org.test", 2),
        Schema.createFixed("Baz", null, "org.test", 1)});
    
    Schema union = buildUnion(new Schema[] {Schema.create(Type.INT)});
    // fail if creating a union of a union
    checkUnionError(new Schema[] {union});
  }
  
  @Test
  public void testComplexProp() throws Exception {
    String json = "{\"type\":\"null\", \"foo\": [0]}";
    Schema s = Schema.parse(json);
    assertEquals(null, s.getProp("foo"));
  }
  
  @Test
  public void testParseInputStream() throws IOException {
    Schema s = Schema.parse(
        new ByteArrayInputStream("\"boolean\"".getBytes("UTF-8")));
    assertEquals(Schema.parse("\"boolean\""), s);
  }

  @Test
  public void testNamespaceScope() throws Exception {
    String z = "{\"type\":\"record\",\"name\":\"Z\",\"fields\":[]}";
    String y = "{\"type\":\"record\",\"name\":\"q.Y\",\"fields\":["
      +"{\"name\":\"f\",\"type\":"+z+"}]}";
    String x = "{\"type\":\"record\",\"name\":\"p.X\",\"fields\":["
      +"{\"name\":\"f\",\"type\":"+y+"},"
      +"{\"name\":\"g\",\"type\":"+z+"}"
      +"]}";
    Schema xs = Schema.parse(x);
    Schema ys = xs.getField("f").schema();
    assertEquals("p.Z", xs.getField("g").schema().getFullName());
    assertEquals("q.Z", ys.getField("f").schema().getFullName());
  }

  private static void checkParseError(String json) {
    try {
      Schema.parse(json);
    } catch (SchemaParseException e) {
      return;
    }
    fail("Should not have parsed: "+json);
  }

  private static void checkUnionError(Schema[] branches) {
    List<Schema> branchList = Arrays.asList(branches);
    try {
      Schema.createUnion(branchList);
      fail("Union should not have constructed from: " + branchList);
    } catch (AvroRuntimeException are) {
      return;
    }
  }

  private static Schema buildUnion(Schema[] branches) {
    List<Schema> branchList = Arrays.asList(branches);
    return Schema.createUnion(branchList);
  }

  /**
   * Makes sure that "doc" tags are transcribed in the schemas.
   * Note that there are docs both for fields and for the records
   * themselves.
   */
  @Test
  public void testDocs() {
    Schema schema = Schema.parse(SCHEMA_WITH_DOC_TAGS);
    assertEquals("This is not a world record.", schema.getDoc());
    assertEquals("Inner Fixed", schema.getField("inner_fixed").doc());
    assertEquals("Very Inner Fixed", schema.getField("inner_fixed").schema().getDoc());
    assertEquals("Inner String", schema.getField("inner_string").doc());
    assertEquals("Inner Enum", schema.getField("inner_enum").doc());
    assertEquals("Very Inner Enum", schema.getField("inner_enum").schema().getDoc());
    assertEquals("Inner Union", schema.getField("inner_union").doc());
  }

  private static void check(String schemaJson, String defaultJson,
                            Object defaultValue) throws Exception {
    check(schemaJson, defaultJson, defaultValue, true);
  }
  private static void check(String schemaJson, String defaultJson,
                            Object defaultValue, boolean induce)
    throws Exception {
    check(schemaJson, induce);
    checkDefault(schemaJson, defaultJson, defaultValue);
  }

  private static void check(String jsonSchema, boolean induce)
    throws Exception {
    Schema schema = Schema.parse(jsonSchema);
    checkProp(schema);
    for (Object datum : new RandomData(schema, COUNT)) {

      if (induce) {
        Schema induced = GenericData.get().induce(datum);
        assertEquals("Induced schema does not match.", schema, induced);
      }
        
      assertTrue("Datum does not validate against schema "+datum,
                 GenericData.get().validate(schema, datum));

      checkBinary(schema, datum,
                  new GenericDatumWriter<Object>(),
                  new GenericDatumReader<Object>());
      checkJson(schema, datum,
                  new GenericDatumWriter<Object>(),
                  new GenericDatumReader<Object>());

      // Check that we can generate the code for every schema we see.
      TestSpecificCompiler.assertCompiles(schema, false);
    }
  }

  private static void checkProp(Schema s0) throws Exception {
    if(s0.getType().equals(Schema.Type.UNION)) return; // unions have no props
    assertEquals(null, s0.getProp("foo"));
    Schema s1 = Schema.parse(s0.toString());
    s1.addProp("foo", "bar");
    assertEquals("bar", s1.getProp("foo"));
    assertFalse(s0.equals(s1));
    Schema s2 = Schema.parse(s1.toString());
    assertEquals("bar", s2.getProp("foo"));
    assertEquals(s1, s2);
    assertFalse(s0.equals(s2));
  }

  public static void checkBinary(Schema schema, Object datum,
                                 DatumWriter<Object> writer,
                                 DatumReader<Object> reader)
    throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.setSchema(schema);
    writer.write(datum, new BinaryEncoder(out));
    byte[] data = out.toByteArray();

    reader.setSchema(schema);
        
    Object decoded =
      reader.read(null, DecoderFactory.defaultFactory().createBinaryDecoder(
          data, null));
      
    assertEquals("Decoded data does not match.", datum, decoded);
  }

  private static void checkJson(Schema schema, Object datum,
                                DatumWriter<Object> writer,
                                DatumReader<Object> reader)
    throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = new JsonEncoder(schema, out);
    writer.setSchema(schema);
    writer.write(datum, encoder);
    writer.write(datum, encoder);
    encoder.flush();
    byte[] data = out.toByteArray();

    reader.setSchema(schema);
    Decoder decoder = new JsonDecoder(schema, new ByteArrayInputStream(data));
    Object decoded = reader.read(null, decoder);
    assertEquals("Decoded data does not match.", datum, decoded);

    decoded = reader.read(decoded, decoder);
    assertEquals("Decoded data does not match.", datum, decoded);
  }

  private static void checkJson(Schema schema, Object datum,
                                String json) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = new JsonEncoder(schema, out);
    DatumWriter<Object> writer = new GenericDatumWriter<Object>();
    writer.setSchema(schema);
    writer.write(datum, encoder);
    encoder.flush();
    byte[] data = out.toByteArray();

    String encoded = new String(data, "UTF-8");
    assertEquals("Encoded data does not match.", json, encoded);

    DatumReader<Object> reader = new GenericDatumReader<Object>();
    reader.setSchema(schema);
    Object decoded =
      reader.read(null, new JsonDecoder(schema,new ByteArrayInputStream(data)));
      
    assertEquals("Decoded data does not match.", datum, decoded);
  }

  private static final Schema ACTUAL =            // an empty record schema
    Schema.parse("{\"type\":\"record\", \"name\":\"Foo\", \"fields\":[]}");

  @SuppressWarnings(value="unchecked")
  private static void checkDefault(String schemaJson, String defaultJson,
                                   Object defaultValue) throws Exception {
    String recordJson =
      "{\"type\":\"record\", \"name\":\"Foo\", \"fields\":[{\"name\":\"f\", "
    +"\"type\":"+schemaJson+", "
    +"\"default\":"+defaultJson+"}]}";
    Schema expected = Schema.parse(recordJson);
    DatumReader in = new GenericDatumReader(ACTUAL, expected);
    GenericData.Record record = (GenericData.Record)
      in.read(null, DecoderFactory.defaultFactory().createBinaryDecoder(
          new byte[0], null));
    assertEquals("Wrong default.", defaultValue, record.get("f"));
    assertEquals("Wrong toString", expected, Schema.parse(expected.toString()));
  }

  @Test(expected=AvroTypeException.class)
  public void testNoDefaultField() throws Exception {
    Schema expected =
      Schema.parse("{\"type\":\"record\", \"name\":\"Foo\", \"fields\":"+
                   "[{\"name\":\"f\", \"type\": \"string\"}]}");
    DatumReader<Object> in = new GenericDatumReader<Object>(ACTUAL, expected);
    in.read(null, DecoderFactory.defaultFactory().createBinaryDecoder(
        new ByteArrayInputStream(new byte[0]), null));
  }

  @Test
  public void testEnumMismatch() throws Exception {
    Schema actual = Schema.parse
      ("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"X\",\"Y\"]}");
    Schema expected = Schema.parse
      ("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"Y\",\"Z\"]}");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<Object> writer = new GenericDatumWriter<Object>(actual);
    Encoder encoder = new BinaryEncoder(out);
    writer.write("Y", encoder);
    writer.write("X", encoder);
    byte[] data = out.toByteArray();
    Decoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(
        data, null);
    DatumReader<String> in = new GenericDatumReader<String>(actual, expected);
    assertEquals("Wrong value", "Y", in.read(null, decoder));
    try {
      in.read(null, decoder);
      fail("Should have thrown exception.");
    } catch (AvroTypeException e) {
      // expected
    }
  }

  @Test(expected=AvroTypeException.class)
  public void testRecordWithPrimitiveName() {
    Schema.parse("{\"type\":\"record\", \"name\":\"string\", \"fields\": []}");
  }
  
  @Test(expected=AvroTypeException.class)
  public void testEnumWithPrimitiveName() {
    Schema.parse("{\"type\":\"enum\", \"name\":\"null\", \"symbols\": [\"A\"]}");
  }
  
  private static Schema enumSchema() {
    return Schema.parse("{ \"type\": \"enum\", \"name\": \"e\", "
        + "\"symbols\": [\"a\", \"b\"]}");
  }

  @Test(expected=AvroRuntimeException.class)
  public void testImmutability1() {
    Schema s = enumSchema();
    s.addProp("p1", "1");
    s.addProp("p1", "2");
  }
  
  @Test(expected=AvroRuntimeException.class)
  public void testImmutability2() {
    Schema s = enumSchema();
    s.addProp("p1", null);
  }

  private static List<String> lockedArrayList() {
    return new Schema.LockableArrayList<String>(Arrays.asList(new String[] {
        "a", "b", "c" })).lock();
  }

  @Test(expected=IllegalStateException.class)
  public void testLockedArrayList1() {
    lockedArrayList().add("p");
  }

  @Test(expected=IllegalStateException.class)
  public void testLockedArrayList2() {
    lockedArrayList().remove("a");
  }

  @Test(expected=IllegalStateException.class)
  public void testLockedArrayList3() {
    lockedArrayList().addAll(Arrays.asList(new String[] { "p" }));
  }

  @Test(expected=IllegalStateException.class)
  public void testLockedArrayList4() {
    lockedArrayList().addAll(0,
        Arrays.asList(new String[] { "p" }));
  }

  @Test(expected=IllegalStateException.class)
  public void testLockedArrayList5() {
    lockedArrayList().
      removeAll(Arrays.asList(new String[] { "a" }));
  }

  @Test(expected=IllegalStateException.class)
  public void testLockedArrayList6() {
    lockedArrayList().
      retainAll(Arrays.asList(new String[] { "a" }));
  }

  @Test(expected=IllegalStateException.class)
  public void testLockedArrayList7() {
    lockedArrayList().clear();
  }

  @Test(expected=IllegalStateException.class)
  public void testLockedArrayList8() {
    lockedArrayList().iterator().remove();
  }


  @Test(expected=IllegalStateException.class)
  public void testLockedArrayList9() {
    Iterator<String> it = lockedArrayList().iterator();
    it.next();
    it.remove();
  }

  @Test(expected=IllegalStateException.class)
  public void testLockedArrayList10() {
    lockedArrayList().remove(1);
  }
}
