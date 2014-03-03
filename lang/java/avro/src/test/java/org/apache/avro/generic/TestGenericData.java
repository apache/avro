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
package org.apache.avro.generic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.ArrayDeque;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.BinaryData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import org.junit.Test;

public class TestGenericData {
  
  @Test(expected=AvroRuntimeException.class)
    public void testrecordConstructorNullSchema() throws Exception {
    new GenericData.Record(null);
  }
    
  @Test(expected=AvroRuntimeException.class)
    public void testrecordConstructorWrongSchema() throws Exception {
    new GenericData.Record(Schema.create(Schema.Type.INT));
  }

  @Test(expected=AvroRuntimeException.class)
    public void testArrayConstructorNullSchema() throws Exception {
    new GenericData.Array<Object>(1, null);
  }
    
  @Test(expected=AvroRuntimeException.class)
    public void testArrayConstructorWrongSchema() throws Exception {
    new GenericData.Array<Object>(1, Schema.create(Schema.Type.INT));
  }

  @Test(expected=AvroRuntimeException.class)
  public void testRecordCreateEmptySchema() throws Exception {
    Schema s = Schema.createRecord("schemaName", "schemaDoc", "namespace", false);
    Record r = new GenericData.Record(s);
  }

  @Test(expected=AvroRuntimeException.class)
  public void testGetEmptySchemaFields() throws Exception {
    Schema s = Schema.createRecord("schemaName", "schemaDoc", "namespace", false);
    s.getFields();
  }

  @Test(expected=AvroRuntimeException.class)
  public void testGetEmptySchemaField() throws Exception {
    Schema s = Schema.createRecord("schemaName", "schemaDoc", "namespace", false);
    s.getField("foo");
  }

  @Test(expected=AvroRuntimeException.class)
  public void testRecordPutInvalidField() throws Exception {
    Schema s = Schema.createRecord("schemaName", "schemaDoc", "namespace", false);
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    fields.add(new Schema.Field("someFieldName", s, "docs", null));
    s.setFields(fields);
    Record r = new GenericData.Record(s);
    r.put("invalidFieldName", "someValue");
  }
  
  @Test
  /** Make sure that even with nulls, hashCode() doesn't throw NPE. */
  public void testHashCode() {
    GenericData.get().hashCode(null, Schema.create(Type.NULL));
    GenericData.get().hashCode(null, Schema.createUnion(
        Arrays.asList(Schema.create(Type.BOOLEAN), Schema.create(Type.STRING))));
    List<CharSequence> stuff = new ArrayList<CharSequence>();
    stuff.add("string");
    Schema schema = recordSchema();
    GenericRecord r = new GenericData.Record(schema);
    r.put(0, stuff);
    GenericData.get().hashCode(r, schema);
  }
  
  @Test
  public void testEquals() {
    Schema s = recordSchema();
    GenericRecord r0 = new GenericData.Record(s);
    GenericRecord r1 = new GenericData.Record(s);
    GenericRecord r2 = new GenericData.Record(s);
    Collection<CharSequence> l0 = new ArrayDeque<CharSequence>();
    List<CharSequence> l1 = new ArrayList<CharSequence>();
    GenericArray<CharSequence> l2 = 
      new GenericData.Array<CharSequence>(1,s.getFields().get(0).schema());
    String foo = "foo";
    l0.add(new StringBuffer(foo));
    l1.add(foo);
    l2.add(new Utf8(foo));
    r0.put(0, l0);
    r1.put(0, l1);
    r2.put(0, l2);
    assertEquals(r0, r1);
    assertEquals(r0, r2);
    assertEquals(r1, r2);
  }
  
  private Schema recordSchema() {
    List<Field> fields = new ArrayList<Field>();
    fields.add(new Field("anArray", Schema.createArray(Schema.create(Type.STRING)), null, null));
    Schema schema = Schema.createRecord("arrayFoo", "test", "mytest", false);
    schema.setFields(fields);
    
    return schema;
  }

  @Test public void testEquals2() {
   Schema schema1 = Schema.createRecord("r", null, "x", false);
   List<Field> fields1 = new ArrayList<Field>();
   fields1.add(new Field("a", Schema.create(Schema.Type.STRING), null, null,
                         Field.Order.IGNORE));
   schema1.setFields(fields1);

   // only differs in field order
   Schema schema2 = Schema.createRecord("r", null, "x", false);
   List<Field> fields2 = new ArrayList<Field>();
   fields2.add(new Field("a", Schema.create(Schema.Type.STRING), null, null,
                         Field.Order.ASCENDING));
   schema2.setFields(fields2);

   GenericRecord record1 = new GenericData.Record(schema1);
   record1.put("a", "1");

   GenericRecord record2 = new GenericData.Record(schema2);
   record2.put("a", "2");

   assertFalse(record2.equals(record1));
   assertFalse(record1.equals(record2));
  }

  @Test
  public void testRecordGetFieldDoesntExist() throws Exception {
    List<Field> fields = new ArrayList<Field>();
    Schema schema = Schema.createRecord(fields);
    GenericData.Record record = new GenericData.Record(schema);
    assertNull(record.get("does not exist"));
  }
  
  @Test
  public void testArrayReversal() {
      Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
      GenericArray<Integer> forward = new GenericData.Array<Integer>(10, schema);
      GenericArray<Integer> backward = new GenericData.Array<Integer>(10, schema);
      for (int i = 0; i <= 9; i++) {
        forward.add(i);
      }
      for (int i = 9; i >= 0; i--) {
        backward.add(i);
      }
      forward.reverse();
      assertTrue(forward.equals(backward));
  }

  @Test
  public void testArrayListInterface() {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
    GenericArray<Integer> array = new GenericData.Array<Integer>(1, schema);
    array.add(99);
    assertEquals(new Integer(99), array.get(0));
    List<Integer> list = new ArrayList<Integer>();
    list.add(99);
    assertEquals(array, list);
    assertEquals(list, array);
    assertEquals(list.hashCode(), array.hashCode());
    try {
      array.get(2);
      fail("Expected IndexOutOfBoundsException getting index 2");
    } catch (IndexOutOfBoundsException e) {}
    array.clear();
    assertEquals(0, array.size());
    try {
      array.get(0);
      fail("Expected IndexOutOfBoundsException getting index 0 after clear()");
    } catch (IndexOutOfBoundsException e) {}

  }
  @Test
  public void testArrayAddAtLocation()
  {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
    GenericArray<Integer> array = new GenericData.Array<Integer>(6, schema);
    array.clear();
    for(int i=0; i<5; ++i)
      array.add(i);
    assertEquals(5, array.size());
    array.add(0, 6);
    assertEquals(new Integer(6), array.get(0));
    assertEquals(6, array.size());
    assertEquals(new Integer(0), array.get(1));
    assertEquals(new Integer(4), array.get(5));
    array.add(6, 7);
    assertEquals(new Integer(7), array.get(6));
    assertEquals(7, array.size());
    assertEquals(new Integer(6), array.get(0));
    assertEquals(new Integer(4), array.get(5));
    array.add(1, 8);
    assertEquals(new Integer(8), array.get(1));
    assertEquals(new Integer(0), array.get(2));
    assertEquals(new Integer(6), array.get(0));
    assertEquals(8, array.size());
    try {
	array.get(9);
	fail("Expected IndexOutOfBoundsException after adding elements");
    } catch (IndexOutOfBoundsException e){}
  }
  @Test
  public void testArrayRemove()
  {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
    GenericArray<Integer> array = new GenericData.Array<Integer>(10, schema);
    array.clear();
    for(int i=0; i<10; ++i)
      array.add(i);
    assertEquals(10, array.size());
    assertEquals(new Integer(0), array.get(0));
    assertEquals(new Integer(9), array.get(9));

    array.remove(0);
    assertEquals(9, array.size());
    assertEquals(new Integer(1), array.get(0));
    assertEquals(new Integer(2), array.get(1));
    assertEquals(new Integer(9), array.get(8));

    // Test boundary errors.
    try {
      array.get(9);
      fail("Expected IndexOutOfBoundsException after removing an element");
    } catch (IndexOutOfBoundsException e){}
    try {
      array.set(9, 99);
      fail("Expected IndexOutOfBoundsException after removing an element");
    } catch (IndexOutOfBoundsException e){}
    try {
      array.remove(9);
      fail("Expected IndexOutOfBoundsException after removing an element");
    } catch (IndexOutOfBoundsException e){}

    // Test that we can still remove for properly sized arrays, and the rval
    assertEquals(new Integer(9), array.remove(8));
    assertEquals(8, array.size());


    // Test insertion after remove
    array.add(88);
    assertEquals(new Integer(88), array.get(8));
  }
  @Test
  public void testArraySet()
  {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
    GenericArray<Integer> array = new GenericData.Array<Integer>(10, schema);
    array.clear();
    for(int i=0; i<10; ++i)
      array.add(i);
    assertEquals(10, array.size());
    assertEquals(new Integer(0), array.get(0));
    assertEquals(new Integer(5), array.get(5));

    assertEquals(new Integer(5), array.set(5, 55));
    assertEquals(10, array.size());
    assertEquals(new Integer(55), array.get(5));
  }
  
  @Test
  public void testToStringIsJson() throws JsonParseException, IOException {
    Field stringField = new Field("string", Schema.create(Type.STRING), null, null);
    Field enumField = new Field("enum", Schema.createEnum("my_enum", "doc", null, Arrays.asList("a", "b", "c")), null, null);
    Schema schema = Schema.createRecord("my_record", "doc", "mytest", false);
    schema.setFields(Arrays.asList(stringField, enumField));
    
    GenericRecord r = new GenericData.Record(schema);
    // \u2013 is EN DASH
    r.put(stringField.name(), "hello\nthere\"\tyou\u2013}");
    r.put(enumField.name(), new GenericData.EnumSymbol(enumField.schema(),"a"));
    
    String json = r.toString();
    JsonFactory factory = new JsonFactory();
    JsonParser parser = factory.createJsonParser(json);
    ObjectMapper mapper = new ObjectMapper();
    
    // will throw exception if string is not parsable json
    mapper.readTree(parser);
  }

  @Test public void testToStringNanInfinity() throws Exception {
    GenericData data = GenericData.get();
    assertEquals("\"Infinity\"",data.toString(Float.POSITIVE_INFINITY));
    assertEquals("\"-Infinity\"",data.toString(Float.NEGATIVE_INFINITY));
    assertEquals("\"NaN\"", data.toString(Float.NaN));
    assertEquals("\"Infinity\"",data.toString(Double.POSITIVE_INFINITY));
    assertEquals("\"-Infinity\"",data.toString(Double.NEGATIVE_INFINITY));
    assertEquals("\"NaN\"", data.toString(Double.NaN));
  }

  @Test
  public void testCompare() {
    // Prepare a schema for testing.
    Field integerField = new Field("test", Schema.create(Type.INT), null, null);
    List<Field> fields = new ArrayList<Field>();
    fields.add(integerField);
    Schema record = Schema.createRecord("test", null, null, false);
    record.setFields(fields);
    
    ByteArrayOutputStream b1 = new ByteArrayOutputStream(5);
    ByteArrayOutputStream b2 = new ByteArrayOutputStream(5);
    BinaryEncoder b1Enc = EncoderFactory.get().binaryEncoder(b1, null);
    BinaryEncoder b2Enc = EncoderFactory.get().binaryEncoder(b2, null);
    // Prepare two different datums
    Record testDatum1 = new Record(record);
    testDatum1.put(0, 1);
    Record testDatum2 = new Record(record);
    testDatum2.put(0, 2);
    GenericDatumWriter<Record> gWriter = new GenericDatumWriter<Record>(record);
    Integer start1 = 0, start2 = 0;
    try {
      // Write two datums in each stream
      // and get the offset length after the first write in each.
      gWriter.write(testDatum1, b1Enc);
      b1Enc.flush();
      start1 = b1.size();
      gWriter.write(testDatum1, b1Enc);
      b1Enc.flush();
      b1.close();
      gWriter.write(testDatum2, b2Enc);
      b2Enc.flush();
      start2 = b2.size();
      gWriter.write(testDatum2, b2Enc);
      b2Enc.flush();
      b2.close();
      // Compare to check if offset-based compare works right.
      assertEquals(-1, BinaryData.compare(b1.toByteArray(), start1, b2.toByteArray(), start2, record));
    } catch (IOException e) {
      fail("IOException while writing records to output stream.");
    }
  }
  
  @Test
  public void testEnumCompare() {
    Schema s = Schema.createEnum("Kind",null,null,Arrays.asList("Z","Y","X"));
    GenericEnumSymbol z = new GenericData.EnumSymbol(s, "Z");
    GenericEnumSymbol y = new GenericData.EnumSymbol(s, "Y");
    assertEquals(0, z.compareTo(z));
    assertTrue(y.compareTo(z) > 0);
    assertTrue(z.compareTo(y) < 0);
  }

  @Test
  public void testByteBufferDeepCopy() {
    // Test that a deep copy of a byte buffer respects the byte buffer
    // limits and capacity.
    byte[] buffer_value = {0, 1, 2, 3, 0, 0, 0};
    ByteBuffer buffer = ByteBuffer.wrap(buffer_value, 1, 4);
    Schema schema = Schema.createRecord("my_record", "doc", "mytest", false);
    Field byte_field =
      new Field("bytes", Schema.create(Type.BYTES), null, null);
    schema.setFields(Arrays.asList(byte_field));
    
    GenericRecord record = new GenericData.Record(schema);
    record.put(byte_field.name(), buffer);
    
    GenericRecord copy = GenericData.get().deepCopy(schema, record);
    ByteBuffer buffer_copy = (ByteBuffer) copy.get(byte_field.name());

    assertEquals(buffer, buffer_copy);
  }
}
