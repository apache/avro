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
import org.apache.avro.util.Utf8;

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
}
