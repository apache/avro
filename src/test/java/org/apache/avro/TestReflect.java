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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.avro.TestReflect.SampleRecord.AnotherSampleRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import org.junit.Test;

public class TestReflect {

  @Test public void testVoid() {
    check(Void.TYPE, "\"null\"");
    check(Void.class, "\"null\"");
  }

  @Test public void testBoolean() {
    check(Boolean.TYPE, "\"boolean\"");
    check(Boolean.class, "\"boolean\"");
  }

  @Test public void testInt() {
    check(Integer.TYPE, "\"int\"");
    check(Integer.class, "\"int\"");
  }

  @Test public void testLong() {
    check(Long.TYPE, "\"long\"");
    check(Long.class, "\"long\"");
  }

  @Test public void testFloat() {
    check(Float.TYPE, "\"float\"");
    check(Float.class, "\"float\"");
  }

  @Test public void testDouble() {
    check(Double.TYPE, "\"double\"");
    check(Double.class, "\"double\"");
  }

  @Test public void testString() {
    check("Foo", "\"string\"");
  }

  @Test public void testBytes() {
    check(new byte[0], "\"bytes\"");
  }

  public static class R1 {
    private Map<String,String> mapField = new HashMap<String,String>();
    private String[] arrayField = new String[] { "foo" };
    private List<String> listField = new ArrayList<String>();

    {
      mapField.put("foo", "bar");
      listField.add("foo");
    }
    
    public boolean equals(Object o) {
      if (!(o instanceof R1)) return false;
      R1 that = (R1)o;
      return mapField.equals(that.mapField)
        && Arrays.equals(this.arrayField, that.arrayField) 
        &&  listField.equals(that.listField);
    }
  }

  @Test public void testMap() throws Exception {
    check(R1.class.getDeclaredField("mapField").getGenericType(),
          "{\"type\":\"map\",\"values\":\"string\"}");
  }

  @Test public void testArray() throws Exception {
    check(R1.class.getDeclaredField("arrayField").getGenericType(),
          "{\"type\":\"array\",\"items\":\"string\"}");
  }
  @Test public void testList() throws Exception {
    check(R1.class.getDeclaredField("listField").getGenericType(),
          "{\"type\":\"array\",\"items\":\"string\"}");
  }

  @Test public void testR1() throws Exception {
    checkReadWrite(new R1());
  }

  public static class R2 {
    private String[] arrayField;
    private Collection<String> collectionField;
    
    public boolean equals(Object o) {
      if (!(o instanceof R2)) return false;
      R2 that = (R2)o;
      return Arrays.equals(this.arrayField, that.arrayField) 
        &&  collectionField.equals(that.collectionField);
    }
  }

  @Test public void testR2() throws Exception {
    R2 r2 = new R2();
    r2.arrayField = new String[] {"foo"};
    r2.collectionField = new ArrayList<String>();
    r2.collectionField.add("foo");
    checkReadWrite(r2);
  }

  public static class R3 {
    private int[] intArray;
    
    public boolean equals(Object o) {
      if (!(o instanceof R3)) return false;
      R3 that = (R3)o;
      return Arrays.equals(this.intArray, that.intArray);
    }
  }

  @Test public void testR3() throws Exception {
    R3 r3 = new R3();
    r3.intArray = new int[] {1};
    checkReadWrite(r3);
  }

  public static class R4 {
    public short value;
    
    public boolean equals(Object o) {
      if (!(o instanceof R4)) return false;
      return this.value == ((R4)o).value;
    }
  }

  public static class R5 extends R4 {}

  @Test public void testR5() throws Exception {
    R5 r5 = new R5();
    r5.value = 1;
    checkReadWrite(r5);
  }

  void checkReadWrite(Object object) throws Exception {
    Schema s = ReflectData.get().getSchema(object.getClass());
    ReflectDatumWriter writer = new ReflectDatumWriter(s);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.write(object, new BinaryEncoder(out));
    ReflectDatumReader reader = new ReflectDatumReader(s);
    Object after =
      reader.read(null, new BinaryDecoder
                  (new ByteArrayInputStream(out.toByteArray())));
    assertEquals(object, after);
  }

  public static enum E { A, B };
  @Test public void testEnum() throws Exception {
    check(E.class, "{\"type\":\"enum\",\"name\":\"E\",\"namespace\":"
          +"\"org.apache.avro.TestReflect$\",\"symbols\":[\"A\",\"B\"]}");
  }

  public static class R { int a; long b; }
  @Test public void testRecord() throws Exception {
    check(R.class, "{\"type\":\"record\",\"name\":\"R\",\"namespace\":"
          +"\"org.apache.avro.TestReflect$\",\"fields\":["
          +"{\"name\":\"a\",\"type\":\"int\"},"
          +"{\"name\":\"b\",\"type\":\"long\"}]}");
  }

  private void check(Object o, String schemaJson) {
    check(o.getClass(), schemaJson);
  }

  private void check(Type type, String schemaJson) {
    assertEquals(schemaJson, ReflectData.get().getSchema(type).toString());
  }

  @Test
  public void testRecordIO() throws IOException {
    Schema schm = ReflectData.get().getSchema(SampleRecord.class);
    ReflectDatumWriter writer = new ReflectDatumWriter(schm);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    SampleRecord record = new SampleRecord();
    record.x = 5;
    record.y = 10;
    writer.write(record, new BinaryEncoder(out));
    ReflectDatumReader reader = new ReflectDatumReader(schm);
    Object decoded =
      reader.read(null, new BinaryDecoder
                  (new ByteArrayInputStream(out.toByteArray())));
    assertEquals(record, decoded);
  }

  @Test
  public void testRecordWithNullIO() throws IOException {
    ReflectData reflectData = ReflectData.AllowNull.get();
    Schema schm = reflectData.getSchema(AnotherSampleRecord.class);
    ReflectDatumWriter writer = new ReflectDatumWriter(schm);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // keep record.a null and see if that works
    AnotherSampleRecord a = new AnotherSampleRecord();
    writer.write(a, new BinaryEncoder(out));
    AnotherSampleRecord b = new AnotherSampleRecord(10);
    writer.write(b, new BinaryEncoder(out));
    ReflectDatumReader reader = new ReflectDatumReader(schm);
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    Object decoded = reader.read(null, new BinaryDecoder(in));
    assertEquals(a, decoded);
    decoded = reader.read(null, new BinaryDecoder(in));
    assertEquals(b, decoded);
  }

  public static class SampleRecord {
    public int x = 1;
    private int y = 2;

    public int hashCode() {
      return x + y;
    }

    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      final SampleRecord other = (SampleRecord)obj;
      if (x != other.x)
        return false;
      if (y != other.y)
        return false;
      return true;
    }
    
    public static class AnotherSampleRecord {
      private Integer a = null;
      private SampleRecord s = null;

      public AnotherSampleRecord() {
      }

      AnotherSampleRecord(Integer a) {
        this.a = a;
        this.s = new SampleRecord();
      }

      public int hashCode() {
        int hash = (a != null ? a.hashCode() : 0);
        hash += (s != null ? s.hashCode() : 0);
        return hash;
      }

      public boolean equals(Object other) {
        if (other instanceof AnotherSampleRecord) {
          AnotherSampleRecord o = (AnotherSampleRecord) other;
          if ( (this.a == null && o.a != null) ||
               (this.a != null && !this.a.equals(o.a)) ||
               (this.s == null && o.s != null) ||
               (this.s != null && !this.s.equals(o.s)) ) {
            return false;
          }
          return true;
        } else {
          return false;
        }
      }
    }
  }

  public static class X { int i; }
  public static class B1 { X x; }
  public static class B2 { X x; }
  public static class A { B1 b1; B2 b2; }
  public static interface C { void foo(A a); }

  @Test
  public void testForwardReference() {
    ReflectData data = ReflectData.get();
    Protocol reflected = data.getProtocol(C.class);
    Protocol reparsed = Protocol.parse(reflected.toString());
    assertEquals(reflected, reparsed);
    assert(reparsed.getTypes().contains(data.getSchema(A.class)));
    assert(reparsed.getTypes().contains(data.getSchema(B1.class)));
    assert(reparsed.getTypes().contains(data.getSchema(B2.class)));
    assert(reparsed.getTypes().contains(data.getSchema(X.class)));
  }

}
