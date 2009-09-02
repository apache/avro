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
import java.io.File;
import java.io.IOException;

import org.apache.avro.TestReflect.SampleRecord.AnotherSampleRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.test.Simple;
import org.apache.avro.test.Simple.TestRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestReflect {
  private static final Logger LOG
    = LoggerFactory.getLogger(TestProtocolSpecific.class);

  private static final File FILE = new File("src/test/schemata/simple.avpr");
  private static final Protocol PROTOCOL;
  static {
    try {
      PROTOCOL = Protocol.parse(FILE);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testSchema() throws IOException {
    assertEquals(PROTOCOL.getTypes().get("TestRecord"),
                 ReflectData.get().getSchema(TestRecord.class));
  }

  @Test
  public void testProtocol() throws IOException {
    assertEquals(PROTOCOL, ReflectData.get().getProtocol(Simple.class));
  }

  @Test
  public void testRecord() throws IOException {
    Schema schm = ReflectData.get().getSchema(SampleRecord.class);
    String prefix = getPrefix(SampleRecord.class);
    ReflectDatumWriter writer = new ReflectDatumWriter(schm);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    SampleRecord record = new SampleRecord();
    record.x = 5;
    record.y = 10;
    writer.write(record, new BinaryEncoder(out));
    ReflectDatumReader reader = new ReflectDatumReader(schm, prefix);
    Object decoded =
      reader.read(null, new BinaryDecoder
                  (new ByteArrayInputStream(out.toByteArray())));
    assertEquals(record, decoded);
  }

  @Test
  public void testRecordWithNull() throws IOException {
    ReflectData reflectData = ReflectData.AllowNull.get();
    Schema schm = reflectData.getSchema(AnotherSampleRecord.class);
    String prefix = getPrefix(AnotherSampleRecord.class);
    ReflectDatumWriter writer = new ReflectDatumWriter(schm);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // keep record.a null and see if that works
    AnotherSampleRecord a = new AnotherSampleRecord();
    writer.write(a, new BinaryEncoder(out));
    AnotherSampleRecord b = new AnotherSampleRecord(10);
    writer.write(b, new BinaryEncoder(out));
    ReflectDatumReader reader = new ReflectDatumReader(schm, prefix);
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    Object decoded = reader.read(null, new BinaryDecoder(in));
    assertEquals(a, decoded);
    decoded = reader.read(null, new BinaryDecoder(in));
    assertEquals(b, decoded);
  }

  private String getPrefix(Class<?> c) {
    String prefix =  
      ((c.getEnclosingClass() == null 
        || "null".equals(c.getEnclosingClass())) ? 
       c.getPackage().getName() + "." 
       : (c.getEnclosingClass().getName() + "$"));
    return prefix;
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
      
      public AnotherSampleRecord() {
      }
      
      AnotherSampleRecord(Integer a) {
        this.a = a;
      }

      public int hashCode() {
        return (a != null ? a.hashCode() : 0);
      }

      public boolean equals(Object other) {
        if (other instanceof AnotherSampleRecord) {
          return this.a == ((AnotherSampleRecord)other).a;
        }
        return false;
      }
    }
  }
}
