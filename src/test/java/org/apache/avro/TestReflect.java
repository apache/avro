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

import org.apache.avro.reflect.*;
import org.apache.avro.io.*;
import org.apache.avro.test.Simple;
import org.apache.avro.test.Simple.TestRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.*;

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
                 ReflectData.getSchema(TestRecord.class));
  }

  @Test
  public void testProtocol() throws IOException {
    assertEquals(PROTOCOL, ReflectData.getProtocol(Simple.class));
  }

  @Test
  public void testRecord() throws IOException {
    Schema schm = ReflectData.getSchema(SampleRecord.class);
    Class<?> c = SampleRecord.class;
    String prefix =  
      ((c.getEnclosingClass() == null 
        || "null".equals(c.getEnclosingClass())) ? 
       c.getPackage().getName() + "." 
       : (c.getEnclosingClass().getName() + "$"));
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
  }
}
