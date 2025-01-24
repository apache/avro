/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.IOException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.io.BinaryData;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import org.apache.avro.test.TestRecord;
import org.apache.avro.test.Kind;
import org.apache.avro.test.MD5;

public class TestCompare {

  @Test
  void specificRecord() throws Exception {
    TestRecord s1 = new TestRecord();
    TestRecord s2 = new TestRecord();
    s1.setName("foo");
    s1.setKind(Kind.BAZ);
    s1.setHash(new MD5(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5 }));
    s2.setName("bar");
    s2.setKind(Kind.BAR);
    s2.setHash(new MD5(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 6 }));
    Schema schema = SpecificData.get().getSchema(TestRecord.class);

    check(schema, s1, s2, true, new SpecificDatumWriter<>(schema), SpecificData.get());
    s2.setKind(Kind.BAZ);
    check(schema, s1, s2, true, new SpecificDatumWriter<>(schema), SpecificData.get());
  }

  private static <T> void check(Schema schema, T o1, T o2, boolean comparable, DatumWriter<T> writer,
      GenericData comparator) throws Exception {

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

    assert (o1.equals(o1));
    assert (o2.equals(o2));
    assert (!o1.equals(o2));
    assert (!o2.equals(o1));
    assert (!o1.equals(new Object()));
    assert (!o2.equals(new Object()));
    assert (!o1.equals(null));
    assert (!o2.equals(null));

    assert (o1.hashCode() != o2.hashCode());

    // check BinaryData.hashCode against Object.hashCode
    if (schema.getType() != Schema.Type.ENUM) {
      assertEquals(o1.hashCode(), BinaryData.hashCode(b1, 0, b1.length, schema));
      assertEquals(o2.hashCode(), BinaryData.hashCode(b2, 0, b2.length, schema));
    }

    // check BinaryData.hashCode against GenericData.hashCode
    assertEquals(comparator.hashCode(o1, schema), BinaryData.hashCode(b1, 0, b1.length, schema));
    assertEquals(comparator.hashCode(o2, schema), BinaryData.hashCode(b2, 0, b2.length, schema));

  }

  @SuppressWarnings(value = "unchecked")
  private static int compare(Object o1, Object o2, Schema schema, boolean comparable, GenericData comparator) {
    return comparable ? ((Comparable<Object>) o1).compareTo(o2) : comparator.compare(o1, o2, schema);
  }

  private static <T> byte[] render(T datum, Schema schema, DatumWriter<T> writer) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.setSchema(schema);
    Encoder enc = new EncoderFactory().directBinaryEncoder(out, null);
    writer.write(datum, enc);
    enc.flush();
    return out.toByteArray();
  }
}
