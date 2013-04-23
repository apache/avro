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

package org.apache.avro.specific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.junit.Before;
import org.junit.Test;

/*
 * If integerClass is primitive, reflection to find method will
 * result in a NoSuchMethodException in the case of a UNION schema
 */
public class TestSpecificData {

  private Class<?> intClass;
  private Class<?> integerClass;

  @Before
  public void setUp() {
    Schema intSchema = Schema.create(Type.INT);
    intClass = SpecificData.get().getClass(intSchema);
    Schema nullSchema = Schema.create(Type.NULL);
    Schema nullIntUnionSchema = Schema.createUnion(Arrays.asList(nullSchema, intSchema));
    integerClass = SpecificData.get().getClass(nullIntUnionSchema);
  }

  @Test
  public void testClassTypes() {
    assertTrue(intClass.isPrimitive());
    assertFalse(integerClass.isPrimitive());
  }

  @Test
  public void testPrimitiveParam() throws Exception {
    assertNotNull(Reflection.class.getMethod("primitive", intClass));
  }

  @Test(expected = NoSuchMethodException.class)
  public void testPrimitiveParamError() throws Exception {
    Reflection.class.getMethod("primitiveWrapper", intClass);
  }

  @Test
  public void testPrimitiveWrapperParam() throws Exception {
    assertNotNull(Reflection.class.getMethod("primitiveWrapper", integerClass));
  }

  @Test(expected = NoSuchMethodException.class)
  public void testPrimitiveWrapperParamError() throws Exception {
    Reflection.class.getMethod("primitive", integerClass);
  }

  static class Reflection {
    public void primitive(int i) {}
    public void primitiveWrapper(Integer i) {}
  }

  private static class TestRecord extends SpecificRecordBase {
    private static final Schema SCHEMA = Schema.createRecord("TestRecord", null, null, false);
    static {
      List<Field> fields = new ArrayList<Field>();
      fields.add(new Field("x", Schema.create(Type.INT), null, null));
      fields.add(new Field("y", Schema.create(Type.STRING), null, null));
      SCHEMA.setFields(fields);
    }
    private int x;
    private String y;

    @Override
    public void put(int i, Object v) {
      switch (i) {
      case 0: x = (Integer) v; break;
      case 1: y = (String) v; break;
      default: throw new RuntimeException();
      }
    }

    @Override
    public Object get(int i) {
      switch (i) {
      case 0: return x;
      case 1: return y;
      }
      throw new RuntimeException();
    }

    @Override
    public Schema getSchema() {
      return SCHEMA;
    }
  }

  @Test
  public void testSpecificRecordBase() {
    final TestRecord record = new TestRecord();
    record.put("x", 1);
    record.put("y", "str");
    assertEquals(1, record.get("x"));
    assertEquals("str", record.get("y"));
  }
}
