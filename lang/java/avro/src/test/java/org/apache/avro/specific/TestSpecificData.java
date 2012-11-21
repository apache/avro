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

import java.io.IOException;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import org.apache.avro.FooBarSpecificRecord;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.TypeEnum;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
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

  @Test
  public void testToString() throws IOException {
   FooBarSpecificRecord foo = FooBarSpecificRecord.newBuilder()
           .setId(123)
           .setRelatedids(Arrays.asList(1,2,3))
           .setTypeEnum(TypeEnum.c)
           .build();
    
    String json = foo.toString();
    JsonFactory factory = new JsonFactory();
    JsonParser parser = factory.createJsonParser(json);
    ObjectMapper mapper = new ObjectMapper();
    
    // will throw exception if string is not parsable json
    mapper.readTree(parser);
  }

  static class Reflection {
    public void primitive(int i) {}
    public void primitiveWrapper(Integer i) {}
  }
}
