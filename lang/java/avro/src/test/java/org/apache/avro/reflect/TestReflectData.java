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

package org.apache.avro.reflect;

import com.sun.org.apache.xml.internal.security.signature.reference.ReferenceData;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.util.internal.JacksonUtils;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestReflectData {
  @Test
  @SuppressWarnings("unchecked")
  void weakSchemaCaching() throws Exception {
    int numSchemas = 1000000;
    for (int i = 0; i < numSchemas; i++) {
      // Create schema
      Schema schema = Schema.createRecord("schema", null, null, false);
      schema.setFields(Collections.emptyList());

      ReflectData.get().getRecordState(new Object(), schema);
    }

    // Reflect the number of schemas currently in the cache
    ReflectData.ClassAccessorData classData = ReflectData.ACCESSOR_CACHE.get(Object.class);

    System.gc(); // Not guaranteed, but seems to be reliable enough

    assertThat("ReflectData cache should release references", classData.bySchema.size(), lessThan(numSchemas));
  }

  @Test
  void genericProtocol() {
    Protocol protocol = ReflectData.get().getProtocol(FooBarProtocol.class);
    Schema recordSchema = ReflectData.get().getSchema(FooBarReflectiveRecord.class);

    assertThat(protocol.getTypes(), contains(recordSchema));

    assertThat(protocol.getMessages().keySet(), containsInAnyOrder("store", "findById", "exists"));

    Schema.Field storeArgument = protocol.getMessages().get("store").getRequest().getFields().get(0);
    assertThat(storeArgument.schema(), equalTo(recordSchema));

    Schema.Field findByIdArgument = protocol.getMessages().get("findById").getRequest().getFields().get(0);
    assertThat(findByIdArgument.schema(), equalTo(Schema.create(Schema.Type.STRING)));

    Schema findByIdResponse = protocol.getMessages().get("findById").getResponse();
    assertThat(findByIdResponse, equalTo(recordSchema));

    Schema.Field existsArgument = protocol.getMessages().get("exists").getRequest().getFields().get(0);
    assertThat(existsArgument.schema(), equalTo(Schema.create(Schema.Type.STRING)));
  }

  @Test
  void fieldsOrder() throws Exception {
    Schema schema = ReflectData.get().getSchema(Meta.class);
    List<Schema.Field> fields = schema.getFields();
    assertEquals(fields.size(), 4);
    assertEquals(fields.get(0).name(), "f1");
    assertEquals(fields.get(1).name(), "f2");
    assertEquals(fields.get(2).name(), "f3");
    assertEquals(fields.get(3).name(), "f4");

    Field orderReflectFields = ReflectData.class.getDeclaredField("ORDER_REFLECT_FIELDS");
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.set(orderReflectFields, orderReflectFields.getModifiers() & ~Modifier.FINAL);
    orderReflectFields.setAccessible(true);
    orderReflectFields.set(null, false);

    schema = ReflectData.get().getSchema(Meta1.class);
    fields = schema.getFields();
    assertEquals(fields.size(), 4);
    assertEquals(fields.get(0).name(), "f1");
    assertEquals(fields.get(1).name(), "f4");
    assertEquals(fields.get(2).name(), "f2");
    assertEquals(fields.get(3).name(), "f3");

    orderReflectFields.set(null, true);
  }

  private interface CrudProtocol<R, I> extends OtherProtocol<I> {
    void store(R record);

    R findById(I id);
  }

  private interface OtherProtocol<G> {
    boolean exists(G id);
  }

  private interface FooBarProtocol extends OtherProtocol<String>, CrudProtocol<FooBarReflectiveRecord, String> {
  }

  private static class FooBarReflectiveRecord {
    private String bar;
    private int baz;
  }

  static class User {
    public String first = "Avro";
    public String last = "Apache";
  }

  static class Meta {
    public int f1 = 55;
    public int f4;
    public String f2 = "a-string";
    public List<String> f3 = Arrays.asList("one", "two", "three");
    // public User usr = new User();
  }

  static class Meta1 {
    public int f1 = 55;
    public int f4;
    public String f2 = "a-string";
    public List<String> f3 = Arrays.asList("one", "two", "three");
    // public User usr = new User();
  }

  @Test
  void createSchemaDefaultValue() {
    Meta meta = new Meta();
    validateSchema(meta);

    meta.f4 = 0x1987;
    validateSchema(meta);
  }

  private void validateSchema(Meta meta) {
    Schema schema = new ReflectData().setDefaultsGenerated(true).setDefaultGeneratedValue(Meta.class, meta)
        .getSchema(Meta.class);

    final String schemaString = schema.toString(true);

    Schema.Parser parser = new Schema.Parser();
    Schema cloneSchema = parser.parse(schemaString);

    Map testCases = JacksonUtils.objectToMap(meta);

    for (Schema.Field field : cloneSchema.getFields()) {
      assertEquals(field.defaultVal(), testCases.get(field.name()), "Invalid field " + field.name());
    }
  }

  public class Definition {
    public Map<String, String> tokens;
  }

  @Test
  void nonStaticInnerClasses() {
    assertThrows(AvroTypeException.class, () -> {
      ReflectData.get().getSchema(Definition.class);
    });
  }

  @Test
  void staticInnerClasses() {
    ReflectData.get().getSchema(Meta.class);
  }
}
