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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestReflectData {
  @Test
  @SuppressWarnings("unchecked")
  public void testWeakSchemaCaching() throws Exception {
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
  public void testGenericProtocol() {
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

  protected static class DefaultReflector extends ReflectData {
    private static final DefaultReflector INSTANCE = new DefaultReflector();

    /** Return the singleton instance. */
    public static DefaultReflector get() {
      return INSTANCE;
    }

    private final Map<String, Object> defaultValues = new ConcurrentHashMap<>();

    protected Object getOrCreateDefaultValue(String className) {
      return this.defaultValues.computeIfAbsent(className, ignored -> {
        try {
          Class<?> aClass = Class.forName(className);
          Constructor constructor = aClass.getDeclaredConstructor();
          constructor.setAccessible(true);
          return constructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException
            | InvocationTargetException e) {
          e.printStackTrace();
        }
        return null;
      });
    }

    @Override
    protected Object createSchemaDefaultValue(Type type, Field field, Schema fieldSchema) {
      String className = ((Class) type).getName();
      field.setAccessible(true);
      Object def = null;

      try {
        Object value = getOrCreateDefaultValue(className);
        if (value != null) {
          def = field.get(value);
        }
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }

      if (def == null) {
        def = super.createSchemaDefaultValue(type, field, fieldSchema);
      }

      return def;
    }
  }

  static class User {
    public String first = "Avro";
    public String last = "Apache";
  }

  static class Meta {
    public int f1 = 55;
    public String f2 = "a-string";
    public List<String> f3 = Arrays.asList("one", "two", "three");
    // public User usr = new User();
  }

  protected static Map objectToMap(Object datum) {
    ObjectMapper mapper = new ObjectMapper();
    // we only care about fields
    mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    return mapper.convertValue(datum, Map.class);
  }

  @Test
  public void testCreateSchemaDefaultValue() {
    Schema schema = DefaultReflector.get().getSchema(Meta.class);

    final String schemaString = schema.toString(true);

    Schema.Parser parser = new Schema.Parser();
    Schema cloneSchema = parser.parse(schemaString);

    Map testCases = objectToMap(new Meta());

    for (Schema.Field field : cloneSchema.getFields()) {
      assertEquals(field.defaultVal(), testCases.get(field.name()));
    }
  }
}
