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
package org.apache.avro.util.internal;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Field.Order;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;

public class Accessor {
  public abstract static class JsonPropertiesAccessor {
    protected abstract void addProp(JsonProperties props, String name, JsonNode value);

    protected abstract JsonNode getJsonProp(JsonProperties props, String name);

    protected abstract Map<String, JsonNode> getJsonProps(JsonProperties props);
  }

  public abstract static class FieldAccessor {
    protected abstract JsonNode defaultValue(Field field);

    protected abstract Field createField(String name, Schema schema, String doc, JsonNode defaultValue, Order order);

    protected abstract Field createField(String name, Schema schema, String doc, JsonNode defaultValue);
  }

  public abstract static class SchemaAccessor {
    protected abstract JsonNode parseJson(String value);
  }

  public abstract static class ResolvingGrammarGeneratorAccessor {
    protected abstract void encode(Encoder e, Schema s, JsonNode n) throws IOException;
  }

  public abstract static class EncoderFactoryAccessor {
    protected abstract JsonEncoder jsonEncoder(EncoderFactory factory, Schema schema, JsonGenerator gen)
        throws IOException;
  }

  private static volatile JsonPropertiesAccessor jsonPropertiesAccessor;

  private static volatile FieldAccessor fieldAccessor;

  private static volatile SchemaAccessor schemaAccessor;

  private static volatile ResolvingGrammarGeneratorAccessor resolvingGrammarGeneratorAccessor;

  private static volatile EncoderFactoryAccessor encoderFactoryAccessor;

  public static void setAccessor(JsonPropertiesAccessor accessor) {
    if (jsonPropertiesAccessor != null)
      throw new IllegalStateException("JsonPropertiesAccessor already initialized");
    jsonPropertiesAccessor = accessor;
  }

  public static void setAccessor(FieldAccessor accessor) {
    if (fieldAccessor != null)
      throw new IllegalStateException("FieldAccessor already initialized");
    fieldAccessor = accessor;
  }

  private static FieldAccessor fieldAccessor() {
    if (fieldAccessor == null)
      ensureLoaded(Field.class);
    return fieldAccessor;
  }

  public static void setAccessor(SchemaAccessor accessor) {
    if (schemaAccessor != null)
      throw new IllegalStateException("SchemaAccessor already initialized");
    schemaAccessor = accessor;
  }

  private static SchemaAccessor schemaAccessor() {
    if (schemaAccessor == null)
      ensureLoaded(Schema.class);
    return schemaAccessor;
  }

  public static void setAccessor(ResolvingGrammarGeneratorAccessor accessor) {
    if (resolvingGrammarGeneratorAccessor != null)
      throw new IllegalStateException("ResolvingGrammarGeneratorAccessor already initialized");
    resolvingGrammarGeneratorAccessor = accessor;
  }

  private static ResolvingGrammarGeneratorAccessor resolvingGrammarGeneratorAccessor() {
    if (resolvingGrammarGeneratorAccessor == null)
      ensureLoaded(ResolvingGrammarGenerator.class);
    return resolvingGrammarGeneratorAccessor;
  }

  public static void setAccessor(EncoderFactoryAccessor accessor) {
    if (encoderFactoryAccessor != null)
      throw new IllegalStateException("EncoderFactoryAccessor already initialized");
    encoderFactoryAccessor = accessor;
  }

  private static void ensureLoaded(Class<?> c) {
    try {
      Class.forName(c.getName());
    } catch (ClassNotFoundException e) {
      // Shall never happen as the class is specified by its Class instance
    }
  }

  public static void addProp(JsonProperties props, String name, JsonNode value) {
    jsonPropertiesAccessor.addProp(props, name, value);
  }

  public static JsonNode getJsonProp(JsonProperties props, String name) {
    return jsonPropertiesAccessor.getJsonProp(props, name);
  }

  public static JsonNode defaultValue(Field field) {
    return fieldAccessor.defaultValue(field);
  }

  public static void encode(Encoder e, Schema s, JsonNode n) throws IOException {
    resolvingGrammarGeneratorAccessor().encode(e, s, n);
  }

  public static JsonNode parseJson(String value) {
    return schemaAccessor().parseJson(value);
  }

  public static Field createField(String name, Schema schema, String doc, JsonNode defaultValue, Order order) {
    return fieldAccessor().createField(name, schema, doc, defaultValue, order);
  }

  public static Field createField(String name, Schema schema, String doc, JsonNode defaultValue) {
    return fieldAccessor().createField(name, schema, doc, defaultValue);
  }

  public static Map<String, JsonNode> getJsonProps(JsonProperties props) {
    return jsonPropertiesAccessor.getJsonProps(props);
  }

  public static JsonEncoder jsonEncoder(EncoderFactory factory, Schema schema, JsonGenerator gen) throws IOException {
    return encoderFactoryAccessor.jsonEncoder(factory, schema, gen);
  }

}
