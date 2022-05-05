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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.AvroMissingFieldException;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.ResolvingDecoder;

public class ReflectRecordEncoding extends CustomEncoding<Object> {

  private final Class<?> type;
  private final List<FieldWriter> writer;
  private final Constructor<?> constructor;
  private List<FieldReader> reader;

  public ReflectRecordEncoding(Class<?> type) {
    this.type = type;
    this.writer = null;
    this.constructor = null;

  }

  public ReflectRecordEncoding(Class<?> type, Schema schema) {
    this.type = type;
    this.schema = schema;
    this.writer = schema.getFields().stream().map(field -> {
      try {
        Field classField = type.getDeclaredField(field.name());
        classField.setAccessible(true);
        AvroEncode enc = classField.getAnnotation(AvroEncode.class);
        if (enc != null)
          return new CustomEncodedFieldWriter(classField, enc.using().getDeclaredConstructor().newInstance());
        return new ReflectFieldWriter(classField, field.schema());
      } catch (ReflectiveOperationException e) {
        throw new AvroMissingFieldException("Field does not exist", field);
      }
    }).collect(Collectors.toList());

    // order of this matches default constructor find order mapping

    Field[] fields = type.getDeclaredFields();

    List<Class<?>> parameterTypes = new ArrayList<>(fields.length);

    Map<String, Integer> offsets = new HashMap<>();

    // need to know offset for mapping
    for (Field field : fields) {
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      offsets.put(field.getName(), parameterTypes.size());
      parameterTypes.add(field.getType());
    }

    try {
      this.constructor = type.getDeclaredConstructor(parameterTypes.toArray(new Class[0]));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }

    this.reader = schema.getFields().stream().map(field -> {
      int offset = offsets.get(field.name());

      try {
        Field classField = type.getDeclaredField(field.name());
        AvroEncode enc = classField.getAnnotation(AvroEncode.class);
        if (enc != null)
          return new CustomEncodedFieldReader(offset, enc.using().getDeclaredConstructor().newInstance());
        return new ReflectFieldReader(offset, field.schema());
      } catch (ReflectiveOperationException e) {
        throw new AvroRuntimeException("Could not instantiate custom Encoding");
      }
    }).collect(Collectors.toList());
  }

  @Override
  public CustomEncoding<Object> setSchema(Schema schema) {
    return new ReflectRecordEncoding(type, schema);
  }

  @Override
  protected void write(Object datum, Encoder out) throws IOException {
    throw new UnsupportedOperationException("No writer specified");
  }

  @Override
  protected void write(Object datum, Encoder out, ReflectDatumWriter writer) throws IOException {
    for (FieldWriter field : this.writer) {
      field.write(datum, out, writer);
    }
  }

  @Override
  protected Object read(Object reuse, Decoder in) throws IOException {
    throw new UnsupportedOperationException("No reader specified");
  }

  @Override
  protected Object read(Object reuse, ResolvingDecoder in, ReflectDatumReader reader) throws IOException {

    Object[] args = new Object[this.reader.size()];

    for (FieldReader field : this.reader) {
      field.read(in, reader, args);
    }

    try {
      return this.constructor.newInstance(args);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException();
    }
  }

  private interface FieldWriter {
    void write(Object datum, Encoder out, ReflectDatumWriter writer) throws IOException;
  }

  private static class ReflectFieldWriter implements FieldWriter {

    private final Field field;
    private final Schema schema;

    public ReflectFieldWriter(Field field, Schema schema) {
      this.field = field;
      this.schema = schema;
    }

    @Override
    public void write(Object datum, Encoder out, ReflectDatumWriter writer) throws IOException {
      try {
        Object obj = field.get(datum);
        writer.write(schema, obj, out);
      } catch (ReflectiveOperationException e) {
        throw new AvroRuntimeException("Could not invoke", e);
      }
    }
  }

  private static class CustomEncodedFieldWriter implements FieldWriter {

    private final Field field;
    private final CustomEncoding<?> encoding;

    public CustomEncodedFieldWriter(Field field, CustomEncoding<?> encoding) {
      this.field = field;
      this.encoding = encoding;
    }

    @Override
    public void write(Object datum, Encoder out, ReflectDatumWriter writer) throws IOException {
      try {
        Object obj = field.get(datum);
        encoding.write(obj, out);
      } catch (ReflectiveOperationException e) {
        throw new AvroRuntimeException("Could not invoke", e);
      }
    }
  }

  private interface FieldReader {
    public void read(ResolvingDecoder in, ReflectDatumReader reader, Object[] constructorArgs) throws IOException;
  }

  private static class ReflectFieldReader implements FieldReader {

    private final int constructorOffset;
    private final Schema schema;

    public ReflectFieldReader(int constructorOffset, Schema schema) {
      this.constructorOffset = constructorOffset;
      this.schema = schema;
    }

    @Override
    public void read(ResolvingDecoder in, ReflectDatumReader reader, Object[] constructorArgs) throws IOException {
      Object obj = reader.read(null, schema, in);
      constructorArgs[constructorOffset] = obj;
    }
  }

  private static class CustomEncodedFieldReader implements FieldReader {

    private final int constructorOffset;
    private final CustomEncoding<?> encoding;

    public CustomEncodedFieldReader(int constructorOffset, CustomEncoding<?> encoding) {
      this.constructorOffset = constructorOffset;
      this.encoding = encoding;
    }

    @Override
    public void read(ResolvingDecoder in, ReflectDatumReader reader, Object[] constructorArgs) throws IOException {
      Object obj = encoding.read(in);
      constructorArgs[constructorOffset] = obj;
    }
  }
}
