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

import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

public class TestSchemaValidateDefault {

  @Test
  public void valueReadWithCorrectDefaultValue() throws IOException {

    ExampleRecord writtenValue = new ExampleRecord(new ComplexValue(42L), new ComplexValue(666L));
    byte[] bytes = getSerializer(ExampleRecord.SCHEMA_WITH_ONE_FIELD).apply(writtenValue);

    ReflectDatumReader<ExampleRecord> reader = new ReflectDatumReader<>(ExampleRecord.SCHEMA_WITH_ONE_FIELD,
        ExampleRecord.SCHEMA_WITH_TWO_FIELDS, ReflectData.get());
    Decoder decoder = DecoderFactory.get().jsonDecoder(ExampleRecord.SCHEMA_WITH_ONE_FIELD,
        new ByteArrayInputStream(bytes));
    ExampleRecord deserializedValue = reader.read(null, decoder);

    Assertions.assertNotNull(deserializedValue.getValue2(), "Null get value2");
    Assertions.assertEquals(15L, deserializedValue.getValue2().getValue());
  }

  public static Function<Object, byte[]> getSerializer(Schema writerSchema) {
    Objects.requireNonNull(writerSchema, "writerSchema must not be null");

    ReflectDatumWriter<Object> writer = new ReflectDatumWriter<>(writerSchema, new ReflectData());
    return object -> {
      try {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().jsonEncoder(writerSchema, stream);
        writer.write(object, encoder);
        encoder.flush();
        return stream.toByteArray();
      } catch (IOException e) {
        throw new IllegalStateException(String.format("Avro failed to encode %s to schema %s", object, writerSchema),
            e);
      }
    };
  }

  public static <T> Function<byte[], T> getDeserializer(Class<T> readClass, Schema readerSchema, Schema writerSchema) {
    Objects.requireNonNull(readClass, "readClass must not be null");
    Objects.requireNonNull(readerSchema, "readerSchema must not be null");
    Objects.requireNonNull(writerSchema, "writerSchema must not be null");

    ReflectDatumReader<T> reader = new ReflectDatumReader<>(writerSchema, readerSchema, new ReflectData());
    return (byte[] bytes) -> {
      try {
        Decoder decoder = DecoderFactory.get().jsonDecoder(writerSchema, new ByteArrayInputStream(bytes));
        T readValue = reader.read(null, decoder);
        return readValue;
      } catch (IOException e) {
        throw new IllegalStateException(String.format("Avro failed to decode %s to %s", new String(bytes), readClass),
            e);
      }
    };
  }

  static final Schema SCHEMA = SchemaBuilder.record("org.apache.avro.TestSchemaValidateDefault.ComplexValue").fields()
      .optionalLong("value").endRecord();

  public static class ComplexValue {

    private Long value;

    public ComplexValue() {
    }

    public ComplexValue(Long value) {
      this.value = value;
    }

    public Long getValue() {
      return this.value;
    }

    @Override
    public String toString() {
      return "{" + "\"value\": { \"long\": " + this.value + "}}";
    }
  }

  public static class ExampleRecord {
    public static final Schema SCHEMA_WITH_ONE_FIELD;
    public static final Schema SCHEMA_WITH_TWO_FIELDS;

    static {
      SCHEMA_WITH_ONE_FIELD = SchemaBuilder.record("org.apache.avro.TestSchemaValidateDefault.ExampleRecord").fields()
          .name("value1").type(TestSchemaValidateDefault.SCHEMA).noDefault().endRecord();

      GenericData.Record record = new GenericData.Record(TestSchemaValidateDefault.SCHEMA);
      record.put("value", 15L);

      SCHEMA_WITH_TWO_FIELDS = SchemaBuilder.record("org.apache.avro.TestSchemaValidateDefault.ExampleRecord").fields()
          .name("value1").type(TestSchemaValidateDefault.SCHEMA).noDefault().name("value2")
          .type(TestSchemaValidateDefault.SCHEMA).withDefault(record).endRecord();
    }

    private ComplexValue value1;
    private ComplexValue value2;

    public ExampleRecord() {
    }

    public ExampleRecord(ComplexValue value1, ComplexValue value2) {
      this.value1 = value1;
      this.value2 = value2;
    }

    public ComplexValue getValue1() {
      return this.value1;
    }

    public ComplexValue getValue2() {
      return this.value2;
    }
  }

}
