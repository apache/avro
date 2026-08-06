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
package org.apache.avro.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

/**
 * Tests for FastReaderBuilder behavior with schemas containing
 * {@link SpecificData#CLASS_PROP} and {@link SpecificData#KEY_CLASS_PROP}
 * attributes. Note that {@link SpecificData#ELEMENT_PROP} isn't tested because
 * it is only used by ReflectData.
 */
public class FastReaderBuilderJavaClassTest {

  private static final Schema SCHEMA_RECORD_WITH_NULLABLE_CLASS_PROP = SchemaBuilder.record("NullableStringRecord")
      .fields().requiredString("id").name("price").type().unionOf().nullType().and()
      .type(SchemaBuilder.builder().stringBuilder().prop(SpecificData.CLASS_PROP, "java.math.BigDecimal").endString())
      .endUnion().noDefault().endRecord();

  private static final GenericRecord RECORD_WITH_NULLABLE_CLASS_PROP = new GenericRecordBuilder(
      SCHEMA_RECORD_WITH_NULLABLE_CLASS_PROP).set("id", "123").set("price", "-0.0002").build();

  private static final Schema SCHEMA_RECORD_WITH_CLASS_PROP = SchemaBuilder.record("StringRecord").fields()
      .requiredString("id").name("price")
      .type(SchemaBuilder.builder().stringBuilder().prop(SpecificData.CLASS_PROP, "java.math.BigDecimal").endString())
      .noDefault().endRecord();

  private static final GenericRecord RECORD_WITH_CLASS_PROP = new GenericRecordBuilder(SCHEMA_RECORD_WITH_CLASS_PROP)
      .set("id", "123").set("price", "-0.0002").build();

  private static final Schema SCHEMA_RECORD_WITH_MAP_KEY_CLASS_PROP = SchemaBuilder.record("MapRecord").fields()
      .requiredString("id").name("prices").type().map().prop(SpecificData.KEY_CLASS_PROP, "java.math.BigDecimal")
      .values().stringType().noDefault().endRecord();

  private static final GenericRecord RECORD_WITH_MAP_KEY_CLASS_PROP = new GenericRecordBuilder(
      SCHEMA_RECORD_WITH_MAP_KEY_CLASS_PROP).set("id", "123")
          .set("prices", Map.of("-0.0002", "cheap", "12345.678", "expensive")).build();

  /**
   * Reusable round-trip logic for a record, using the given model.
   */
  public static GenericRecord roundTrip(GenericRecord record, GenericData model) throws IOException {
    byte[] serialized;

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      writer.write(record, encoder);
      encoder.flush();
      serialized = baos.toByteArray();
    }

    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(record.getSchema(), record.getSchema(), model);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(serialized, null);
    return reader.read(null, decoder);
  }

  /**
   * Tests that a plain GenericDatumReader (GenericData model) ignores the
   * {@link SpecificData#CLASS_PROP} attribute on a string field inside a union,
   * matching the classic (non fast-reader) behavior of GenericData.
   * <p>
   * This test also reproduces a bug (AVRO-4225) where
   * FastReaderBuilder.getTransformingStringReader() casts the result of
   * stringReader.read() directly to String, but GenericData returns Utf8, causing
   * a ClassCastException
   */
  @Test
  void genericDataModelIgnoresJavaClassPropWithStringUnion() throws IOException {
    // This round trip shouldn't cause a ClassCastException (AVRO-4225)
    GenericRecord result = roundTrip(RECORD_WITH_NULLABLE_CLASS_PROP, GenericData.get());

    assertNotNull(result);
    assertInstanceOf(Utf8.class, result.get("id"));
    assertEquals("123", result.get("id").toString());
    assertInstanceOf(Utf8.class, result.get("price"), "GenericData should ignore 'java-class'");
    assertEquals("-0.0002", result.get("price").toString());
  }

  /**
   * Tests that a plain GenericDatumReader (GenericData model) ignores the
   * {@link SpecificData#CLASS_PROP} attribute on a direct (non-union) string
   * field.
   */
  @Test
  void genericDataModelIgnoresJavaClassPropWithString() throws IOException {
    GenericRecord result = roundTrip(RECORD_WITH_CLASS_PROP, GenericData.get());

    assertNotNull(result);
    assertInstanceOf(Utf8.class, result.get("id"));
    assertEquals("123", result.get("id").toString());
    assertInstanceOf(Utf8.class, result.get("price"), "GenericData should ignore 'java-class'");
    assertEquals("-0.0002", result.get("price").toString());
  }

  /**
   * Tests that a GenericDatumReader built on the SpecificData model uses
   * {@link SpecificData#CLASS_PROP} on a string field inside a union,
   * transforming it into the named class without throwing ClassCastException.
   */
  @Test
  void specificDataModelUsesJavaClassProp() throws IOException {
    assumeTrue(SpecificData.get().isFastReaderEnabled(),
        "java-class conversion is only applied on the fast-reader path");
    GenericRecord result = roundTrip(RECORD_WITH_NULLABLE_CLASS_PROP, SpecificData.get());

    assertNotNull(result);
    assertInstanceOf(Utf8.class, result.get("id"));
    assertEquals("123", result.get("id").toString());
    assertInstanceOf(BigDecimal.class, result.get("price"), "SpecificData should use the class in 'java-class'");
    assertEquals(new BigDecimal("-0.0002"), result.get("price"));
  }

  /**
   * Tests that a GenericDatumReader built on the SpecificData model uses
   * {@link SpecificData#CLASS_PROP} on a direct (non-union) string field,
   * transforming it into the named class without throwing ClassCastException.
   */
  @Test
  void specificDataModelUsesJavaClassPropWithDirectString() throws IOException {
    assumeTrue(SpecificData.get().isFastReaderEnabled(),
        "java-class conversion is only applied on the fast-reader path");
    GenericRecord result = roundTrip(RECORD_WITH_CLASS_PROP, SpecificData.get());

    assertNotNull(result);
    assertInstanceOf(Utf8.class, result.get("id"));
    assertEquals("123", result.get("id").toString());
    assertInstanceOf(BigDecimal.class, result.get("price"), "SpecificData should use the class in 'java-class'");
    assertEquals(new BigDecimal("-0.0002"), result.get("price"));
  }

  /**
   * Tests that a plain GenericDatumReader (GenericData model) ignores the
   * {@link SpecificData#KEY_CLASS_PROP} property on a map schema, matching the
   * classic (non-fast-reader) behavior of GenericData, and leaves the map keys as
   * Utf8/String.
   */
  @Test
  void genericDataModelIgnoresJavaKeyClassPropWithMap() throws IOException {
    GenericRecord result = roundTrip(RECORD_WITH_MAP_KEY_CLASS_PROP, GenericData.get());

    assertNotNull(result);
    assertInstanceOf(Utf8.class, result.get("id"));
    assertEquals("123", result.get("id").toString());

    @SuppressWarnings("unchecked")
    Map<Object, Object> prices = (Map<Object, Object>) result.get("prices");
    assertEquals(2, prices.size());
    for (Object key : prices.keySet()) {
      assertInstanceOf(Utf8.class, key, "GenericData should ignore 'java-key-class'");
    }
    assertEquals("cheap", prices.get(new Utf8("-0.0002")).toString());
    assertEquals("expensive", prices.get(new Utf8("12345.678")).toString());
  }

  /**
   * Tests that a GenericDatumReader built on the SpecificData model uses
   * {@link SpecificData#KEY_CLASS_PROP} property on a map schema, transforming
   * the map keys into the named class.
   */
  @Test
  void specificDataModelUsesJavaKeyClassProp() throws IOException {
    assumeTrue(SpecificData.get().isFastReaderEnabled(),
        "java-key-class conversion is only applied on the fast-reader path");
    GenericRecord result = roundTrip(RECORD_WITH_MAP_KEY_CLASS_PROP, SpecificData.get());

    assertNotNull(result);
    assertInstanceOf(Utf8.class, result.get("id"));
    assertEquals("123", result.get("id").toString());

    @SuppressWarnings("unchecked")
    Map<Object, Object> prices = (Map<Object, Object>) result.get("prices");
    assertEquals(2, prices.size());
    for (Object key : prices.keySet()) {
      assertInstanceOf(BigDecimal.class, key, "SpecificData should use the class in 'java-key-class'");
    }
    assertEquals("cheap", prices.get(new BigDecimal("-0.0002")).toString());
    assertEquals("expensive", prices.get(new BigDecimal("12345.678")).toString());
  }
}
