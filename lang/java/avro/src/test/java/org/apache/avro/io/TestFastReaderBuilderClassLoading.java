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

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.ClassSecurityValidator;
import org.apache.avro.util.ClassSecurityValidator.ClassSecurityPredicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that FastReaderBuilder.findClass() routes class loading through
 * ClassUtils.forName(), so that ClassSecurityValidator is applied consistently.
 */
public class TestFastReaderBuilderClassLoading {

  private static final String TEST_VALUE = "https://example.com";

  private ClassSecurityPredicate originalValidator;

  @BeforeEach
  public void saveValidator() {
    originalValidator = ClassSecurityValidator.getGlobal();
  }

  @AfterEach
  public void restoreValidator() {
    ClassSecurityValidator.setGlobal(originalValidator);
  }

  /**
   * When the validator blocks a class referenced by java-class, FastReaderBuilder
   * must NOT instantiate it. The value should be returned as a plain string.
   */
  @Test
  void blockedClassIsNotInstantiated() {
    // Block java.net.URI
    ClassSecurityValidator.setGlobal(ClassSecurityValidator.composite(ClassSecurityValidator.DEFAULT_TRUSTED_CLASSES,
        ClassSecurityValidator.builder().add("org.apache.avro.util.Utf8").build()));

    GenericRecord result = readWithJavaClass("java.net.URI");

    assertNotNull(result.get("value"));
    assertFalse(result.get("value") instanceof URI, "Blocked class should not be instantiated");
  }

  /**
   * When the validator trusts a class referenced by java-class, FastReaderBuilder
   * should instantiate it normally.
   */
  @Test
  void trustedClassIsInstantiated() {
    ClassSecurityValidator.setGlobal(ClassSecurityValidator.composite(ClassSecurityValidator.DEFAULT_TRUSTED_CLASSES,
        ClassSecurityValidator.builder().add("java.net.URI").add("org.apache.avro.util.Utf8").build()));

    GenericRecord result = readWithJavaClass("java.net.URI");

    assertInstanceOf(URI.class, result.get("value"));
    assertEquals(URI.create(TEST_VALUE), result.get("value"));
  }

  /**
   * Encode a string, then read it back through FastReaderBuilder with the given
   * java-class.
   */
  private GenericRecord readWithJavaClass(String javaClass) {
    try {
      Schema stringSchema = Schema.create(Schema.Type.STRING);
      stringSchema.addProp(SpecificData.CLASS_PROP, javaClass);
      stringSchema.addProp(GenericData.STRING_PROP, GenericData.StringType.String.name());

      Schema recordSchema = Schema.createRecord("TestRecord", null, "test", false);
      recordSchema.setFields(Collections.singletonList(new Schema.Field("value", stringSchema, null, null)));

      // Encode
      GenericRecord record = new GenericRecordBuilder(recordSchema).set("value", TEST_VALUE).build();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      new GenericDatumWriter<GenericRecord>(recordSchema).write(record, encoder);
      encoder.flush();

      // Decode with fast reader enabled
      GenericData data = new GenericData();
      data.setFastReaderEnabled(true);
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(recordSchema, recordSchema, data);
      return reader.read(null, DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));
    } catch (IOException e) {
      return fail("Unexpected IOException during encode/decode", e);
    }
  }
}
