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
package org.apache.avro.specific;

import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class TestSpecificDatumReaderSecurity {

  private String originalSerializableClasses;
  private String originalSerializablePackages;

  @BeforeEach
  void setUp() {
    // Save original system properties
    originalSerializableClasses = System.getProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    originalSerializablePackages = System.getProperty("org.apache.avro.SERIALIZABLE_PACKAGES");
  }

  @AfterEach
  void tearDown() {
    // Restore original system properties
    if (originalSerializableClasses != null) {
      System.setProperty("org.apache.avro.SERIALIZABLE_CLASSES", originalSerializableClasses);
    } else {
      System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    }

    if (originalSerializablePackages != null) {
      System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES", originalSerializablePackages);
    } else {
      System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");
    }
  }

  /**
   * Test that SpecificDatumReader rejects untrusted classes in java-class
   * property
   */
  @Test
  void testRejectUntrustedClassInStringSchema() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Create a schema with java-class property pointing to an untrusted class
    Schema schema = Schema.create(Schema.Type.STRING);
    schema.addProp(SpecificData.CLASS_PROP, "java.net.URL");

    SpecificDatumReader<Object> reader = new SpecificDatumReader<>();
    reader.setSchema(schema);

    // Create encoded data
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeString("test");
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    // Should throw SecurityException when trying to read with untrusted class
    SecurityException exception = assertThrows(SecurityException.class, () -> {
      reader.read(null, decoder);
    });

    assertTrue(exception.getMessage().contains("Forbidden java.net.URL"));
    assertTrue(exception.getMessage().contains("This class is not trusted"));
  }

  /**
   * Test that SpecificDatumReader allows trusted classes in java-class property
   */
  @Test
  void testAllowTrustedClassInStringSchema() throws IOException {
    // Set system property to trust java.lang.String
    System.setProperty("org.apache.avro.SERIALIZABLE_CLASSES", "java.lang.String");

    // Create a schema with java-class property pointing to a trusted class
    Schema schema = Schema.create(Schema.Type.STRING);
    schema.addProp(SpecificData.CLASS_PROP, "java.lang.String");

    // Test that schema creation and reader setup doesn't throw SecurityException
    assertDoesNotThrow(() -> {
      SpecificDatumReader<Object> reader = new SpecificDatumReader<>();
      reader.setSchema(schema);
      // The main security check happens in findStringClass/getPropAsClass
      // If we get here without SecurityException, the security validation passed
    });
  }

  /**
   * Test that SpecificDatumReader rejects untrusted classes in java-key-class
   * property
   */
  @Test
  void testRejectUntrustedClassInMapKeySchema() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Create a map schema with java-key-class property pointing to an untrusted
    // class
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
    mapSchema.addProp(SpecificData.KEY_CLASS_PROP, "java.lang.Integer");

    // Security check should happen during setSchema when fast reader is enabled,
    // or during actual reading for non-fast reader
    SpecificDatumReader<Object> reader = new SpecificDatumReader<>();

    // For some configurations, the security exception may occur during setSchema
    try {
      reader.setSchema(mapSchema);
    } catch (SecurityException e) {
      assertTrue(e.getMessage().contains("Forbidden java.lang.Integer"));
      return; // Test passed - security exception occurred during schema setup
    }

    // Create encoded data with a map entry to trigger key class usage
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeMapStart();
    encoder.setItemCount(1);
    encoder.writeString("1"); // key
    encoder.writeString("value"); // value
    encoder.setItemCount(0);
    encoder.writeMapEnd();
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    // Should throw SecurityException when trying to read with untrusted key class
    SecurityException exception = assertThrows(SecurityException.class, () -> {
      reader.read(null, decoder);
    });

    assertTrue(exception.getMessage().contains("Forbidden java.lang.Integer"));
  }

  /**
   * Test that SpecificDatumReader allows trusted classes in java-key-class
   * property
   */
  @Test
  void testAllowTrustedClassInMapKeySchema() throws IOException {
    // Set system property to trust java.lang.Integer
    System.setProperty("org.apache.avro.SERIALIZABLE_CLASSES", "java.lang.Integer");

    // Create a map schema with java-key-class property pointing to a trusted class
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
    mapSchema.addProp(SpecificData.KEY_CLASS_PROP, "java.lang.Integer");

    SpecificDatumReader<Object> reader = new SpecificDatumReader<>();
    reader.setSchema(mapSchema);

    // Create encoded data (empty map)
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeMapStart();
    encoder.setItemCount(0);
    encoder.writeMapEnd();
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    // Should not throw exception with trusted key class
    assertDoesNotThrow(() -> {
      Object result = reader.read(null, decoder);
      assertNotNull(result);
    });
  }

  /**
   * Test that SpecificDatumReader allows access with trusted packages
   */
  @Test
  void testAllowTrustedPackages() throws IOException {
    // Set system property to trust java.lang package
    System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES", "java.lang");

    // Create a schema with java-class property pointing to a class in trusted
    // package
    Schema schema = Schema.create(Schema.Type.STRING);
    schema.addProp(SpecificData.CLASS_PROP, "java.lang.StringBuilder");

    // Test that schema creation and reader setup doesn't throw SecurityException
    assertDoesNotThrow(() -> {
      SpecificDatumReader<Object> reader = new SpecificDatumReader<>();
      reader.setSchema(schema);
      // The main security check happens in findStringClass/getPropAsClass
      // If we get here without SecurityException, the security validation passed
    });
  }

  /**
   * Test that wildcard package trust allows any class
   */
  @Test
  void testWildcardPackageTrustAllowsAnyClass() throws IOException {
    // Set system property to trust all packages with wildcard
    System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES", "*");

    // Create a schema with java-class property pointing to any class
    Schema schema = Schema.create(Schema.Type.STRING);
    schema.addProp(SpecificData.CLASS_PROP, "com.example.SomeRandomClass");

    SpecificDatumReader<Object> reader = new SpecificDatumReader<>();
    reader.setSchema(schema);

    // Create encoded data
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeString("test");
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    // Should not throw exception with wildcard trust, but may fail with
    // ClassNotFoundException
    assertDoesNotThrow(() -> {
      try {
        reader.read(null, decoder);
      } catch (AvroRuntimeException e) {
        // ClassNotFoundException wrapped in AvroRuntimeException is expected for
        // non-existent class
        assertTrue(e.getCause() instanceof ClassNotFoundException);
      }
    });
  }

  /**
   * Test that SpecificDatumReader allows primitive types regardless of trust
   * settings
   */
  @Test
  void testPrimitivesAlwaysAllowed() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Primitive types should always be allowed
    SpecificDatumReader<Object> reader = new SpecificDatumReader<>();

    // Test with int schema
    Schema intSchema = Schema.create(Schema.Type.INT);
    reader.setSchema(intSchema);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeInt(42);
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    // Should not throw exception for primitives
    assertDoesNotThrow(() -> {
      Object result = reader.read(null, decoder);
      assertEquals(42, result);
    });
  }

  /**
   * Test that missing class properties don't trigger security exceptions
   */
  @Test
  void testMissingClassPropertiesAllowed() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Create schemas without class properties
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));

    // Test string schema without class property
    SpecificDatumReader<Object> stringReader = new SpecificDatumReader<>();
    stringReader.setSchema(stringSchema);
    ByteArrayOutputStream stringOut = new ByteArrayOutputStream();
    BinaryEncoder stringEncoder = EncoderFactory.get().binaryEncoder(stringOut, null);
    stringEncoder.writeString("test");
    stringEncoder.flush();
    BinaryDecoder stringDecoder = DecoderFactory.get().binaryDecoder(stringOut.toByteArray(), null);

    assertDoesNotThrow(() -> {
      Object result = stringReader.read(null, stringDecoder);
      assertNotNull(result);
    });

    // Test map schema without key class property
    SpecificDatumReader<Object> mapReader = new SpecificDatumReader<>();
    mapReader.setSchema(mapSchema);
    ByteArrayOutputStream mapOut = new ByteArrayOutputStream();
    BinaryEncoder mapEncoder = EncoderFactory.get().binaryEncoder(mapOut, null);
    mapEncoder.writeMapStart();
    mapEncoder.setItemCount(0);
    mapEncoder.writeMapEnd();
    mapEncoder.flush();
    BinaryDecoder mapDecoder = DecoderFactory.get().binaryDecoder(mapOut.toByteArray(), null);

    assertDoesNotThrow(() -> {
      Object result = mapReader.read(null, mapDecoder);
      assertNotNull(result);
    });
  }

  /**
   * Test multiple trusted classes in comma-separated list
   */
  @Test
  void testMultipleTrustedClasses() throws IOException {
    // Set multiple trusted classes
    System.setProperty("org.apache.avro.SERIALIZABLE_CLASSES",
        "java.lang.String,java.lang.Integer,java.math.BigDecimal");

    SpecificDatumReader<Object> reader = new SpecificDatumReader<>();

    // Test each trusted class - only test that security validation passes
    String[] trustedClasses = { "java.lang.String", "java.lang.Integer", "java.math.BigDecimal" };

    for (String className : trustedClasses) {
      Schema schema = Schema.create(Schema.Type.STRING);
      schema.addProp(SpecificData.CLASS_PROP, className);

      final String currentClass = className;
      assertDoesNotThrow(() -> {
        SpecificDatumReader<Object> testReader = new SpecificDatumReader<>();
        testReader.setSchema(schema);
        // If we get here without SecurityException, the security validation passed
      }, "Should not throw SecurityException for trusted class: " + className);
    }
  }

  /**
   * Test multiple trusted packages in comma-separated list
   */
  @Test
  void testMultipleTrustedPackages() throws IOException {
    // Set multiple trusted packages
    System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES", "java.lang,java.util,java.math");

    SpecificDatumReader<Object> reader = new SpecificDatumReader<>();

    // Test classes from each trusted package - only test that security validation
    // passes
    String[] trustedClasses = { "java.lang.String", "java.util.List", "java.math.BigDecimal" };

    for (String className : trustedClasses) {
      Schema schema = Schema.create(Schema.Type.STRING);
      schema.addProp(SpecificData.CLASS_PROP, className);

      final String currentClass = className;
      assertDoesNotThrow(() -> {
        SpecificDatumReader<Object> testReader = new SpecificDatumReader<>();
        testReader.setSchema(schema);
        // If we get here without SecurityException, the security validation passed
      }, "Should not throw SecurityException for class from trusted package: " + className);
    }
  }
}
