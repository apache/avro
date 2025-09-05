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

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class TestFastReaderBuilderSecurity {

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
   * Test that FastReaderBuilder rejects untrusted classes in java-class property
   * for strings
   */
  @Test
  void testRejectUntrustedClassInStringSchema() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Create a string schema with java-class property pointing to an untrusted
    // class
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    stringSchema.addProp(SpecificData.CLASS_PROP, "java.lang.StringBuilder");

    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Should throw SecurityException when trying to create reader with untrusted
    // class
    SecurityException exception = assertThrows(SecurityException.class, () -> {
      builder.createDatumReader(stringSchema);
    });

    assertTrue(exception.getMessage().contains("Forbidden java.lang.StringBuilder"));
    assertTrue(exception.getMessage().contains("This class is not trusted"));
  }

  /**
   * Test that FastReaderBuilder allows trusted classes in java-class property for
   * strings
   */
  @Test
  void testAllowTrustedClassInStringSchema() throws IOException {
    // Set system property to trust java.lang.StringBuilder
    System.setProperty("org.apache.avro.SERIALIZABLE_CLASSES", "java.lang.StringBuilder");

    // Create a string schema with java-class property pointing to a trusted class
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    stringSchema.addProp(SpecificData.CLASS_PROP, "java.lang.StringBuilder");

    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Should not throw exception with trusted class
    assertDoesNotThrow(() -> {
      DatumReader<Object> reader = builder.createDatumReader(stringSchema);
      assertNotNull(reader);
    });
  }

  /**
   * Test that FastReaderBuilder rejects untrusted classes in java-key-class
   * property for maps
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

    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Should throw SecurityException when trying to create reader with untrusted
    // key class
    SecurityException exception = assertThrows(SecurityException.class, () -> {
      builder.createDatumReader(mapSchema);
    });

    assertTrue(exception.getMessage().contains("Forbidden java.lang.Integer"));
  }

  /**
   * Test that FastReaderBuilder allows trusted classes in java-key-class property
   * for maps
   */
  @Test
  void testAllowTrustedClassInMapKeySchema() throws IOException {
    // Set system property to trust java.lang.Integer
    System.setProperty("org.apache.avro.SERIALIZABLE_CLASSES", "java.lang.Integer");

    // Create a map schema with java-key-class property pointing to a trusted class
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
    mapSchema.addProp(SpecificData.KEY_CLASS_PROP, "java.lang.Integer");

    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Should not throw exception with trusted key class
    assertDoesNotThrow(() -> {
      DatumReader<Object> reader = builder.createDatumReader(mapSchema);
      assertNotNull(reader);
    });
  }

  /**
   * Test that FastReaderBuilder allows access with trusted packages
   */
  @Test
  void testAllowTrustedPackages() throws IOException {
    // Set system property to trust java.lang package
    System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES", "java.lang");

    // Create a string schema with java-class property pointing to a class in
    // trusted package
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    stringSchema.addProp(SpecificData.CLASS_PROP, "java.lang.StringBuilder");

    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Should not throw exception with class from trusted package
    assertDoesNotThrow(() -> {
      DatumReader<Object> reader = builder.createDatumReader(stringSchema);
      assertNotNull(reader);
    });
  }

  /**
   * Test that wildcard package trust allows any class
   */
  @Test
  void testWildcardPackageTrustAllowsAnyClass() throws IOException {
    // Set system property to trust all packages with wildcard
    System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES", "*");

    // Create a string schema with java-class property pointing to any class
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    stringSchema.addProp(SpecificData.CLASS_PROP, "com.example.SomeRandomClass");

    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Should not throw SecurityException with wildcard trust (may have other issues
    // like ClassNotFoundException)
    assertDoesNotThrow(() -> {
      DatumReader<Object> reader = builder.createDatumReader(stringSchema);
      assertNotNull(reader);
    });
  }

  /**
   * Test that FastReaderBuilder allows primitive types regardless of trust
   * settings
   */
  @Test
  void testPrimitivesAlwaysAllowed() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Primitive types should always be allowed
    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Test with schemas that don't require class loading
    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema booleanSchema = Schema.create(Schema.Type.BOOLEAN);

    assertDoesNotThrow(() -> {
      DatumReader<Object> intReader = builder.createDatumReader(intSchema);
      assertNotNull(intReader);

      DatumReader<Object> stringReader = builder.createDatumReader(stringSchema);
      assertNotNull(stringReader);

      DatumReader<Object> booleanReader = builder.createDatumReader(booleanSchema);
      assertNotNull(booleanReader);
    });
  }

  /**
   * Test that FastReaderBuilder with GenericData applies security restrictions
   */
  @Test
  void testGenericDataAppliesSecurity() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Create a string schema with java-class property pointing to an untrusted
    // class
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    stringSchema.addProp(SpecificData.CLASS_PROP, "java.lang.StringBuilder");

    // Use generic FastReaderBuilder (should not apply security restrictions)
    FastReaderBuilder builder = FastReaderBuilder.get();

    // Should not throw SecurityException with GenericData
    assertThrows(SecurityException.class, () -> {
      builder.createDatumReader(stringSchema);
    });
  }

  /**
   * Test that FastReaderBuilder with SpecificData applies security restrictions
   */
  @Test
  void testSpecificDataAppliesSecurity() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Create a string schema with java-class property pointing to an untrusted
    // class
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    stringSchema.addProp(SpecificData.CLASS_PROP, "java.lang.StringBuilder");

    // Use specific FastReaderBuilder (should apply security restrictions)
    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Should throw SecurityException with SpecificData
    assertThrows(SecurityException.class, () -> {
      builder.createDatumReader(stringSchema);
    });
  }

  /**
   * Test that FastReaderBuilder respects class property enable/disable settings
   */
  @Test
  void testClassPropEnabledDisablesSecurity() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Create a string schema with java-class property pointing to an untrusted
    // class
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    stringSchema.addProp(SpecificData.CLASS_PROP, "java.lang.StringBuilder");

    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Disable class property processing
    builder.withClassPropEnabled(false);

    // Should not throw SecurityException when class property processing is disabled
    assertDoesNotThrow(() -> {
      DatumReader<Object> reader = builder.createDatumReader(stringSchema);
      assertNotNull(reader);
    });
  }

  /**
   * Test that FastReaderBuilder respects key class property enable/disable
   * settings
   */
  @Test
  void testKeyClassEnabledDisablesSecurity() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Create a map schema with java-key-class property pointing to an untrusted
    // class
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
    mapSchema.addProp(SpecificData.KEY_CLASS_PROP, "java.lang.Integer");

    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Disable key class property processing
    builder.withKeyClassEnabled(false);

    // Should not throw SecurityException when key class property processing is
    // disabled
    assertDoesNotThrow(() -> {
      DatumReader<Object> reader = builder.createDatumReader(mapSchema);
      assertNotNull(reader);
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
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));

    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Test schemas without class properties
    assertDoesNotThrow(() -> {
      DatumReader<Object> stringReader = builder.createDatumReader(stringSchema);
      assertNotNull(stringReader);

      DatumReader<Object> mapReader = builder.createDatumReader(mapSchema);
      assertNotNull(mapReader);

      DatumReader<Object> arrayReader = builder.createDatumReader(arraySchema);
      assertNotNull(arrayReader);
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

    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Test each trusted class
    String[] trustedClasses = { "java.lang.String", "java.lang.Integer", "java.math.BigDecimal" };

    for (String className : trustedClasses) {
      Schema stringSchema = Schema.create(Schema.Type.STRING);
      stringSchema.addProp(SpecificData.CLASS_PROP, className);

      final String currentClass = className;
      assertDoesNotThrow(() -> {
        DatumReader<Object> reader = builder.createDatumReader(stringSchema);
        assertNotNull(reader);
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

    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Test classes from each trusted package
    String[] trustedClasses = { "java.lang.String", "java.util.List", "java.math.BigDecimal" };

    for (String className : trustedClasses) {
      Schema stringSchema = Schema.create(Schema.Type.STRING);
      stringSchema.addProp(SpecificData.CLASS_PROP, className);

      final String currentClass = className;
      assertDoesNotThrow(() -> {
        DatumReader<Object> reader = builder.createDatumReader(stringSchema);
        assertNotNull(reader);
      }, "Should not throw SecurityException for class from trusted package: " + className);
    }
  }

  /**
   * Test that complex nested schemas with class properties are validated
   */
  @Test
  void testNestedSchemaSecurityValidation() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Create a complex schema with nested maps and arrays
    Schema innerMapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
    innerMapSchema.addProp(SpecificData.KEY_CLASS_PROP, "java.lang.Integer");

    Schema arrayOfMapsSchema = Schema.createArray(innerMapSchema);

    Schema outerMapSchema = Schema.createMap(arrayOfMapsSchema);

    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Should throw SecurityException for untrusted key class in nested structure
    assertThrows(SecurityException.class, () -> {
      builder.createDatumReader(outerMapSchema);
    });
  }

  /**
   * Test writer/reader schema compatibility with security validation
   */
  @Test
  void testWriterReaderSchemaSecurityValidation() throws IOException {
    // Set trusted class for writer schema
    System.setProperty("org.apache.avro.SERIALIZABLE_CLASSES", "java.lang.String");

    // Create writer and reader schemas with different class properties
    Schema writerSchema = Schema.create(Schema.Type.STRING);
    writerSchema.addProp(SpecificData.CLASS_PROP, "java.lang.String");

    Schema readerSchema = Schema.create(Schema.Type.STRING);
    readerSchema.addProp(SpecificData.CLASS_PROP, "java.lang.StringBuilder"); // untrusted

    FastReaderBuilder builder = FastReaderBuilder.getSpecific();

    // Should throw SecurityException for untrusted class in reader schema
    assertThrows(SecurityException.class, () -> {
      builder.createDatumReader(writerSchema, readerSchema);
    });
  }

  /**
   * Test that builder state is isolated between instances
   */
  @Test
  void testBuilderStateIsolation() throws IOException {
    // Clear system properties
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    Schema schemaWithClass = Schema.create(Schema.Type.STRING);
    schemaWithClass.addProp(SpecificData.CLASS_PROP, "java.lang.StringBuilder");

    // Create two builders with different settings
    FastReaderBuilder builder1 = FastReaderBuilder.getSpecific().withClassPropEnabled(true);
    FastReaderBuilder builder2 = FastReaderBuilder.getSpecific().withClassPropEnabled(false);

    // First builder should throw SecurityException
    assertThrows(SecurityException.class, () -> {
      builder1.createDatumReader(schemaWithClass);
    });

    // Second builder should not throw SecurityException
    assertDoesNotThrow(() -> {
      DatumReader<Object> reader = builder2.createDatumReader(schemaWithClass);
      assertNotNull(reader);
    });
  }
}
