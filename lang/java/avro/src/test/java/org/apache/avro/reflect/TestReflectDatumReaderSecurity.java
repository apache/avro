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

import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestReflectDatumReaderSecurity {

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
   * Test that ReflectDatumReader rejects untrusted classes in java-class property
   * for arrays
   */
  @Test
  void testRejectUntrustedClassInArraySchema() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Create an array schema with java-class property pointing to an untrusted
    // class (use java.util.Vector which is not in DEFAULT_SAFE_CLASSES)
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    arraySchema.addProp(SpecificData.CLASS_PROP, "java.util.Vector");

    ReflectDatumReader<Object> reader = new ReflectDatumReader<>();
    reader.setSchema(arraySchema);

    // Create encoded data (empty array)
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeArrayStart();
    encoder.setItemCount(0);
    encoder.writeArrayEnd();
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    // Should throw SecurityException when trying to read with untrusted class
    SecurityException exception = assertThrows(SecurityException.class, () -> {
      reader.read(null, decoder);
    });

    assertTrue(exception.getMessage().contains("Forbidden java.util.Vector"));
    assertTrue(exception.getMessage().contains("This class is not trusted"));
  }

  /**
   * Test that ReflectDatumReader allows trusted classes in java-class property
   * for arrays
   */
  @Test
  void testAllowTrustedClassInArraySchema() throws IOException {
    // Set system property to trust java.util.Vector
    System.setProperty("org.apache.avro.SERIALIZABLE_CLASSES", "java.util.Vector");

    // Create an array schema with java-class property pointing to a trusted class
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    arraySchema.addProp(SpecificData.CLASS_PROP, "java.util.Vector");

    ReflectDatumReader<Object> reader = new ReflectDatumReader<>();
    reader.setSchema(arraySchema);

    // Create encoded data (empty array)
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeArrayStart();
    encoder.setItemCount(0);
    encoder.writeArrayEnd();
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    // Should not throw exception with trusted class
    assertDoesNotThrow(() -> {
      Object result = reader.read(null, decoder);
      assertNotNull(result);
      assertTrue(result instanceof List);
    });
  }

  /**
   * Test that ReflectDatumReader rejects untrusted classes in java-element-class
   * property
   */
  @Test
  void testRejectUntrustedElementClassInArraySchema() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Create an array schema with java-element-class property pointing to an
    // untrusted class
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    arraySchema.addProp(SpecificData.ELEMENT_PROP, "java.lang.StringBuilder");

    ReflectDatumReader<Object> reader = new ReflectDatumReader<>();
    reader.setSchema(arraySchema);

    // Create encoded data (empty array)
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeArrayStart();
    encoder.setItemCount(0);
    encoder.writeArrayEnd();
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    // Should throw SecurityException when trying to read with untrusted element
    // class
    SecurityException exception = assertThrows(SecurityException.class, () -> {
      reader.read(null, decoder);
    });

    assertTrue(exception.getMessage().contains("Forbidden java.lang.StringBuilder"));
  }

  /**
   * Test that ReflectDatumReader allows trusted classes in java-element-class
   * property
   */
  @Test
  void testAllowTrustedElementClassInArraySchema() throws IOException {
    // Set system property to trust java.lang.StringBuilder
    System.setProperty("org.apache.avro.SERIALIZABLE_CLASSES", "java.lang.StringBuilder");

    // Create an array schema with java-element-class property pointing to a trusted
    // class
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    arraySchema.addProp(SpecificData.ELEMENT_PROP, "java.lang.StringBuilder");

    ReflectDatumReader<Object> reader = new ReflectDatumReader<>();
    reader.setSchema(arraySchema);

    // Create encoded data (empty array)
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeArrayStart();
    encoder.setItemCount(0);
    encoder.writeArrayEnd();
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    // Should not throw exception with trusted element class
    assertDoesNotThrow(() -> {
      Object result = reader.read(null, decoder);
      assertNotNull(result);
    });
  }

  /**
   * Test that ReflectDatumReader rejects untrusted classes in java-class property
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

    ReflectDatumReader<Object> reader = new ReflectDatumReader<>();
    reader.setSchema(stringSchema);

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

    assertTrue(exception.getMessage().contains("Forbidden java.lang.StringBuilder"));
  }

  /**
   * Test that ReflectDatumReader allows trusted packages
   */
  @Test
  void testAllowTrustedPackages() throws IOException {
    // Set system property to trust java.util package
    System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES", "java.util");

    // Create an array schema with java-class property pointing to a class in
    // trusted package
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    arraySchema.addProp(SpecificData.CLASS_PROP, "java.util.ArrayList");

    ReflectDatumReader<Object> reader = new ReflectDatumReader<>();
    reader.setSchema(arraySchema);

    // Create encoded data (empty array)
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeArrayStart();
    encoder.setItemCount(0);
    encoder.writeArrayEnd();
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    // Should not throw exception with class from trusted package
    assertDoesNotThrow(() -> {
      Object result = reader.read(null, decoder);
      assertNotNull(result);
      assertTrue(result instanceof ArrayList);
    });
  }

  /**
   * Test that wildcard package trust allows any class
   */
  @Test
  void testWildcardPackageTrustAllowsAnyClass() throws IOException {
    // Set system property to trust all packages with wildcard
    System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES", "*");

    // Create an array schema with java-class property pointing to any class
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    arraySchema.addProp(SpecificData.CLASS_PROP, "java.util.LinkedList");

    ReflectDatumReader<Object> reader = new ReflectDatumReader<>();
    reader.setSchema(arraySchema);

    // Create encoded data (empty array)
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeArrayStart();
    encoder.setItemCount(0);
    encoder.writeArrayEnd();
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    // Should not throw exception with wildcard trust
    assertDoesNotThrow(() -> {
      Object result = reader.read(null, decoder);
      assertNotNull(result);
    });
  }

  /**
   * Test that ReflectDatumReader allows primitive types regardless of trust
   * settings
   */
  @Test
  void testPrimitivesAlwaysAllowed() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Primitive types should always be allowed
    ReflectDatumReader<Object> reader = new ReflectDatumReader<>();

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
   * Test security validation with bytes schema and array class property
   */
  @Test
  void testRejectUntrustedClassInBytesSchema() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Create a bytes schema with java-class property pointing to an untrusted array
    // class
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);
    bytesSchema.addProp(SpecificData.CLASS_PROP, "[java.net.URL"); // byte array class name

    ReflectDatumReader<Object> reader = new ReflectDatumReader<>();
    reader.setSchema(bytesSchema);

    // Create encoded data
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeBytes("test".getBytes());
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    // Should throw SecurityException when trying to read with untrusted array class
    SecurityException exception = assertThrows(SecurityException.class, () -> {
      reader.read(null, decoder);
    });

    assertTrue(exception.getMessage().contains("Forbidden [java.net.URL"));
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
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    Schema stringSchema = Schema.create(Schema.Type.STRING);

    // Test array schema without class property - create new reader
    ReflectDatumReader<Object> arrayReader = new ReflectDatumReader<>();
    arrayReader.setSchema(arraySchema);
    ByteArrayOutputStream out1 = new ByteArrayOutputStream();
    BinaryEncoder encoder1 = EncoderFactory.get().binaryEncoder(out1, null);
    encoder1.writeArrayStart();
    encoder1.setItemCount(0);
    encoder1.writeArrayEnd();
    encoder1.flush();
    BinaryDecoder decoder1 = DecoderFactory.get().binaryDecoder(out1.toByteArray(), null);

    assertDoesNotThrow(() -> {
      Object result = arrayReader.read(null, decoder1);
      assertNotNull(result);
    });

    // Test string schema without class property - create new reader
    ReflectDatumReader<Object> stringReader = new ReflectDatumReader<>();
    stringReader.setSchema(stringSchema);
    ByteArrayOutputStream out2 = new ByteArrayOutputStream();
    BinaryEncoder encoder2 = EncoderFactory.get().binaryEncoder(out2, null);
    encoder2.writeString("test");
    encoder2.flush();
    BinaryDecoder decoder2 = DecoderFactory.get().binaryDecoder(out2.toByteArray(), null);

    assertDoesNotThrow(() -> {
      Object result = stringReader.read(null, decoder2);
      assertNotNull(result);
    });
  }

  /**
   * Test security validation with custom ReflectData
   */
  @Test
  void testCustomReflectDataInheritsSecurity() throws IOException {
    // Clear system properties to ensure no classes are trusted
    System.clearProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    System.clearProperty("org.apache.avro.SERIALIZABLE_PACKAGES");

    // Use constructor with custom ReflectData
    ReflectData reflectData = new ReflectData();
    ReflectDatumReader<Object> reader = new ReflectDatumReader<>(reflectData);

    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    arraySchema.addProp(SpecificData.CLASS_PROP, "com.evil.LinkedList");
    reader.setSchema(arraySchema);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeArrayStart();
    encoder.setItemCount(0);
    encoder.writeArrayEnd();
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    // Should throw SecurityException for untrusted class even with custom
    // ReflectData
    assertThrows(SecurityException.class, () -> {
      reader.read(null, decoder);
    });
  }

  /**
   * Test that array element class validation works correctly
   */
  @Test
  void testArrayElementClassValidation() throws IOException {
    // Trust ArrayList and String
    System.setProperty("org.apache.avro.SERIALIZABLE_CLASSES", "java.util.ArrayList,java.lang.String");

    ReflectDatumReader<Object> reader = new ReflectDatumReader<>();

    // Test with trusted array class and trusted element class
    Schema arraySchemaWithTrustedElement = Schema.createArray(Schema.create(Schema.Type.STRING));
    arraySchemaWithTrustedElement.addProp(SpecificData.CLASS_PROP, "java.util.ArrayList");
    arraySchemaWithTrustedElement.addProp(SpecificData.ELEMENT_PROP, "java.lang.String");
    reader.setSchema(arraySchemaWithTrustedElement);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeArrayStart();
    encoder.setItemCount(0);
    encoder.writeArrayEnd();
    encoder.flush();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    assertDoesNotThrow(() -> {
      Object result = reader.read(null, decoder);
      assertTrue(result instanceof ArrayList);
    });

    // Test with untrusted array class - should fail on the array class, not element
    // class
    Schema arraySchemaWithUntrustedClass = Schema.createArray(Schema.create(Schema.Type.STRING));
    arraySchemaWithUntrustedClass.addProp(SpecificData.CLASS_PROP, "com.evil.ArrayList");
    arraySchemaWithUntrustedClass.addProp(SpecificData.ELEMENT_PROP, "java.lang.String");

    ReflectDatumReader<Object> reader2 = new ReflectDatumReader<>();
    reader2.setSchema(arraySchemaWithUntrustedClass);

    ByteArrayOutputStream out2 = new ByteArrayOutputStream();
    BinaryEncoder encoder2 = EncoderFactory.get().binaryEncoder(out2, null);
    encoder2.writeArrayStart();
    encoder2.setItemCount(0);
    encoder2.writeArrayEnd();
    encoder2.flush();

    BinaryDecoder decoder2 = DecoderFactory.get().binaryDecoder(out2.toByteArray(), null);

    assertThrows(SecurityException.class, () -> {
      reader2.read(null, decoder2);
    });
  }

  /**
   * Test multiple trusted classes work correctly
   */
  @Test
  void testMultipleTrustedClasses() throws IOException {
    // Set multiple trusted classes
    System.setProperty("org.apache.avro.SERIALIZABLE_CLASSES",
        "java.util.ArrayList,java.util.LinkedList,java.util.Vector");

    ReflectDatumReader<Object> reader = new ReflectDatumReader<>();

    String[] trustedClasses = { "java.util.ArrayList", "java.util.LinkedList", "java.util.Vector" };

    for (String className : trustedClasses) {
      Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
      arraySchema.addProp(SpecificData.CLASS_PROP, className);
      reader.setSchema(arraySchema);

      ByteArrayOutputStream currentOut = new ByteArrayOutputStream();
      BinaryEncoder currentEncoder = EncoderFactory.get().binaryEncoder(currentOut, null);
      currentEncoder.writeArrayStart();
      currentEncoder.setItemCount(0);
      currentEncoder.writeArrayEnd();
      currentEncoder.flush();

      BinaryDecoder currentDecoder = DecoderFactory.get().binaryDecoder(currentOut.toByteArray(), null);

      final String currentClass = className;
      assertDoesNotThrow(() -> {
        Object result = reader.read(null, currentDecoder);
        assertNotNull(result);
      }, "Should not throw SecurityException for trusted class: " + className);
    }
  }
}
