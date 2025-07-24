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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestValidateClassLoading {

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

  @Test
  void testDefaultConstructorWithoutSystemProperties() {
    // Test the behavior with empty trusted lists
    ValidateClassLoading validator = new ValidateClassLoading(Arrays.asList(), Arrays.asList());

    assertTrue(validator.getTrustedClasses().isEmpty());
    assertTrue(validator.getTrustedPackages().isEmpty());
  }

  @Test
  void testDefaultConstructorWithSystemProperties() {
    // Test with specific trusted classes and packages
    List<String> trustedClasses = Arrays.asList("java.lang.String", "java.lang.Integer");
    List<String> trustedPackages = Arrays.asList("java.util.");

    ValidateClassLoading validator = new ValidateClassLoading(trustedPackages, trustedClasses);

    assertEquals(2, validator.getTrustedClasses().size());
    assertEquals(1, validator.getTrustedPackages().size());
    assertEquals(1, validator.getTrustedPackages().size());
  }

  @Test
  void testCustomConstructor() {
    ValidateClassLoading validator = new ValidateClassLoading(Arrays.asList("com.example", "org.apache"),
        Arrays.asList("com.example.MyClass", "org.apache.SomeClass"));

    assertEquals(2, validator.getTrustedPackages().size());
    assertTrue(validator.getTrustedPackages().contains("com.example"));
    assertTrue(validator.getTrustedPackages().contains("org.apache"));

    assertEquals(2, validator.getTrustedClasses().size());
    assertTrue(validator.getTrustedClasses().contains("com.example.MyClass"));
    assertTrue(validator.getTrustedClasses().contains("org.apache.SomeClass"));
  }

  @Test
  void testCheckSecurityAllowsPrimitives() {
    ValidateClassLoading validator = new ValidateClassLoading(Collections.emptyList(), Collections.emptyList());

    // These should not throw exceptions
    assertDoesNotThrow(() -> validator.checkSecurity("boolean"));
    assertDoesNotThrow(() -> validator.checkSecurity("char"));
    assertDoesNotThrow(() -> validator.checkSecurity("byte"));
    assertDoesNotThrow(() -> validator.checkSecurity("short"));
    assertDoesNotThrow(() -> validator.checkSecurity("int"));
    assertDoesNotThrow(() -> validator.checkSecurity("long"));
    assertDoesNotThrow(() -> validator.checkSecurity("float"));
    assertDoesNotThrow(() -> validator.checkSecurity("double"));
    assertDoesNotThrow(() -> validator.checkSecurity("void"));
  }

  @Test
  void testCheckSecurityAllowsTrustedClasses() {
    ValidateClassLoading validator = new ValidateClassLoading(Collections.emptyList(),
        Arrays.asList("java.lang.String", "java.lang.Integer"));

    assertDoesNotThrow(() -> validator.checkSecurity("java.lang.String"));
    assertDoesNotThrow(() -> validator.checkSecurity("java.lang.Integer"));
  }

  @Test
  void testCheckSecurityAllowsTrustedPackages() {
    ValidateClassLoading validator = new ValidateClassLoading(Arrays.asList("java.lang", "java.util"),
        Collections.emptyList());

    assertDoesNotThrow(() -> validator.checkSecurity("java.lang.String"));
    assertDoesNotThrow(() -> validator.checkSecurity("java.lang.Integer"));
    assertDoesNotThrow(() -> validator.checkSecurity("java.util.List"));
    assertDoesNotThrow(() -> validator.checkSecurity("java.util.HashMap"));
  }

  @Test
  void testCheckSecurityThrowsForUntrustedClasses() {
    ValidateClassLoading validator = new ValidateClassLoading(Collections.emptyList(), Collections.emptyList());

    SecurityException exception = assertThrows(SecurityException.class, () -> validator.checkSecurity("java.net.URL"));

    assertTrue(exception.getMessage().contains("Forbidden java.net.URL"));
    assertTrue(exception.getMessage().contains("This class is not trusted"));
    assertTrue(exception.getMessage().contains("org.apache.avro.SERIALIZABLE_CLASSES"));
    assertTrue(exception.getMessage().contains("org.apache.avro.SERIALIZABLE_PACKAGES"));
  }

  @Test
  void testCheckSecurityThrowsForUntrustedPackages() {
    ValidateClassLoading validator = new ValidateClassLoading(Arrays.asList("java.lang"), Collections.emptyList());

    SecurityException exception = assertThrows(SecurityException.class,
        () -> validator.checkSecurity("com.example.UntrustedClass"));

    assertTrue(exception.getMessage().contains("Forbidden com.example.UntrustedClass"));
  }

  @Test
  void testTrustAllPackagesWithWildcard() {
    ValidateClassLoading validator = new ValidateClassLoading(Arrays.asList("*"), Collections.emptyList());

    assertTrue(validator.trustAllPackages());

    // Should allow any class when wildcard is used
    assertDoesNotThrow(() -> validator.checkSecurity("any.package.AnyClass"));
    assertDoesNotThrow(() -> validator.checkSecurity("java.lang.String"));
    assertDoesNotThrow(() -> validator.checkSecurity("com.malicious.Class"));
  }

  @Test
  void testTrustAllPackagesWithoutWildcard() {
    ValidateClassLoading validator = new ValidateClassLoading(Arrays.asList("java.lang", "java.util"),
        Collections.emptyList());

    assertFalse(validator.trustAllPackages());
  }

  @Test
  void testTrustAllPackagesWithMultiplePackagesIncludingWildcard() {
    ValidateClassLoading validator = new ValidateClassLoading(Arrays.asList("java.lang", "*", "java.util"),
        Collections.emptyList());

    // Should return false because there are multiple packages
    assertFalse(validator.trustAllPackages());
  }

  @Test
  void testEmptyTrustedPackagesAndClasses() {
    ValidateClassLoading validator = new ValidateClassLoading(Collections.emptyList(), Collections.emptyList());

    assertFalse(validator.trustAllPackages());

    // Should still allow primitives
    assertDoesNotThrow(() -> validator.checkSecurity("int"));

    // Should throw for non-primitives
    assertThrows(SecurityException.class, () -> validator.checkSecurity("java.net.URL"));
  }

  @Test
  void testPackagePrefixMatching() {
    ValidateClassLoading validator = new ValidateClassLoading(Arrays.asList("java.lang", "org.apache.avro"),
        Collections.emptyList());

    // Should allow classes that start with trusted package names
    assertDoesNotThrow(() -> validator.checkSecurity("java.lang.String"));
    assertDoesNotThrow(() -> validator.checkSecurity("java.lang.reflect.Method"));
    assertDoesNotThrow(() -> validator.checkSecurity("org.apache.avro.Schema"));
    assertDoesNotThrow(() -> validator.checkSecurity("org.apache.avro.specific.SpecificRecord"));

    // Should not allow classes that don't start with trusted package names
    assertThrows(SecurityException.class, () -> validator.checkSecurity("java.xml.transform.Templates"));
    assertThrows(SecurityException.class, () -> validator.checkSecurity("org.apache.commons.Lang"));
  }

  @Test
  void testExactClassMatching() {
    ValidateClassLoading validator = new ValidateClassLoading(Collections.emptyList(),
        Arrays.asList("java.lang.String", "java.util.List"));

    // Should allow exact class matches
    assertDoesNotThrow(() -> validator.checkSecurity("java.lang.String"));
    assertDoesNotThrow(() -> validator.checkSecurity("java.util.List"));

    // Should not allow partial matches
    assertThrows(SecurityException.class, () -> validator.checkSecurity("java.lang.StringBuilder"));
    assertThrows(SecurityException.class, () -> validator.checkSecurity("java.util.PrefixedList"));
  }

  @Test
  void testCombinedPackageAndClassTrusting() {
    ValidateClassLoading validator = new ValidateClassLoading(Arrays.asList("java.lang"),
        Arrays.asList("java.util.List", "com.example.SpecialClass"));

    // Should allow all java.lang classes via package trust
    assertDoesNotThrow(() -> validator.checkSecurity("java.lang.String"));
    assertDoesNotThrow(() -> validator.checkSecurity("java.lang.Integer"));

    // Should allow specific trusted classes
    assertDoesNotThrow(() -> validator.checkSecurity("java.util.List"));
    assertDoesNotThrow(() -> validator.checkSecurity("com.example.SpecialClass"));

    // Should not allow untrusted classes
    assertThrows(SecurityException.class, () -> validator.checkSecurity("java.xml.transform.Templates"));
    assertThrows(SecurityException.class, () -> validator.checkSecurity("com.example.UntrustedClass"));
  }

  @Test
  void testNullClassNameThrowsSecurityException() {
    ValidateClassLoading validator = new ValidateClassLoading(Collections.emptyList(), Collections.emptyList());

    // Null class names should be rejected for security
    assertThrows(SecurityException.class, () -> validator.checkSecurity(null));
  }

  @Test
  void testEmptyClassNameThrowsSecurityException() {
    ValidateClassLoading validator = new ValidateClassLoading(Collections.emptyList(), Collections.emptyList());

    assertThrows(SecurityException.class, () -> validator.checkSecurity(""));
  }
}
