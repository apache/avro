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
package org.apache.avro.util;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import org.apache.avro.util.ClassSecurityValidator.ClassSecurityPredicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestClassSecurityValidator {

  // To test inner classes
  private static class TestInnerClass {
  }

  private ClassSecurityPredicate originalValidator;

  @BeforeEach
  public void saveOriginalValidator() {
    originalValidator = ClassSecurityValidator.getGlobal();
  }

  @AfterEach
  public void restoreOriginalValidator() {
    ClassSecurityValidator.setGlobal(originalValidator);
  }

  @Test
  void testDefault() {
    // Test a couple of default trusted classes via ClassUtils
    assertDoesNotThrow(() -> ClassUtils.forName(boolean[][][][][][].class.getName()));
    assertDoesNotThrow(() -> ClassUtils.forName("java.lang.String"));
    assertDoesNotThrow(() -> ClassUtils.forName(java.math.BigDecimal[][][][].class.getName()));

    // The package "org.apache.avro" is allowed by default for the test environment
    assertDoesNotThrow(() -> ClassUtils.forName("org.apache.avro.util.TestClassSecurityValidator$TestInnerClass"));

    // Test a couple of default untrusted classes via ClassUtils
    assertThrows(SecurityException.class, () -> ClassUtils.forName("java.net.InetAddress"));
    assertThrows(SecurityException.class, () -> ClassUtils.forName("java.io.FileInputStream"));
  }

  @Test
  void testBuilder() {
    ClassSecurityValidator.setGlobal(ClassSecurityValidator.builder().add(TestClassSecurityValidator.class).build());

    assertDoesNotThrow(() -> ClassUtils.forName("org.apache.avro.util.TestClassSecurityValidator"));
    assertThrows(SecurityException.class,
        () -> ClassUtils.forName("org.apache.avro.util.TestClassSecurityValidator$TestInnerClass"));

    // Test that arrays and primitives also work
    assertDoesNotThrow(() -> ClassUtils.forName(short[][][][][].class.getName()));
    assertDoesNotThrow(() -> ClassUtils.forName(TestClassSecurityValidator[][][][].class.getName()));
    assertThrows(SecurityException.class, () -> ClassUtils.forName(TestInnerClass[][].class.getName()));
  }

  @Test
  void testOwnImplementation() {
    ClassSecurityValidator.setGlobal(new ClassSecurityPredicate() {
      @Override
      public boolean isTrusted(Class<?> clazz) {
        return clazz.getSimpleName().contains("Inner");
      }

      @Override
      public void forbiddenClass(String className) {
        throw new SecurityException("Not inner");
      }
    });
    assertDoesNotThrow(() -> ClassUtils.forName(TestInnerClass.class.getName()));
    Exception e = assertThrows(SecurityException.class,
        () -> ClassUtils.forName(TestClassSecurityValidator.class.getName()));
    assertEquals("Not inner", e.getMessage());

    // Test that arrays and primitives also work
    assertDoesNotThrow(() -> ClassUtils.forName(char[][][][].class.getName()));
    assertDoesNotThrow(() -> ClassUtils.forName(TestInnerClass[][][][].class.getName()));
    e = assertThrows(SecurityException.class, () -> ClassUtils.forName(TestClassSecurityValidator[][].class.getName()));
    assertEquals("Not inner", e.getMessage());
  }

  @Test
  void testBuildComplexPredicate() {
    ClassSecurityValidator.setGlobal(ClassSecurityValidator.composite(
        ClassSecurityValidator.builder().add(TestInnerClass.class).add(TestClassSecurityValidator.class).build(),
        ClassSecurityValidator.DEFAULT, c -> c.getPackageName().equals("java.lang")));

    // Test that the defaults work since we included them
    testDefault();

    assertDoesNotThrow(() -> ClassUtils.forName(TestInnerClass.class.getName()));
    assertDoesNotThrow(() -> ClassUtils.forName(TestClassSecurityValidator.class.getName()));
    assertDoesNotThrow(() -> ClassUtils.forName(StringBuilder.class.getName()));
    assertDoesNotThrow(() -> ClassUtils.forName("java.lang.StringBuffer"));
    assertDoesNotThrow(() -> ClassUtils.forName(BigInteger.class.getName()));
  }
}
