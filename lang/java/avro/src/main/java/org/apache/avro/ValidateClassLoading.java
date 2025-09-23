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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validates class loading during Avro deserialization. This class validates
 * that only trusted classes and packages are loaded when processing schema
 * properties like java-class, java-key-class, and java-element-class.
 */
public class ValidateClassLoading {

  /**
   * @deprecated prefer to use SERIALIZABLE_CLASSES instead.
   */
  @Deprecated
  public static final String[] SERIALIZABLE_PACKAGES;

  public static final String[] SERIALIZABLE_CLASSES;

  static {
    // no serializable classes by default
    String serializableClassesProp = System.getProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    SERIALIZABLE_CLASSES = (serializableClassesProp == null) ? new String[0] : serializableClassesProp.split(",");

    // no serializable packages by default
    String serializablePackagesProp = System.getProperty("org.apache.avro.SERIALIZABLE_PACKAGES");
    SERIALIZABLE_PACKAGES = (serializablePackagesProp == null) ? new String[0] : serializablePackagesProp.split(",");
  }

  // The always-allowed class names. This is primitives (i.e. Class.isPrimitive())
  // and default collection types.
  private static final Set<String> DEFAULT_SAFE_CLASSES = new HashSet<>(
      Arrays.asList(Boolean.TYPE.getName(), Character.TYPE.getName(), Byte.TYPE.getName(), Short.TYPE.getName(),
          Integer.TYPE.getName(), Long.TYPE.getName(), Float.TYPE.getName(), Double.TYPE.getName(), Void.TYPE.getName(),
          "java.util.List", "java.util.Map", "java.util.Set", "java.util.Collection", "java.util.ArrayList",
          "java.util.HashMap", "java.util.HashSet", "java.util.LinkedList", "java.util.LinkedHashMap",
          "java.util.TreeMap", "java.util.TreeSet", "java.util.concurrent.ConcurrentHashMap", "java.lang.String"));

  private final List<String> trustedPackages;
  private final List<String> trustedClasses;

  /**
   * Create a validator with default trusted packages and classes from system
   * properties.
   */
  public ValidateClassLoading() {
    // Re-read system properties to allow for test scenarios
    String serializableClassesProp = System.getProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    String[] runtimeClasses = (serializableClassesProp == null) ? new String[0] : serializableClassesProp.split(",");

    String serializablePackagesProp = System.getProperty("org.apache.avro.SERIALIZABLE_PACKAGES");
    String[] runtimePackages = (serializablePackagesProp == null) ? new String[0] : serializablePackagesProp.split(",");

    this.trustedPackages = Arrays.asList(runtimePackages);
    this.trustedClasses = Arrays.asList(runtimeClasses);
  }

  /**
   * Create a validator with specific trusted packages and classes.
   * 
   * @param trustedPackages List of trusted package prefixes
   * @param trustedClasses  List of trusted class names
   */
  public ValidateClassLoading(List<String> trustedPackages, List<String> trustedClasses) {
    this.trustedPackages = trustedPackages;
    this.trustedClasses = trustedClasses;
  }

  /**
   * Check if a class name is trusted for loading.
   * 
   * @param className The fully qualified class name to check
   * @throws SecurityException if the class is not trusted
   */
  public void checkSecurity(String className) {
    if (trustAllPackages() || trustAllClasses() || DEFAULT_SAFE_CLASSES.contains(className)) {
      return;
    }

    for (String trustedClass : trustedClasses) {
      if (className.equals(trustedClass)) {
        return;
      }
    }

    // Check configured trusted packages
    for (String trustedPackage : trustedPackages) {
      if (className.startsWith(trustedPackage)) {
        return;
      }
    }

    // Resolve the class name to a Class object to ensure it exists
    try {
      Class<?> clazz = Class.forName(className, false, Thread.currentThread().getContextClassLoader());
      if (clazz.isPrimitive()) {
        return;
      }
      if (clazz.isArray()) {
        // For arrays, check the component type
        Class<?> componentType = clazz.getComponentType();
        checkSecurity(componentType.getName());
        return;
      }
    } catch (ClassNotFoundException e) {
      // Class not found, not trusted
    } catch (NullPointerException e) {
      // Class name is null, not trusted
    }

    throw new SecurityException("Forbidden " + className
        + "! This class is not trusted to be included in Avro schema. Please set org.apache.avro.SERIALIZABLE_CLASSES system property with the class you trust or org.apache.avro.SERIALIZABLE_PACKAGES system property with the packages you trust.");
  }

  /**
   * Check if all packages are trusted (wildcard "*" configuration).
   * 
   * @return true if all packages are trusted
   */
  public boolean trustAllPackages() {
    return (trustedPackages.size() == 1 && "*".equals(trustedPackages.get(0)));
  }

  /**
   * Get the list of trusted packages.
   * 
   * @return List of trusted package prefixes
   * @deprecated Use getTrustedClasses() instead
   */
  @Deprecated
  public List<String> getTrustedPackages() {
    return trustedPackages;
  }

  /**
   * Check if all classes are trusted (wildcard "*" configuration).
   * 
   * @return true if all classes are trusted
   */
  public boolean trustAllClasses() {
    return (trustedClasses.size() == 1 && "*".equals(trustedClasses.get(0)));
  }

  /**
   * Get the list of trusted classes.
   * 
   * @return List of trusted class names
   */
  public List<String> getTrustedClasses() {
    return trustedClasses;
  }
}
