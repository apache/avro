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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Validates that a class is trusted to be included in Avro schemas. To be used
 * by {@link ClassUtils} which therefore automatically guards not only the
 * loading of the classes but, since the class names are translated into
 * {@link Class} objects by using {@link ClassUtils}, also guards any other
 * reflection-based mechanisms (e.g. instantiation, setting internal variables).
 *
 * @see #setGlobal(ClassSecurityPredicate)
 * @see #getGlobal()
 */
public final class ClassSecurityValidator {

  /**
   * Validates that the class is trusted to be included in Avro schemas.
   *
   * <p>
   * Note: this method shall be invoked with un-initialized classes only to
   * prevent any potential security issues the initialization may trigger.
   *
   * @param clazz the class to validate
   * @throws SecurityException if the class is not trusted
   */
  public static void validate(Class<?> clazz) {
    while (clazz.isArray()) {
      clazz = clazz.getComponentType();
    }
    if (clazz.isPrimitive()) {
      return;
    }
    if (!globalInstance.isTrusted(clazz)) {
      globalInstance.forbiddenClass(clazz.getName());
    }
  }

  /**
   * Sets the global {@link ClassSecurityPredicate} that is used by
   * {@link ClassUtils} to validate the trusted classes.
   *
   * @param validator the validator to use
   */
  public static void setGlobal(ClassSecurityPredicate validator) {
    globalInstance = Objects.requireNonNull(validator);
  }

  /**
   * Returns the global {@link ClassSecurityPredicate} that is used by
   * {@link ClassUtils} to validate the trusted classes.
   *
   * @return the global validator
   */
  public static ClassSecurityPredicate getGlobal() {
    return globalInstance;
  }

  private ClassSecurityValidator() {
  }

  /**
   * A predicate that checks if a class is trusted to be included in Avro schemas.
   */
  public interface ClassSecurityPredicate {
    /**
     * Checks if the class is trusted to be included in Avro schemas.
     *
     * @param clazz the class to check
     * @return true if the class is trusted, false otherwise
     */
    boolean isTrusted(Class<?> clazz);

    /**
     * Throws a {@link SecurityException} with a message indicating that the class
     * is not trusted to be included in Avro schemas.
     *
     * @param className the name of the class that is not trusted
     */
    default void forbiddenClass(String className) {
      throw new SecurityException("Forbidden " + className + "! This class is not trusted to be included in Avro "
          + "schemas. You may either use the system properties org.apache.avro.SERIALIZABLE_CLASSES and "
          + "org.apache.avro.SERIALIZABLE_PACKAGES to set the comma separated list of the classes or packages you trust, "
          + "or you can set them via the API (see org.apache.avro.util.ClassSecurityValidator).");
    }
  }

  /**
   * A couple of trusted classes that are safe to be loaded, instantiated with any
   * constructors or alter any internals via reflection.
   */
  public static final ClassSecurityPredicate DEFAULT_TRUSTED_CLASSES = builder().add("java.lang.Boolean")
      .add("java.lang.Byte").add("java.lang.Character").add("java.lang.CharSequence").add("java.lang.Double")
      .add("java.lang.Enum").add("java.lang.Float").add("java.lang.Integer").add("java.lang.Long")
      .add("java.lang.Number").add("java.lang.Object").add("java.lang.Short").add("java.lang.String")
      .add("java.lang.Void").add("java.math.BigDecimal").add("java.math.BigInteger").build();

  /**
   * The predicate that uses the system properties
   * {@value SystemPropertiesPredicate#SYSPROP_SERIALIZABLE_CLASSES} and
   * {@value SystemPropertiesPredicate#SYSPROP_SERIALIZABLE_PACKAGES}.
   */
  public static final ClassSecurityPredicate SYSTEM_PROPERTIES = new SystemPropertiesPredicate();

  /**
   * The default predicate that uses both the system properties and the hard-coded
   * trusted classes.
   *
   * @see #DEFAULT_TRUSTED_CLASSES
   * @see #SYSTEM_PROPERTIES
   */
  public static final ClassSecurityPredicate DEFAULT = composite(DEFAULT_TRUSTED_CLASSES, SYSTEM_PROPERTIES);

  private static ClassSecurityPredicate globalInstance = DEFAULT;

  /**
   * Creates a builder for a {@link ClassSecurityValidator} that validates the
   * trusted classes by whitelisting their names. Note: no parent validator is
   * used.
   *
   * @return a new {@link ClassSecurityValidator} builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a composite {@link ClassSecurityValidator} that delegates to the
   * given validators.
   *
   * @param validators the validators to delegate to
   * @return a new {@link ClassSecurityValidator} that delegates to the given
   *         validators
   */
  public static ClassSecurityPredicate composite(ClassSecurityPredicate... validators) {
    return clazz -> Arrays.stream(validators).anyMatch(v -> v.isTrusted(clazz));
  }

  public static class Builder {
    private final Set<String> allowedClassNames = new HashSet<>();

    private Builder() {
    }

    public Builder add(String className) {
      allowedClassNames.add(className);
      return this;
    }

    public Builder add(Class<?> clazz) {
      return add(clazz.getName());
    }

    public ClassSecurityPredicate build() {
      return clazz -> allowedClassNames.contains(clazz.getName());
    }
  }

  public static class SystemPropertiesPredicate implements ClassSecurityPredicate {

    /**
     * The set of trusted classes specified by the system property
     * {@value #SYSPROP_SERIALIZABLE_CLASSES}. Empty by default.
     */
    public static final Set<String> SERIALIZABLE_CLASSES;

    /**
     * The set of trusted packages specified by the system property
     * {@value #SYSPROP_SERIALIZABLE_PACKAGES}. Empty by default.
     */
    public static final NavigableSet<String> SERIALIZABLE_PACKAGES;

    private static final boolean TRUST_ALL_PACKAGES;

    private static final String SYSPROP_SERIALIZABLE_CLASSES = "org.apache.avro.SERIALIZABLE_CLASSES";

    private static final String SYSPROP_SERIALIZABLE_PACKAGES = "org.apache.avro.SERIALIZABLE_PACKAGES";

    static {
      // add the hard-coded trusted classes as well
      SERIALIZABLE_CLASSES = Collections.unmodifiableSet(
          streamPropertyEntries(System.getProperty(SYSPROP_SERIALIZABLE_CLASSES)).collect(Collectors.toSet()));

      // no default serializable packages are hard-coded
      NavigableSet<String> packages = streamPropertyEntries(System.getProperty(SYSPROP_SERIALIZABLE_PACKAGES))
          // Add a '.' suffix to ensure we'll be matching package names instead of
          // arbitrary prefixes, except for the wildcard "*", which allows all
          // packages (this is only safe in fully controlled environments!).
          .map(entry -> "*".equals(entry) ? entry : entry + ".").collect(TreeSet::new, TreeSet::add, TreeSet::addAll);
      TRUST_ALL_PACKAGES = packages.remove("*");

      SERIALIZABLE_PACKAGES = Collections.unmodifiableNavigableSet(packages);
    }

    /**
     * Parse a comma separated list into non-empty entries. Leading and trailing
     * whitespace is stripped.
     *
     * @param commaSeparatedEntries the comma separated list of entries
     * @return a stream of the entries
     */
    private static Stream<String> streamPropertyEntries(String commaSeparatedEntries) {
      if (commaSeparatedEntries == null) {
        return Stream.empty();
      }
      return Stream.of(commaSeparatedEntries.split(",")).map(s -> s.replaceAll("^\\s+|\\s+$", ""))
          .filter(s -> !s.isEmpty());
    }

    private SystemPropertiesPredicate() {
    }

    @Override
    public boolean isTrusted(Class<?> clazz) {
      if (TRUST_ALL_PACKAGES) {
        return true;
      }

      String className = clazz.getName();

      if (SERIALIZABLE_CLASSES.contains(className)) {
        return true;
      }

      String lower = SERIALIZABLE_PACKAGES.lower(className);
      return lower != null && className.startsWith(lower);
    }
  }
}
