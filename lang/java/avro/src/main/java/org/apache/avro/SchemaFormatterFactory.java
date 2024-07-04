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

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service Provider Interface (SPI) for {@link SchemaFormatter}.
 *
 * <p>
 * Notes to implementers:
 * </p>
 *
 * <ul>
 *
 * <li>Implementations are located using a {@link java.util.ServiceLoader}. See
 * that class for details.</li>
 *
 * <li>Implementing classes should either be named
 * {@code <format>SchemaFormatterFactory} (where the format is alphanumeric), or
 * implement {@link #formatName()}.</li>
 *
 * <li>Implement at least {@link #getDefaultFormatter()}; use it to call
 * {@link #getFormatterForVariant(String)} if the format supports multiple
 * variants.</li>
 *
 * <li>Example implementations are {@link JsonSchemaFormatterFactory} and
 * {@link CanonicalSchemaFormatterFactory}</li>
 *
 * </ul>
 *
 * @see java.util.ServiceLoader
 */
public interface SchemaFormatterFactory {
  /**
   * Return the name of the format this formatter factory supports.
   *
   * <p>
   * The default implementation returns the lowercase prefix of the implementing
   * class if it is named {@code <format>SchemaFormatterFactory}. That is, if the
   * implementing class is named {@code some.package.JsonSchemaFormatterFactory},
   * it returns: {@literal "json"}
   * </p>
   *
   * @return the name of the format
   */
  default String formatName() {
    String simpleName = getClass().getSimpleName();
    Matcher matcher = SchemaFormatterFactoryConstants.SIMPLE_NAME_PATTERN.matcher(simpleName);
    if (matcher.matches()) {
      return matcher.group(1).toLowerCase(Locale.ROOT);
    } else {
      throw new AvroRuntimeException(
          "Formatter is not named \"<format>SchemaFormatterFactory\"; cannot determine format name.");
    }
  }

  /**
   * Get the default formatter for this schema format. Instances should be
   * thread-safe, as they may be cached.
   *
   * <p>
   * Implementations should either return the only formatter for this format, or
   * call {@link #getFormatterForVariant(String)} with the default variant and
   * implement that method as well.
   * </p>
   *
   * @return the default formatter for this schema format
   */
  SchemaFormatter getDefaultFormatter();

  /**
   * Get a formatter for the specified schema format variant, if multiple variants
   * are supported. Instances should be thread-safe, as they may be cached.
   *
   * @param variantName the name of the format variant (lower case), if specified
   * @return if the factory supports the format, a schema writer; {@code null}
   *         otherwise
   */
  default SchemaFormatter getFormatterForVariant(String variantName) {
    throw new AvroRuntimeException("The schema format \"" + formatName() + "\" has no variants.");
  }
}

class SchemaFormatterFactoryConstants {
  static final Pattern SIMPLE_NAME_PATTERN = Pattern.compile(
      "([a-z][0-9a-z]*)" + SchemaFormatterFactory.class.getSimpleName(),
      Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
}
