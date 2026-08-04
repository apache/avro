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
import java.util.ServiceLoader;

/**
 * Interface and factory to format schemas to text.
 *
 * <p>
 * Schema formats have a name, and optionally a variant (all lowercase). The
 * Avro library supports a few formats out of the box:
 * </p>
 *
 * <dl>
 *
 * <dt>{@code json}</dt>
 * <dd>Classic schema definition (which is a form of JSON). Supports the
 * variants {@code pretty} (the default) and {@code inline}. Can be written as
 * .avsc files. See the specification (<a href=
 * "https://avro.apache.org/docs/current/specification/#schema-declaration">Schema
 * Declaration</a>) for more details.</dd>
 *
 * <dt>{@code canonical}</dt>
 * <dd>Parsing Canonical Form; this uniquely defines how Avro data is written.
 * Used to generate schema fingerprints.<br/>
 * See the specification (<a href=
 * "https://avro.apache.org/docs/current/specification/#parsing-canonical-form-for-schemas">Parsing
 * Canonical Form for Schemas</a>) for more details.</dd>
 *
 * <dt>{@code idl}</dt>
 * <dd>IDL: a format that looks much like source code, and is arguably easier to
 * read than JSON. Available when the module {@code avro-idl} is on the
 * classpath. See
 * <a href="https://avro.apache.org/docs/current/idl-language/">IDL Language</a>
 * for more details.</dd>
 *
 * </dl>
 *
 * <p>
 * Additional formats can be defined by implementing
 * {@link SchemaFormatterFactory}. They are located using a
 * {@link java.util.ServiceLoader}, which loads them using the context
 * ClassLoader when available, or the application ClassLoader when not. See the
 * {@code ServiceLoader} class for more detailsπ.
 * </p>
 *
 * @see <a href=
 *      "https://avro.apache.org/docs/current/specification/#schema-declaration">Specification:
 *      Schema Declaration</a>
 * @see <a href=
 *      "https://avro.apache.org/docs/current/specification/#parsing-canonical-form-for-schemas">Specification:
 *      Parsing Canonical Form for Schemas</a>
 * @see <a href="https://avro.apache.org/docs/current/idl-language/">IDL
 *      Language</a>
 * @see java.util.ServiceLoader
 */
public interface SchemaFormatter {
  /**
   * Get the schema formatter for the specified format name with optional variant.
   *
   * @param name a format with optional variant, for example "json/pretty",
   *             "canonical" or "idl"
   * @return the schema formatter for the specified format
   * @throws AvroRuntimeException if the schema format is not supported
   */
  static SchemaFormatter getInstance(String name) {
    int slashPos = name.indexOf("/");
    // SchemaFormatterFactory.getFormatterForVariant(String) receives the name of
    // the variant in lowercase (as stated in its javadoc). We're doing a
    // case-insensitive comparison on the format name instead, so we don't have to
    // convert the format name provided by the factory to lower case.
    // This ensures the least amount of assumptions about implementations.
    String formatName = slashPos < 0 ? name : name.substring(0, slashPos);
    String variantName = slashPos < 0 ? null : name.substring(slashPos + 1).toLowerCase(Locale.ROOT);

    for (SchemaFormatterFactory formatterFactory : SchemaFormatterCache.LOADER) {
      if (formatName.equalsIgnoreCase(formatterFactory.formatName())) {
        if (variantName == null) {
          return formatterFactory.getDefaultFormatter();
        } else {
          return formatterFactory.getFormatterForVariant(variantName);
        }
      }
    }
    throw new AvroRuntimeException("Unsupported schema format: " + name + "; see the javadoc for valid examples");
  }

  /**
   * Format a schema with the specified format. Shorthand for
   * {@code getInstance(name).format(schema)}.
   *
   * @param name   the name of the schema format
   * @param schema the schema to format
   * @return the formatted schema
   * @throws AvroRuntimeException if the schema format is not supported
   * @see #getInstance(String)
   * @see #format(Schema)
   */
  static String format(String name, Schema schema) {
    return getInstance(name).format(schema);
  }

  /**
   * Write the specified schema as a String.
   *
   * @param schema the schema to write
   * @return the formatted schema
   */
  String format(Schema schema);
}

class SchemaFormatterCache {
  static final ServiceLoader<SchemaFormatterFactory> LOADER = ServiceLoader.load(SchemaFormatterFactory.class);
}
