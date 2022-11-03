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

import java.io.IOException;
import java.net.URI;

/**
 * Schema parser for JSON formatted schemata. This initial implementation simply
 * delegates to the {@link Schema.Parser} class, though it should be refactored
 * out of there.
 *
 * <p>
 * Note: this class is intentionally not available via the Java
 * {@link java.util.ServiceLoader}, as its use is hardcoded as fallback when no
 * service exists. This enables users to reliably override the standard JSON
 * parser as well.
 * </p>
 */
public class JsonSchemaParser implements FormattedSchemaParser {
  /**
   * Internal method to parse a schema without any validations. This is intended
   * for use in generated code, as schema validations should be done before
   * compiling the code.
   *
   * @param fragments one or more strings making up the schema (some schemata
   *                  exceed the compiler limits)
   * @return the parsed schema
   */
  public static Schema parseInternal(String... fragments) {
    StringBuilder buffer = new StringBuilder();
    for (String fragment : fragments) {
      buffer.append(fragment);
    }
    return new JsonSchemaParser().parse(new NameContext(), buffer, true);
  }

  @Override
  public Schema parse(NameContext nameContext, URI baseUri, CharSequence formattedSchema)
      throws IOException, SchemaParseException {
    return parse(nameContext, formattedSchema, false);
  }

  private Schema parse(NameContext nameContext, CharSequence formattedSchema, boolean skipValidation)
      throws SchemaParseException {
    // TODO: refactor JSON parsing out of the Schema class
    Schema.Parser parser = new Schema.Parser().addTypes(nameContext.typesByName());
    if (skipValidation) {
      parser.setValidate(false);
      parser.setValidateDefaults(false);
    }
    Schema schema = parser.parse(formattedSchema.toString());
    parser.getTypes().values().forEach(nameContext::put);
    return schema;
  }
}
