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

/**
 * Schema formatter factory that supports the "Parsing Canonical Form".
 *
 * @see <a href=
 *      "https://avro.apache.org/docs/1.11.1/specification/#parsing-canonical-form-for-schemas">Specification:
 *      Parsing Canonical Form for Schemas</a>
 */
public class CanonicalSchemaFormatterFactory implements SchemaFormatterFactory, SchemaFormatter {
  @Override
  public SchemaFormatter getDefaultFormatter() {
    return this;
  }

  @Override
  public String format(Schema schema) {
    return SchemaNormalization.toParsingForm(schema);
  }
}
