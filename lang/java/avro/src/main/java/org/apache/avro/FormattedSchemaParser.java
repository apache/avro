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
 * Schema parser for a specific schema format.
 *
 * <p>
 * The {@link SchemaParser} class uses this interface, supporting both binary
 * and text based schema sources.
 * </p>
 *
 * <h2>Note to implementers:</h2>
 *
 * <p>
 * Implementations are located using a {@link java.util.ServiceLoader}. See that
 * class for details.
 * </p>
 *
 * <p>
 * You can expect that schemas being read are invalid, so you are encouraged to
 * return {@code null} upon parsing failure where the input clearly doesn't make
 * sense (e.g., reading "/**" when expecting JSON). If the input is likely in
 * the correct format, but invalid, throw a {@link SchemaParseException}
 * instead.
 * </p>
 *
 * <p>
 * Note that throwing anything other than a {@code SchemaParseException} will
 * abort the parsing process, so reserve that for rethrowing exceptions.
 * </p>
 *
 * @see java.util.ServiceLoader
 */
public interface FormattedSchemaParser {
  /**
   * Parse a schema from a text based source. Can use the base location of the
   * schema (e.g., the directory where the schema file lives) if available.
   *
   * @param nameContext     the name context with already known types to use
   * @param baseUri         the base location of the schema, or {@code null} if
   *                        not known
   * @param formattedSchema the schema as text
   * @return the parsed schema, or {@code null} if the format is not supported
   * @throws IOException          when the schema cannot be read
   * @throws SchemaParseException when the schema cannot be parsed
   */
  Schema parse(NameContext nameContext, URI baseUri, CharSequence formattedSchema)
      throws IOException, SchemaParseException;
}
