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
 * The {@link SchemaParser} class uses this interface, supporting text based
 * schema sources.
 * </p>
 *
 * <p>
 * Implementations are located using a {@link java.util.ServiceLoader} and must
 * therefore be threadsafe. See the {@code ServiceLoader} class for details on
 * loading your implementation.
 * </p>
 *
 * @see java.util.ServiceLoader
 */
public interface FormattedSchemaParser {
  /**
   * <p>
   * Parse schema definitions from a text based source.
   * </p>
   *
   * <h2>Notes for implementers:</h2>
   *
   * <ul>
   * <li>Schema definitions are expected not to be in the format the parser
   * expects. So when the input clearly doesn't make sense (e.g., reading "/**"
   * when expecting JSON), it is a good idea not to do anything (especially
   * calling methods on the @code ParseContext}).</li>
   * <li>The parameter {@code parseContext} is not thread-safe.</li>
   * <li>When parsing, all parsed schema definitions should be added to the
   * provided {@link ParseContext}.</li>
   * <li>Optionally, you may return a "main" schema. Some schema definitions have
   * one, for example the schema defined by the root of the JSON document in a
   * <a href="https://avro.apache.org/docs/current/specification/">standard schema
   * definition</a>. If unsure, return {@code null}.</li>
   * <li>If parsing fails, throw a {@link SchemaParseException}. This will let the
   * parsing process recover and continue.</li>
   * <li>Throwing anything other than a {@code SchemaParseException} will abort
   * the parsing process, so reserve that for rethrowing exceptions.</li>
   * </ul>
   *
   * @param parseContext    the current parse context: all parsed schemata should
   *                        be added here to resolve names with; contains all
   *                        previously known types
   * @param baseUri         the base location of the schema, or {@code null} if
   *                        not known
   * @param formattedSchema the text of the schema definition(s) to parse
   * @return the main schema, if any
   * @throws IOException          when the schema cannot be read
   * @throws SchemaParseException when the schema cannot be parsed
   */
  Schema parse(ParseContext parseContext, URI baseUri, CharSequence formattedSchema)
      throws IOException, SchemaParseException;
}
