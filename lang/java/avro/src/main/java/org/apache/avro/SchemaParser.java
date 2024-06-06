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

import org.apache.avro.util.UtfTextUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Avro schema parser for text-based formats like JSON, IDL, etc.
 *
 * <p>
 * Parses formatted (i.e., text based) schemata from a given source using the
 * available {@link FormattedSchemaParser} implementations, and returns the
 * first result. This means it can transparently handle any schema format. The
 * Avro project defines a JSON based format and an IDL format (the latter
 * available as a separate dependency), but you can also provide your own.
 * </p>
 *
 * <p>
 * The parser can handle various text based sources. If the source contains a
 * UTF encoded latin text based format it can even detect which UTF encoding was
 * used (UTF-8, UTF16BE, UTF16LE, UTF-32BE or UTF32LE).
 * </p>
 *
 * @see FormattedSchemaParser
 * @see UtfTextUtils
 */
public class SchemaParser {
  private final ParseContext parseContext;
  private final Collection<FormattedSchemaParser> formattedSchemaParsers;

  /**
   * Create a schema parser. Initially, the list of known (named) schemata is
   * empty.
   */
  public SchemaParser() {
    this.parseContext = new ParseContext();
    this.formattedSchemaParsers = new ArrayList<>();
    for (FormattedSchemaParser formattedSchemaParser : ServiceLoader.load(FormattedSchemaParser.class)) {
      formattedSchemaParsers.add(formattedSchemaParser);
    }
    // Add the default / JSON parser last (not as a service, even though it
    // implements the service interface), to allow implementations that parse JSON
    // files into schemata differently.
    formattedSchemaParsers.add(new JsonSchemaParser());
  }

  /**
   * Parse an Avro schema from a file. The file content is assumed to be UTF-8
   * text.
   *
   * @param file the file to read
   * @return the schema
   * @throws IOException          when the schema cannot be read
   * @throws SchemaParseException if parsing the schema failed; contains
   *                              suppressed underlying parse exceptions if
   *                              available
   * @see UtfTextUtils
   */
  public ParseResult parse(File file) throws IOException, SchemaParseException {
    return parse(file, null);
  }

  /**
   * Parse an Avro schema from a file written with a specific character set.
   *
   * @param file    the file to read
   * @param charset the character set of the file contents
   * @return the schema
   * @throws IOException          when the schema cannot be read
   * @throws SchemaParseException if parsing the schema failed; contains
   *                              suppressed underlying parse exceptions if
   *                              available
   */
  public ParseResult parse(File file, Charset charset) throws IOException, SchemaParseException {
    return parse(file.toPath(), charset);
  }

  /**
   * Parse an Avro schema from a file. The file content is assumed to be UTF-8
   * text.
   *
   * @param file the file to read
   * @return the schema
   * @throws IOException          when the schema cannot be read
   * @throws SchemaParseException if parsing the schema failed; contains
   *                              suppressed underlying parse exceptions if
   *                              available
   * @see UtfTextUtils
   */
  public ParseResult parse(Path file) throws IOException, SchemaParseException {
    return parse(file, null);
  }

  /**
   * Parse an Avro schema from a file written with a specific character set.
   *
   * @param file    the file to read
   * @param charset the character set of the file contents
   * @return the schema
   * @throws IOException          when the schema cannot be read
   * @throws SchemaParseException if parsing the schema failed; contains
   *                              suppressed underlying parse exceptions if
   *                              available
   */
  public ParseResult parse(Path file, Charset charset) throws IOException, SchemaParseException {
    URI inputDir = file.getParent().toUri();
    try (InputStream stream = Files.newInputStream(file)) {
      String formattedSchema = UtfTextUtils.readAllBytes(stream, charset);
      return parse(inputDir, formattedSchema);
    }
  }

  /**
   * Parse an Avro schema from a file written with a specific character set.
   *
   * @param location the location of the schema resource
   * @param charset  the character set of the schema resource
   * @return the schema
   * @throws IOException          when the schema cannot be read
   * @throws SchemaParseException if parsing the schema failed; contains
   *                              suppressed underlying parse exceptions if
   *                              available
   */
  public ParseResult parse(URI location, Charset charset) throws IOException, SchemaParseException {
    try (InputStream stream = location.toURL().openStream()) {
      String formattedSchema = UtfTextUtils.readAllBytes(stream, charset);
      return parse(location, formattedSchema);
    }
  }

  /**
   * Parse an Avro schema from an input stream. The stream content is assumed to
   * be UTF-8 text. Note that the stream stays open after reading.
   *
   * @param in the stream to read
   * @return the schema
   * @throws IOException          when the schema cannot be read
   * @throws SchemaParseException if parsing the schema failed; contains
   *                              suppressed underlying parse exceptions if
   *                              available
   * @see UtfTextUtils
   */
  public ParseResult parse(InputStream in) throws IOException, SchemaParseException {
    return parse(in, null);
  }

  /**
   * Parse an Avro schema from an input stream. Note that the stream stays open
   * after reading.
   *
   * @param in      the stream to read
   * @param charset the character set of the stream contents
   * @return the schema
   * @throws IOException          when the schema cannot be read
   * @throws SchemaParseException if parsing the schema failed; contains
   *                              suppressed underlying parse exceptions if
   *                              available
   */
  public ParseResult parse(InputStream in, Charset charset) throws IOException, SchemaParseException {
    return parse(UtfTextUtils.readAllBytes(in, charset));
  }

  /**
   * Parse an Avro schema from an input reader.
   *
   * @param in the stream to read
   * @return the schema
   * @throws IOException          when the schema cannot be read
   * @throws SchemaParseException if parsing the schema failed; contains
   *                              suppressed underlying parse exceptions if
   *                              available
   */
  public ParseResult parse(Reader in) throws IOException, SchemaParseException {
    return parse(UtfTextUtils.readAllChars(in));
  }

  /**
   * Parse an Avro schema from a string.
   *
   * @param text the text to parse
   * @return the schema
   * @throws SchemaParseException if parsing the schema failed; contains
   *                              suppressed underlying parse exceptions if
   *                              available
   */
  public ParseResult parse(CharSequence text) throws SchemaParseException {
    try {
      return parse(null, text);
    } catch (IOException e) {
      // This can only happen if parser implementations try to read other (related)
      // schemata from somewhere.
      throw new AvroRuntimeException("Could not read schema", e);
    }
  }

  /**
   * Parse the given schema (string) within the specified context using all
   * available {@link FormattedSchemaParser} implementations, collecting any
   * {@link SchemaParseException}s that occur, and return the first successfully
   * parsed schema. If all parsers fail, throw a {@code SchemaParseException} with
   * all collected parse exceptions added as suppressed exceptions. Uses the base
   * location of the schema (e.g., the directory where the schema file lives) if
   * available.
   *
   * @param baseUri         the base location of the schema, or {@code null} if
   *                        not known
   * @param formattedSchema the schema as text
   * @return the parsed schema
   * @throws IOException          if thrown by one of the parsers
   * @throws RuntimeException     if thrown by one of the parsers
   * @throws SchemaParseException when all parsers fail
   */
  private ParseResult parse(URI baseUri, CharSequence formattedSchema) throws IOException, SchemaParseException {
    List<SchemaParseException> parseExceptions = new ArrayList<>();
    for (FormattedSchemaParser formattedSchemaParser : formattedSchemaParsers) {
      try {
        Schema schema = formattedSchemaParser.parse(parseContext, baseUri, formattedSchema);
        if (parseContext.hasNewSchemas() || schema != null) {
          // Parsing succeeded: return the result.
          return parseContext.commit(schema);
        }
      } catch (SchemaParseException e) {
        parseContext.rollback();
        parseExceptions.add(e);
      }
    }

    // None of the available parsers succeeded

    if (parseExceptions.size() == 1) {
      throw parseExceptions.get(0);
    }
    SchemaParseException parseException = new SchemaParseException(
        "Could not parse the schema (the suppressed exceptions tell why).");
    parseExceptions.forEach(parseException::addSuppressed);
    throw parseException;
  }

  /**
   * Get all parsed schemata.
   *
   * @return all parsed schemas, in the order they were parsed
   */
  public List<Schema> getParsedNamedSchemas() {
    return parseContext.resolveAllSchemas();
  }

  // Temporary method to reduce PR size
  @Deprecated
  public Schema resolve(ParseResult result) {
    return result.mainSchema();
  }

  public interface ParseResult {
    /**
     * The main schema parsed from a file. Can be any schema, or {@code null} if the
     * parsed file has no "main" schema.
     */
    Schema mainSchema();

    /**
     * The list of named schemata that were parsed.
     */
    List<Schema> parsedNamedSchemas();
  }
}
