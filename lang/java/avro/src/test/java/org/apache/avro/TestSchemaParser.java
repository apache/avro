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

import com.fasterxml.jackson.core.JsonParseException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSchemaParser {
  private static final Schema SCHEMA_REAL = Schema.createFixed("Real", null, "tests", 42);
  private static final String SCHEMA_JSON = SchemaFormatter.getInstance("json").format(SCHEMA_REAL);
  private static final Charset[] UTF_CHARSETS = { StandardCharsets.UTF_8, StandardCharsets.UTF_16LE,
      StandardCharsets.UTF_16BE };

  @Test
  void testParseFile() throws IOException {
    Path tempFile = Files.createTempFile("TestSchemaParser", null);
    Files.write(tempFile, singletonList(SCHEMA_JSON));

    Schema schema = new SchemaParser().parse(tempFile.toFile()).mainSchema();
    assertEquals(SCHEMA_REAL, schema);
  }

  @Test
  void testParsePath() throws IOException {
    Path tempFile = Files.createTempFile("TestSchemaParser", null);
    Files.write(tempFile, singletonList(SCHEMA_JSON));

    Schema schema = new SchemaParser().parse(tempFile).mainSchema();
    assertEquals(SCHEMA_REAL, schema);
  }

  @Test
  void testParseURI() throws IOException {
    Path tempFile = Files.createTempFile("TestSchemaParser", null);
    Charset charset = UTF_CHARSETS[(int) Math.floor(UTF_CHARSETS.length * Math.random())];
    Files.write(tempFile, singletonList(SCHEMA_JSON), charset);

    Schema schema = new SchemaParser().parse(tempFile.toUri(), null).mainSchema();
    assertEquals(SCHEMA_REAL, schema);
  }

  @Test
  void testParseReader() throws IOException {
    Schema schema = new SchemaParser().parse(new StringReader(SCHEMA_JSON)).mainSchema();
    assertEquals(SCHEMA_REAL, schema);
  }

  @Test
  void testParseStream() throws IOException {
    Schema schema = new SchemaParser().parse(new ByteArrayInputStream(SCHEMA_JSON.getBytes(StandardCharsets.UTF_16)))
        .mainSchema();
    assertEquals(SCHEMA_REAL, schema);
  }

  @Test
  void testParseTextWithFallbackJsonParser() {
    Schema schema = new SchemaParser().parse(SCHEMA_JSON).mainSchema();
    assertEquals(SCHEMA_REAL, schema);
  }

  @Test
  void testParseByCustomParser() {
    SchemaParser.ParseResult parseResult = new SchemaParser().parse(DummySchemaParser.SCHEMA_TEXT_ONE);
    List<Schema> namedSchemas = parseResult.parsedNamedSchemas();
    assertEquals(1, namedSchemas.size());
    assertEquals(DummySchemaParser.FIXED_SCHEMA, namedSchemas.get(0));
    Schema schema = parseResult.mainSchema();
    assertEquals(DummySchemaParser.FIXED_SCHEMA, schema);
  }

  @Test
  void testSingleParseError() {
    SchemaParseException parseException = assertThrows(SchemaParseException.class,
        () -> new SchemaParser().parse("foo").mainSchema());
    assertEquals(JsonParseException.class, parseException.getCause().getClass());
    assertEquals(0, parseException.getSuppressed().length);
  }

  @Test
  void testMultipleParseErrors() {
    SchemaParseException parseException = assertThrows(SchemaParseException.class,
        () -> new SchemaParser().parse(DummySchemaParser.SCHEMA_TEXT_ERROR).mainSchema());
    assertTrue(parseException.getMessage().startsWith("Could not parse the schema"));
    Throwable[] suppressed = parseException.getSuppressed();
    assertEquals(2, suppressed.length);
    assertEquals(DummySchemaParser.ERROR_MESSAGE, suppressed[0].getMessage());
    assertEquals(JsonParseException.class, suppressed[1].getCause().getClass());
  }

  @Test
  void testIOFailureWhileParsingText() {
    AvroRuntimeException exception = assertThrows(AvroRuntimeException.class,
        () -> new SchemaParser().parse(DummySchemaParser.SCHEMA_TEXT_IO_ERROR).mainSchema());
    assertEquals(IOException.class, exception.getCause().getClass());
    assertEquals(DummySchemaParser.IO_ERROR_MESSAGE, exception.getCause().getMessage());
  }
}
