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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.core.JsonParseException;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSchemaParser {
  private static final String SCHEMA_ONE_TEXT = DummyOneSchemaParser.SCHEMA_TEXT_ONE;
  private static final String SCHEMA_ERROR_TEXT = DummyOneSchemaParser.SCHEMA_TEXT_ERROR;
  private static final Schema SCHEMA_ONE = DummyOneSchemaParser.FIXED_SCHEMA;
  private static final Schema SCHEMA_REAL = Schema.createFixed("Real", null, "tests", 42);;
  private static final String SCHEMA_JSON = SCHEMA_REAL.toString(false);
  private Path tempFile;

  @Test
  void testParseFile() throws IOException {
    Path tempFile = Files.createTempFile("TestSchemaParser", null);
    Files.write(tempFile, singletonList(SCHEMA_JSON));

    Schema schema = new SchemaParser().parse(tempFile.toFile());
    assertEquals(SCHEMA_REAL, schema);
  }

  @Test
  void testParsePath() throws IOException {
    Path tempFile = Files.createTempFile("TestSchemaParser", null);
    Files.write(tempFile, singletonList(SCHEMA_JSON));

    Schema schema = new SchemaParser().parse(tempFile);
    assertEquals(SCHEMA_REAL, schema);
  }

  @Test
  void testParseReader() throws IOException {
    Schema schema = new SchemaParser().parse(new StringReader(SCHEMA_JSON));
    assertEquals(SCHEMA_REAL, schema);
  }

  @Test
  void testParseStream() throws IOException {
    Schema schema = new SchemaParser().parse(new ByteArrayInputStream(SCHEMA_JSON.getBytes(StandardCharsets.UTF_16)));
    assertEquals(SCHEMA_REAL, schema);
  }

  @Test
  void testParseTextWithFallbackJsonParser() {
    Schema schema = new SchemaParser().parse(SCHEMA_JSON);
    assertEquals(SCHEMA_REAL, schema);
  }

  @Test
  void testParseByCustomParser() {
    Schema schema = new SchemaParser().parse(SCHEMA_ONE_TEXT);
    assertEquals(SCHEMA_ONE, schema);
  }

  @Test
  void testSingleParseError() {
    SchemaParseException parseException = assertThrows(SchemaParseException.class,
        () -> new SchemaParser().parse("foo"));
    assertEquals(JsonParseException.class, parseException.getCause().getClass());
    assertEquals(0, parseException.getSuppressed().length);
  }

  @Test
  void testMultipleParseErrors() {
    SchemaParseException parseException = assertThrows(SchemaParseException.class,
        () -> new SchemaParser().parse(DummyOneSchemaParser.SCHEMA_TEXT_ERROR));
    assertTrue(parseException.getMessage().startsWith("Could not parse the schema"));
    Throwable[] suppressed = parseException.getSuppressed();
    assertEquals(2, suppressed.length);
    assertEquals(DummyOneSchemaParser.ERROR_MESSAGE, suppressed[0].getMessage());
    assertEquals(JsonParseException.class, suppressed[1].getCause().getClass());
  }

  @Test
  void testIOFailureWhileParsingText() {
    AvroRuntimeException exception = assertThrows(AvroRuntimeException.class,
        () -> new SchemaParser().parse(DummyOneSchemaParser.SCHEMA_TEXT_IO_ERROR));
    assertEquals(IOException.class, exception.getCause().getClass());
    assertEquals(DummyOneSchemaParser.IO_ERROR_MESSAGE, exception.getCause().getMessage());
  }
}
