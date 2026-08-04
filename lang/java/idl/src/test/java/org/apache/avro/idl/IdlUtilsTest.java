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
package org.apache.avro.idl;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class IdlUtilsTest {
  @Test
  public void idlUtilsUtilitiesThrowRuntimeExceptionsOnProgrammerError() {
    assertThrows(IllegalStateException.class, () -> IdlUtils.getField(Object.class, "noSuchField"), "Programmer error");
    assertThrows(IllegalStateException.class,
        () -> IdlUtils.getFieldValue(String.class.getDeclaredField("value"), "anything"), "Programmer error");

    assertEquals("foo", IdlUtils.uncheckExceptions(() -> "foo"));
    assertThrows(IllegalArgumentException.class, () -> IdlUtils.uncheckExceptions(() -> {
      throw new IllegalArgumentException("Oops");
    }), "Oops");
    assertThrows(AvroRuntimeException.class, () -> IdlUtils.uncheckExceptions(() -> {
      throw new IOException("Oops");
    }), "Oops");
  }

  @Test
  public void validateHappyFlowForProtocol() throws IOException {
    Protocol protocol = parseIdlResource("idl_utils_test_protocol.avdl").getProtocol();

    StringWriter buffer = new StringWriter();
    IdlUtils.writeIdlProtocol(buffer, protocol);

    assertEquals(getResourceAsString("idl_utils_test_protocol.avdl"), buffer.toString());
  }

  private IdlFile parseIdlResource(String name) throws IOException {
    IdlFile idlFile;
    IdlReader idlReader = new IdlReader();
    try (InputStream stream = getClass().getResourceAsStream(name)) {
      idlFile = idlReader.parse(requireNonNull(stream));
    }
    return idlFile;
  }

  private String getResourceAsString(String name) throws IOException {
    StringWriter schemaBuffer = new StringWriter();
    try (InputStreamReader reader = new InputStreamReader(requireNonNull(getClass().getResourceAsStream(name)))) {
      char[] buf = new char[1024];
      int charsRead;
      while ((charsRead = reader.read(buf)) > -1) {
        schemaBuffer.write(buf, 0, charsRead);
      }
    }
    return schemaBuffer.toString();
  }

  @Test
  public void validateHappyFlowForSingleSchema() throws IOException {
    final IdlFile idlFile = parseIdlResource("idl_utils_test_schema.avdl");
    Schema mainSchema = idlFile.getMainSchema();

    StringWriter buffer = new StringWriter();
    IdlUtils.writeIdlSchema(buffer, mainSchema.getTypes().iterator().next());

    assertEquals(getResourceAsString("idl_utils_test_schema.avdl"), buffer.toString());
  }

  @Test
  public void cannotWriteProtocolWithUnnamedTypes() {
    assertThrows(AvroRuntimeException.class,
        () -> IdlUtils.writeIdlProtocol(new StringWriter(), Schema.create(Schema.Type.STRING)));
  }

  @Test
  public void cannotWriteEmptyEnums() {
    assertThrows(AvroRuntimeException.class,
        () -> IdlUtils.writeIdlProtocol(new StringWriter(), Schema.createEnum("Single", null, "naming", emptyList())));
  }

  @Test
  public void cannotWriteEmptyUnionTypes() {
    assertThrows(AvroRuntimeException.class,
        () -> IdlUtils.writeIdlProtocol(new StringWriter(), Schema.createRecord("Single", null, "naming", false,
            singletonList(new Schema.Field("field", Schema.createUnion())))));
  }

  @Test
  public void validateNullToJson() throws IOException {
    assertEquals("null", callToJson(JsonProperties.NULL_VALUE));
  }

  @Test
  public void validateMapToJson() throws IOException {
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("key", "name");
    data.put("value", 81763);
    assertEquals("{\"key\":\"name\",\"value\":81763}", callToJson(data));
  }

  @Test
  public void validateCollectionToJson() throws IOException {
    assertEquals("[123,\"abc\"]", callToJson(Arrays.asList(123, "abc")));
  }

  @Test
  public void validateBytesToJson() throws IOException {
    assertEquals("\"getalletjes\"", callToJson("getalletjes".getBytes(StandardCharsets.US_ASCII)));
  }

  @Test
  public void validateStringToJson() throws IOException {
    assertEquals("\"foo\"", callToJson("foo"));
  }

  @Test
  public void validateEnumToJson() throws IOException {
    assertEquals("\"FILE_NOT_FOUND\"", callToJson(SingleValue.FILE_NOT_FOUND));
  }

  @Test
  public void validateDoubleToJson() throws IOException {
    assertEquals("25000.025", callToJson(25_000.025));
  }

  @Test
  public void validateFloatToJson() throws IOException {
    assertEquals("15000.002", callToJson(15_000.002f));
  }

  @Test
  public void validateLongToJson() throws IOException {
    assertEquals("7254378234", callToJson(7254378234L));
  }

  @Test
  public void validateIntegerToJson() throws IOException {
    assertEquals("123", callToJson(123));
  }

  @Test
  public void validateBooleanToJson() throws IOException {
    assertEquals("true", callToJson(true));
  }

  @Test
  public void validateUnknownCannotBeWrittenAsJson() {
    assertThrows(AvroRuntimeException.class, () -> callToJson(new Object()));
  }

  private String callToJson(Object datum) throws IOException {
    StringWriter buffer = new StringWriter();
    try (JsonGenerator generator = IdlUtils.MAPPER.createGenerator(buffer)) {
      IdlUtils.MAPPER.writeValueAsString(datum);
    }
    return buffer.toString();
  }

  private enum SingleValue {
    FILE_NOT_FOUND
  }
}
