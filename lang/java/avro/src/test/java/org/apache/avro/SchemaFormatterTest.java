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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SchemaFormatterTest {

  @Test
  void validateDefaultNaming() {
    assertEquals("json", new JsonSchemaFormatterFactory().formatName());
    assertThrows(AvroRuntimeException.class, () -> new Wrongly_Named_SchemaFormatterFactory().formatName());
    assertThrows(AvroRuntimeException.class, () -> new SchemaFormatterFactoryWithOddName().formatName());
  }

  @Test
  void validateJsonFormatDefaultsToPrettyPrinting() {
    Schema schema = Schema.createFixed("ns.Fixed", null, null, 16);
    assertEquals(SchemaFormatter.format("json", schema), SchemaFormatter.format("json/pretty", schema));
  }

  @Test
  void validateSupportForPrettyJsonFormat() {
    Schema schema = Schema.createFixed("ns.Fixed", null, null, 16);
    assertEquals("{\n  \"type\" : \"fixed\",\n  \"name\" : \"Fixed\",\n  \"namespace\" : \"ns\",\n  \"size\" : 16\n}",
        SchemaFormatter.format("json/pretty", schema));
  }

  @Test
  void validateSupportForInlineJsonFormat() {
    Schema schema = Schema.createFixed("ns.Fixed", null, null, 16);
    assertEquals("{\"type\":\"fixed\",\"name\":\"Fixed\",\"namespace\":\"ns\",\"size\":16}",
        SchemaFormatter.format("json/inline", schema));
  }

  @Test
  void checkThatJsonHasNoExtraVariant() {
    assertThrows(AvroRuntimeException.class, () -> SchemaFormatter.getInstance("json/extra"));
  }

  @Test
  void validateSupportForCanonicalFormat() {
    Schema schema = Schema.createFixed("Fixed", "Another test", "ns", 16);
    assertEquals("{\"name\":\"ns.Fixed\",\"type\":\"fixed\",\"size\":16}", SchemaFormatter.format("canonical", schema));
  }

  @Test
  void checkThatCanonicalFormHasNoVariants() {
    assertThrows(AvroRuntimeException.class, () -> SchemaFormatter.getInstance("canonical/foo"));
  }

  @Test
  void checkExceptionForMissingFormat() {
    assertThrows(AvroRuntimeException.class, () -> SchemaFormatter.getInstance("unknown"));
  }

  private static class Wrongly_Named_SchemaFormatterFactory implements SchemaFormatterFactory {

    @Override
    public SchemaFormatter getDefaultFormatter() {
      return null;
    }
  }

  private static class SchemaFormatterFactoryWithOddName implements SchemaFormatterFactory {
    @Override
    public SchemaFormatter getDefaultFormatter() {
      return null;
    }
  }
}
