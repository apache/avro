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

import org.apache.avro.Schema;
import org.apache.avro.SchemaFormatter;
import org.apache.avro.SchemaParser;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

class IdlSchemaFormatterFactoryTest {
  @Test
  void verifyIdlFormatting() throws IOException {
    SchemaFormatter idlFormatter = SchemaFormatter.getInstance("idl");
    assertEquals(IdlSchemaFormatter.class, idlFormatter.getClass());

    String formattedHappyFlowSchema = getResourceAsString("../util/idl_utils_test_schema.avdl");

    String schemaResourceName = "../util/idl_utils_test_schema.avdl";
    try (InputStream stream = getClass().getResourceAsStream(schemaResourceName)) {
      Schema happyFlowSchema = new SchemaParser().parse(formattedHappyFlowSchema).mainSchema();
      // The Avro project indents .avdl files less than common
      String formatted = idlFormatter.format(happyFlowSchema).replaceAll("    ", "\t").replaceAll("\t", "  ");
      assertEquals(formattedHappyFlowSchema, formatted);
    }
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
}
