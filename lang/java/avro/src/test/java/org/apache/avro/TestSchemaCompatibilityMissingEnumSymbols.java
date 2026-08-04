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

import org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.avro.TestSchemaCompatibility.validateIncompatibleSchemas;
import static org.apache.avro.TestSchemas.ENUM1_ABC_SCHEMA;
import static org.apache.avro.TestSchemas.ENUM1_AB_SCHEMA;
import static org.apache.avro.TestSchemas.ENUM1_BC_SCHEMA;

public class TestSchemaCompatibilityMissingEnumSymbols {

  private static final Schema RECORD1_WITH_ENUM_AB = SchemaBuilder.record("Record1").fields() //
      .name("field1").type(ENUM1_AB_SCHEMA).noDefault() //
      .endRecord();
  private static final Schema RECORD1_WITH_ENUM_ABC = SchemaBuilder.record("Record1").fields() //
      .name("field1").type(ENUM1_ABC_SCHEMA).noDefault() //
      .endRecord();

  public static Stream<Arguments> data() {
    return Stream.of(Arguments.of(ENUM1_AB_SCHEMA, ENUM1_ABC_SCHEMA, "[C]", "/symbols"),
        Arguments.of(ENUM1_BC_SCHEMA, ENUM1_ABC_SCHEMA, "[A]", "/symbols"),
        Arguments.of(RECORD1_WITH_ENUM_AB, RECORD1_WITH_ENUM_ABC, "[C]", "/fields/0/type/symbols"));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testTypeMismatchSchemas(Schema reader, Schema writer, String details, String location) {
    validateIncompatibleSchemas(reader, writer, SchemaIncompatibilityType.MISSING_ENUM_SYMBOLS, details, location);
  }
}
