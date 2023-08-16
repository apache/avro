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
import static org.apache.avro.TestSchemas.A_DINT_B_DENUM_1_RECORD1;
import static org.apache.avro.TestSchemas.A_DINT_B_DENUM_2_RECORD1;
import static org.apache.avro.TestSchemas.EMPTY_RECORD1;
import static org.apache.avro.TestSchemas.EMPTY_RECORD2;
import static org.apache.avro.TestSchemas.ENUM1_AB_SCHEMA;
import static org.apache.avro.TestSchemas.ENUM2_AB_SCHEMA;
import static org.apache.avro.TestSchemas.FIXED_4_BYTES;

public class TestSchemaCompatibilityNameMismatch {

  private static final Schema FIXED_4_ANOTHER_NAME = Schema.createFixed("AnotherName", null, null, 4);

  public static Stream<Arguments> data() {
    return Stream.of(Arguments.of(ENUM1_AB_SCHEMA, ENUM2_AB_SCHEMA, "expected: Enum2", "/name"),
        Arguments.of(EMPTY_RECORD2, EMPTY_RECORD1, "expected: Record1", "/name"),
        Arguments.of(FIXED_4_BYTES, FIXED_4_ANOTHER_NAME, "expected: AnotherName", "/name"),
        Arguments.of(A_DINT_B_DENUM_1_RECORD1, A_DINT_B_DENUM_2_RECORD1, "expected: Enum2", "/fields/1/type/name"));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testNameMismatchSchemas(Schema reader, Schema writer, String details, String location) throws Exception {
    validateIncompatibleSchemas(reader, writer, SchemaIncompatibilityType.NAME_MISMATCH, details, location);
  }
}
