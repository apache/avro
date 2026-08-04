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
import static org.apache.avro.TestSchemas.A_INT_B_DINT_RECORD1;
import static org.apache.avro.TestSchemas.A_INT_RECORD1;
import static org.apache.avro.TestSchemas.EMPTY_RECORD1;

public class TestSchemaCompatibilityReaderFieldMissingDefaultValue {

  public static Stream<Arguments> data() {
    return Stream.of(Arguments.of(A_INT_RECORD1, EMPTY_RECORD1, "a", "/fields/0"),
        Arguments.of(A_INT_B_DINT_RECORD1, EMPTY_RECORD1, "a", "/fields/0"));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testReaderFieldMissingDefaultValueSchemas(Schema reader, Schema writer, String details, String location) {
    validateIncompatibleSchemas(reader, writer, SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE, details,
        location);
  }
}
