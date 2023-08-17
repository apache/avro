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
import static org.apache.avro.TestSchemas.A_INT_RECORD1;
import static org.apache.avro.TestSchemas.BOOLEAN_SCHEMA;
import static org.apache.avro.TestSchemas.BYTES_SCHEMA;
import static org.apache.avro.TestSchemas.DOUBLE_SCHEMA;
import static org.apache.avro.TestSchemas.ENUM2_AB_SCHEMA;
import static org.apache.avro.TestSchemas.FIXED_4_BYTES;
import static org.apache.avro.TestSchemas.FLOAT_SCHEMA;
import static org.apache.avro.TestSchemas.INT_ARRAY_SCHEMA;
import static org.apache.avro.TestSchemas.INT_FLOAT_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.INT_LIST_RECORD;
import static org.apache.avro.TestSchemas.INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.INT_MAP_SCHEMA;
import static org.apache.avro.TestSchemas.INT_SCHEMA;
import static org.apache.avro.TestSchemas.LONG_ARRAY_SCHEMA;
import static org.apache.avro.TestSchemas.LONG_LIST_RECORD;
import static org.apache.avro.TestSchemas.LONG_MAP_SCHEMA;
import static org.apache.avro.TestSchemas.LONG_SCHEMA;
import static org.apache.avro.TestSchemas.NULL_SCHEMA;
import static org.apache.avro.TestSchemas.STRING_SCHEMA;

public class TestSchemaCompatibilityTypeMismatch {

  public static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of(NULL_SCHEMA, INT_SCHEMA, "reader type: NULL not compatible with writer type: INT", "/"),
        Arguments.of(NULL_SCHEMA, LONG_SCHEMA, "reader type: NULL not compatible with writer type: LONG", "/"),

        Arguments.of(BOOLEAN_SCHEMA, INT_SCHEMA, "reader type: BOOLEAN not compatible with writer type: INT", "/"),

        Arguments.of(INT_SCHEMA, NULL_SCHEMA, "reader type: INT not compatible with writer type: NULL", "/"),
        Arguments.of(INT_SCHEMA, BOOLEAN_SCHEMA, "reader type: INT not compatible with writer type: BOOLEAN", "/"),
        Arguments.of(INT_SCHEMA, LONG_SCHEMA, "reader type: INT not compatible with writer type: LONG", "/"),
        Arguments.of(INT_SCHEMA, FLOAT_SCHEMA, "reader type: INT not compatible with writer type: FLOAT", "/"),
        Arguments.of(INT_SCHEMA, DOUBLE_SCHEMA, "reader type: INT not compatible with writer type: DOUBLE", "/"),

        Arguments.of(LONG_SCHEMA, FLOAT_SCHEMA, "reader type: LONG not compatible with writer type: FLOAT", "/"),
        Arguments.of(LONG_SCHEMA, DOUBLE_SCHEMA, "reader type: LONG not compatible with writer type: DOUBLE", "/"),

        Arguments.of(FLOAT_SCHEMA, DOUBLE_SCHEMA, "reader type: FLOAT not compatible with writer type: DOUBLE", "/"),

        Arguments.of(DOUBLE_SCHEMA, STRING_SCHEMA, "reader type: DOUBLE not compatible with writer type: STRING", "/"),

        Arguments.of(FIXED_4_BYTES, STRING_SCHEMA, "reader type: FIXED not compatible with writer type: STRING", "/"),

        Arguments.of(STRING_SCHEMA, BOOLEAN_SCHEMA, "reader type: STRING not compatible with writer type: BOOLEAN",
            "/"),
        Arguments.of(STRING_SCHEMA, INT_SCHEMA, "reader type: STRING not compatible with writer type: INT", "/"),

        Arguments.of(BYTES_SCHEMA, NULL_SCHEMA, "reader type: BYTES not compatible with writer type: NULL", "/"),
        Arguments.of(BYTES_SCHEMA, INT_SCHEMA, "reader type: BYTES not compatible with writer type: INT", "/"),

        Arguments.of(A_INT_RECORD1, INT_SCHEMA, "reader type: RECORD not compatible with writer type: INT", "/"),

        Arguments.of(INT_ARRAY_SCHEMA, LONG_ARRAY_SCHEMA, "reader type: INT not compatible with writer type: LONG",
            "/items"),
        Arguments.of(INT_MAP_SCHEMA, INT_ARRAY_SCHEMA, "reader type: MAP not compatible with writer type: ARRAY", "/"),
        Arguments.of(INT_ARRAY_SCHEMA, INT_MAP_SCHEMA, "reader type: ARRAY not compatible with writer type: MAP", "/"),
        Arguments.of(INT_MAP_SCHEMA, LONG_MAP_SCHEMA, "reader type: INT not compatible with writer type: LONG",
            "/values"),

        Arguments.of(INT_SCHEMA, ENUM2_AB_SCHEMA, "reader type: INT not compatible with writer type: ENUM", "/"),
        Arguments.of(ENUM2_AB_SCHEMA, INT_SCHEMA, "reader type: ENUM not compatible with writer type: INT", "/"),

        Arguments.of(FLOAT_SCHEMA, INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA,
            "reader type: FLOAT not compatible with writer type: DOUBLE", "/3"),
        Arguments.of(LONG_SCHEMA, INT_FLOAT_UNION_SCHEMA, "reader type: LONG not compatible with writer type: FLOAT",
            "/1"),
        Arguments.of(INT_SCHEMA, INT_FLOAT_UNION_SCHEMA, "reader type: INT not compatible with writer type: FLOAT",
            "/1"),

        Arguments.of(INT_LIST_RECORD, LONG_LIST_RECORD, "reader type: INT not compatible with writer type: LONG",
            "/fields/0/type"),

        Arguments.of(NULL_SCHEMA, INT_SCHEMA, "reader type: NULL not compatible with writer type: INT", "/"));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testTypeMismatchSchemas(Schema reader, Schema writer, String details, String location) throws Exception {
    validateIncompatibleSchemas(reader, writer, SchemaIncompatibilityType.TYPE_MISMATCH, details, location);
  }
}
