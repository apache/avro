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
package org.apache.avro.compiler.idl;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestLogicalTypes {
  private Schema logicalTypeFields;

  @BeforeEach
  public void setup() throws ParseException {
    final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Idl idl = new Idl(cl.getResourceAsStream("logicalTypes.avdl"), "UTF-8");
    Protocol protocol = idl.CompilationUnit();

    logicalTypeFields = protocol.getType("org.apache.avro.test.LogicalTypeFields");
  }

  @Test
  void dateBecomesLogicalType() {
    assertEquals(LogicalTypes.date(), logicalTypeOfField("aDate"));
  }

  @Test
  void timeMsBecomesLogicalType() {
    assertEquals(LogicalTypes.timeMillis(), logicalTypeOfField("aTime"));
  }

  @Test
  void timestampMsBecomesLogicalType() {
    assertEquals(LogicalTypes.timestampMillis(), logicalTypeOfField("aTimestamp"));
  }

  @Test
  void localTimestampMsBecomesLogicalType() {
    assertEquals(LogicalTypes.localTimestampMillis(), logicalTypeOfField("aLocalTimestamp"));
  }

  @Test
  void decimalBecomesLogicalType() {
    assertEquals(LogicalTypes.decimal(6, 2), logicalTypeOfField("pocketMoney"));
  }

  @Test
  void uuidBecomesLogicalType() {
    assertEquals(LogicalTypes.uuid(), logicalTypeOfField("identifier"));
  }

  @Test
  void annotatedLongBecomesLogicalType() {
    assertEquals(LogicalTypes.timestampMicros(), logicalTypeOfField("anotherTimestamp"));
  }

  @Test
  void annotatedBytesFieldBecomesLogicalType() {
    assertEquals(LogicalTypes.decimal(6, 2), logicalTypeOfField("allowance"));
  }

  @Test
  void incorrectlyAnnotatedBytesFieldHasNoLogicalType() {
    Schema fieldSchema = logicalTypeFields.getField("byteArray").schema();

    assertNull(fieldSchema.getLogicalType());
    assertEquals("decimal", fieldSchema.getObjectProp("logicalType"));
    assertEquals(3000000000L, fieldSchema.getObjectProp("precision")); // Not an int, so not a valid precision
    assertEquals(0, fieldSchema.getObjectProp("scale"));
  }

  private LogicalType logicalTypeOfField(String name) {
    return logicalTypeFields.getField(name).schema().getLogicalType();
  }
}
