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

import org.apache.avro.util.SchemaResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParseContextTest {
  Schema fooRecord, fooRecordCopy, barEnum, bazFixed, mehRecord;
  ParseContext fooBarBaz;

  @BeforeEach
  public void setUp() throws Exception {
    fooRecord = SchemaBuilder.record("ns.Foo").fields().endRecord();
    fooRecordCopy = SchemaBuilder.record("ns.Foo").fields().endRecord();
    barEnum = SchemaBuilder.enumeration("ns.Bar").symbols();
    bazFixed = SchemaBuilder.fixed("ns.Baz").size(8);
    mehRecord = SchemaBuilder.record("ns.Meh").fields().endRecord();

    fooBarBaz = new ParseContext();
    fooBarBaz.put(fooRecord);
    fooBarBaz.put(barEnum);
    fooBarBaz.put(bazFixed);
  }

  @Test
  public void checkNewNameContextContainsPrimitives() {
    EnumSet<Schema.Type> complexTypes = EnumSet.of(Schema.Type.RECORD, Schema.Type.ENUM, Schema.Type.FIXED,
        Schema.Type.UNION, Schema.Type.ARRAY, Schema.Type.MAP);
    EnumSet<Schema.Type> primitives = EnumSet.complementOf(complexTypes);

    ParseContext context = new ParseContext();
    for (Schema.Type type : complexTypes) {
      assertFalse(context.contains(type.getName()));
    }
    for (Schema.Type type : primitives) {
      assertTrue(context.contains(type.getName()));
    }
  }

  @Test
  public void primitivesAreNotCached() {
    EnumSet<Schema.Type> primitives = EnumSet.complementOf(EnumSet.of(Schema.Type.RECORD, Schema.Type.ENUM,
        Schema.Type.FIXED, Schema.Type.UNION, Schema.Type.ARRAY, Schema.Type.MAP));

    ParseContext context = new ParseContext();
    for (Schema.Type type : primitives) {
      Schema first = context.resolve(type.getName());
      Schema second = context.resolve(type.getName());
      assertEquals(first, second);
      assertNotSame(first, second);

      first.addProp("logicalType", "brick");
      assertNotEquals(first, second);
    }
  }

  @Test
  public void validateSchemaRetrievalFailure() {
    Schema unknown = Schema.createFixed("unknown", null, null, 0);

    Schema unresolved = fooBarBaz.resolve("unknown");
    assertTrue(SchemaResolver.isUnresolvedSchema(unresolved));
    assertEquals(unknown.getFullName(), SchemaResolver.getUnresolvedSchemaName(unresolved));
  }

  @Test
  public void validateSchemaRetrievalByFullName() {
    assertSame(fooRecord, fooBarBaz.resolve(fooRecord.getFullName()));
  }

  @Test
  public void verifyPutIsIdempotent() {
    ParseContext context = new ParseContext();
    assertNotEquals(fooRecord, context.resolve(fooRecord.getFullName()));

    context.put(fooRecord);
    assertEquals(fooRecord, context.resolve(fooRecord.getFullName()));

    context.put(fooRecord);
    assertEquals(fooRecord, context.resolve(fooRecord.getFullName()));
  }

  @Test
  public void verifyPutOnlyAcceptsNamedSchemas() {
    ParseContext context = new ParseContext();
    assertThrows(AvroRuntimeException.class, () -> context.put(Schema.create(Schema.Type.STRING)));
  }

  @Test
  public void verifyAddDoesNotAllowChangingSchemas() {
    Schema fooEnum = SchemaBuilder.enumeration("ns.Foo").symbols();

    ParseContext context = new ParseContext();
    context.put(fooRecord);
    assertThrows(AvroRuntimeException.class, () -> context.put(fooEnum));
  }
}
