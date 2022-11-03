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

import org.junit.Before;
import org.junit.Test;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.*;

public class NameContextTest {
  Schema fooRecord, fooRecordCopy, barEnum, bazFixed, mehRecord;
  NameContext fooBarBaz;

  @Before
  public void setUp() throws Exception {
    fooRecord = SchemaBuilder.record("ns.Foo").fields().endRecord();
    fooRecordCopy = SchemaBuilder.record("ns.Foo").fields().endRecord();
    barEnum = SchemaBuilder.enumeration("ns.Bar").symbols();
    bazFixed = SchemaBuilder.fixed("ns.Baz").size(8);
    mehRecord = SchemaBuilder.record("ns.Meh").fields().endRecord();

    fooBarBaz = new NameContext();
    fooBarBaz.put(fooRecord);
    fooBarBaz.put(barEnum);
    fooBarBaz.put(bazFixed);
  }

  @Test
  public void checkNewNameContextContainsPrimitives() {
    EnumSet<Schema.Type> complexTypes = EnumSet.of(Schema.Type.RECORD, Schema.Type.ENUM, Schema.Type.FIXED,
        Schema.Type.UNION, Schema.Type.ARRAY, Schema.Type.MAP);
    EnumSet<Schema.Type> primitives = EnumSet.complementOf(complexTypes);

    NameContext context = new NameContext();
    for (Schema.Type type : complexTypes) {
      assertFalse(context.contains(type.getName()));
    }
    for (Schema.Type type : primitives) {
      assertTrue(context.contains(type.getName()));
    }
  }

  @Test
  public void validateSchemaTests() {
    assertTrue(fooBarBaz.contains(fooRecord));
    assertTrue(fooBarBaz.contains(barEnum));
    assertTrue(fooBarBaz.contains(bazFixed));
    assertFalse(fooBarBaz.contains(mehRecord));

    assertTrue(fooBarBaz.contains(fooRecord.getFullName()));
    assertTrue(fooBarBaz.contains(barEnum.getFullName()));
    assertTrue(fooBarBaz.contains(bazFixed.getFullName()));
    assertFalse(fooBarBaz.contains(mehRecord.getFullName()));
  }

  @Test
  public void validateNameResolvingAgainstDefaultNamespace() {
    NameContext context = new NameContext().namespace("");
    assertEquals("string", context.resolveName("string", ""));
    assertEquals("string", context.resolveName("string", null));
    assertEquals("foo.string", context.resolveName("string", "foo"));
    assertEquals("Bar", context.resolveName("Bar", ""));
    assertEquals("Bar", context.resolveName("Bar", null));
    assertEquals("foo.Bar", context.resolveName("Bar", "foo"));
  }

  @Test
  public void validateNameResolvingAgainstSetNamespace() {
    NameContext context = new NameContext().namespace("ns");
    assertEquals("string", context.resolveName("string", ""));
    assertEquals("string", context.resolveName("string", null));
    assertEquals("foo.string", context.resolveName("string", "foo"));
    assertEquals("ns.Bar", context.resolveName("Bar", ""));
    assertEquals("ns.Bar", context.resolveName("Bar", null));
    assertEquals("foo.Bar", context.resolveName("Bar", "foo"));
  }

  @Test(expected = AvroRuntimeException.class)
  public void validateSchemaRetrievalFailure() {
    fooBarBaz.resolveWithFallback("unknown", null);
  }

  @Test
  public void validateSchemaRetrievalByFullName() {
    assertSame(fooRecord, fooBarBaz.resolveWithFallback(fooRecord.getFullName(), null));
  }

  @Test
  public void validateSchemaRetrievalByNameAndNamespace() {
    assertSame(fooRecord, fooBarBaz.resolve(fooRecord.getName(), fooRecord.getNamespace()));
  }

  @Test
  public void validateSchemaRetrievalByNameAndInheritedNamespace() {
    assertSame(fooRecord, fooBarBaz.namespace(fooRecord.getNamespace()).resolveWithFallback(fooRecord.getName(), null));
  }

  @Test
  public void verifyPutIsIdempotent() {
    NameContext context = new NameContext();
    assertFalse(context.contains(fooRecord));

    context.put(fooRecord);
    assertTrue(context.contains(fooRecord));

    context.put(fooRecord);
    assertTrue(context.contains(fooRecord));
  }

  @Test(expected = AvroRuntimeException.class)
  public void verifyPutOnlyAcceptsNamedSchemas() {
    NameContext context = new NameContext();

    context.put(Schema.create(Schema.Type.STRING));
  }

  @Test(expected = AvroRuntimeException.class)
  public void verifyAddDoesNotAllowChangingSchemas() {
    Schema fooEnum = SchemaBuilder.enumeration("ns.Foo").symbols();

    NameContext context = new NameContext();
    context.put(fooRecord);
    context.put(fooEnum);
  }
}
