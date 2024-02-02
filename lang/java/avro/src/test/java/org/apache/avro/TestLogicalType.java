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

import org.hamcrest.collection.IsMapContaining;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.Callable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestLogicalType {

  @Test
  void decimalFromSchema() {
    Schema schema = Schema.createFixed("aFixed", null, null, 4);
    schema.addProp("logicalType", "decimal");
    schema.addProp("precision", 9);
    schema.addProp("scale", 2);
    LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(schema);

    assertTrue(logicalType instanceof LogicalTypes.Decimal, "Should be a Decimal");
    LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
    assertEquals(9, decimal.getPrecision(), "Should have correct precision");
    assertEquals(2, decimal.getScale(), "Should have correct scale");
  }

  @Test
  void invalidLogicalTypeIgnored() {
    final Schema schema = Schema.createFixed("aFixed", null, null, 2);
    schema.addProp("logicalType", "decimal");
    schema.addProp("precision", 9);
    schema.addProp("scale", 2);

    assertNull(LogicalTypes.fromSchemaIgnoreInvalid(schema), "Should ignore invalid logical type");
  }

  @Test
  void decimalWithNonByteArrayTypes() {
    final LogicalType decimal = LogicalTypes.decimal(5, 2);
    // test simple types
    Schema[] nonBytes = new Schema[] { Schema.createRecord("Record", null, null, false),
        Schema.createArray(Schema.create(Schema.Type.BYTES)), Schema.createMap(Schema.create(Schema.Type.BYTES)),
        Schema.createEnum("Enum", null, null, Arrays.asList("a", "b")),
        Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.BYTES), Schema.createFixed("fixed", null, null, 4))),
        Schema.create(Schema.Type.BOOLEAN), Schema.create(Schema.Type.INT), Schema.create(Schema.Type.LONG),
        Schema.create(Schema.Type.FLOAT), Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.STRING) };
    for (final Schema schema : nonBytes) {
      assertThrows("Should reject type: " + schema.getType(), IllegalArgumentException.class,
          "Logical type decimal must be backed by fixed or bytes", () -> {
            decimal.addToSchema(schema);
            return null;
          });
    }
  }

  @Test
  void unknownFromJsonNode() {
    Schema schema = Schema.create(Schema.Type.STRING);
    schema.addProp("logicalType", "unknown");
    schema.addProp("someProperty", 34);
    LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(schema);
    assertNull(logicalType, "Should not return a LogicalType instance");
  }

  @Test
  void decimalBytesHasNoPrecisionLimit() {
    Schema schema = Schema.create(Schema.Type.BYTES);
    // precision is not limited for bytes
    LogicalTypes.decimal(Integer.MAX_VALUE).addToSchema(schema);
    assertEquals(Integer.MAX_VALUE,
        ((LogicalTypes.Decimal) LogicalTypes.fromSchemaIgnoreInvalid(schema)).getPrecision(),
        "Precision should be an Integer.MAX_VALUE");
  }

  @Test
  void decimalFixedPrecisionLimit() {
    // 4 bytes can hold up to 9 digits of precision
    final Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    assertThrows("Should reject precision", IllegalArgumentException.class, "fixed(4) cannot store 10 digits (max 9)",
        () -> {
          LogicalTypes.decimal(10).addToSchema(schema);
          return null;
        });
    assertNull(LogicalTypes.fromSchemaIgnoreInvalid(schema), "Invalid logical type should not be set on schema");

    // 129 bytes can hold up to 310 digits of precision
    final Schema schema129 = Schema.createFixed("aDecimal", null, null, 129);
    assertThrows("Should reject precision", IllegalArgumentException.class,
        "fixed(129) cannot store 311 digits (max 310)", () -> {
          LogicalTypes.decimal(311).addToSchema(schema129);
          return null;
        });
    assertNull(LogicalTypes.fromSchemaIgnoreInvalid(schema129), "Invalid logical type should not be set on schema");
  }

  @Test
  void decimalFailsWithZeroPrecision() {
    final Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    assertThrows("Should reject precision", IllegalArgumentException.class,
        "Invalid decimal precision: 0 (must be positive)", () -> {
          LogicalTypes.decimal(0).addToSchema(schema);
          return null;
        });
    assertNull(LogicalTypes.fromSchemaIgnoreInvalid(schema), "Invalid logical type should not be set on schema");
  }

  @Test
  void decimalFailsWithNegativePrecision() {
    final Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    assertThrows("Should reject precision", IllegalArgumentException.class,
        "Invalid decimal precision: -9 (must be positive)", () -> {
          LogicalTypes.decimal(-9).addToSchema(schema);
          return null;
        });
    assertNull(LogicalTypes.fromSchemaIgnoreInvalid(schema), "Invalid logical type should not be set on schema");
  }

  @Test
  void decimalScaleBoundedByPrecision() {
    final Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    assertThrows("Should reject precision", IllegalArgumentException.class,
        "Invalid decimal scale: 10 (greater than precision: 9)", () -> {
          LogicalTypes.decimal(9, 10).addToSchema(schema);
          return null;
        });
    assertNull(LogicalTypes.fromSchemaIgnoreInvalid(schema), "Invalid logical type should not be set on schema");
  }

  @Test
  void decimalFailsWithNegativeScale() {
    final Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    assertThrows("Should reject precision", IllegalArgumentException.class,
        "Invalid decimal scale: -2 (must be positive)", () -> {
          LogicalTypes.decimal(9, -2).addToSchema(schema);
          return null;
        });
    assertNull(LogicalTypes.fromSchemaIgnoreInvalid(schema), "Invalid logical type should not be set on schema");
  }

  @Test
  void schemaRejectsSecondLogicalType() {
    final Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    LogicalTypes.decimal(9).addToSchema(schema);
    assertThrows("Should reject second logical type", AvroRuntimeException.class, "Can't overwrite property: scale",
        () -> {
          LogicalTypes.decimal(9, 2).addToSchema(schema);
          return null;
        });
    assertEquals(LogicalTypes.decimal(9), LogicalTypes.fromSchemaIgnoreInvalid(schema),
        "First logical type should still be set on schema");
  }

  @Test
  void decimalDefaultScale() {
    Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    // 4 bytes can hold up to 9 digits of precision
    LogicalTypes.decimal(9).addToSchema(schema);
    assertEquals(0, ((LogicalTypes.Decimal) LogicalTypes.fromSchemaIgnoreInvalid(schema)).getScale(),
        "Scale should be a 0");
  }

  @Test
  void fixedDecimalToFromJson() {
    Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    LogicalTypes.decimal(9, 2).addToSchema(schema);
    Schema parsed = new Schema.Parser().parse(schema.toString(true));
    assertEquals(schema, parsed, "Constructed and parsed schemas should match");
  }

  @Test
  void bytesDecimalToFromJson() {
    Schema schema = Schema.create(Schema.Type.BYTES);
    LogicalTypes.decimal(9, 2).addToSchema(schema);
    Schema parsed = new Schema.Parser().parse(schema.toString(true));
    assertEquals(schema, parsed, "Constructed and parsed schemas should match");
  }

  @Test
  void uuidExtendsString() {
    Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
    assertEquals(LogicalTypes.uuid(), uuidSchema.getLogicalType());

    assertThrows("UUID requires a string", IllegalArgumentException.class,
        "Uuid can only be used with an underlying string or fixed type",
        () -> LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.INT)));
  }

  @Test
  void durationExtendsFixed12() {
    Schema durationSchema = LogicalTypes.duration().addToSchema(Schema.createFixed("f", null, null, 12));
    assertEquals(LogicalTypes.duration(), durationSchema.getLogicalType());

    assertThrows("Duration requires a fixed(12)", IllegalArgumentException.class,
        "Duration can only be used with an underlying fixed type of size 12.",
        () -> LogicalTypes.duration().addToSchema(Schema.create(Schema.Type.INT)));

    assertThrows("Duration requires a fixed(12)", IllegalArgumentException.class,
        "Duration can only be used with an underlying fixed type of size 12.",
        () -> LogicalTypes.duration().addToSchema(Schema.createFixed("wrong", null, null, 42)));
  }

  @Test
  void logicalTypeEquals() {
    LogicalTypes.Decimal decimal90 = LogicalTypes.decimal(9);
    LogicalTypes.Decimal decimal80 = LogicalTypes.decimal(8);
    LogicalTypes.Decimal decimal92 = LogicalTypes.decimal(9, 2);

    assertEqualsTrue("Same decimal", LogicalTypes.decimal(9, 0), decimal90);
    assertEqualsTrue("Same decimal", LogicalTypes.decimal(8, 0), decimal80);
    assertEqualsTrue("Same decimal", LogicalTypes.decimal(9, 2), decimal92);
    assertEqualsFalse("Different logical type", LogicalTypes.uuid(), decimal90);
    assertEqualsFalse("Different precision", decimal90, decimal80);
    assertEqualsFalse("Different scale", decimal90, decimal92);
  }

  @Test
  void logicalTypeInSchemaEquals() {
    Schema schema1 = Schema.createFixed("aDecimal", null, null, 4);
    Schema schema2 = Schema.createFixed("aDecimal", null, null, 4);
    Schema schema3 = Schema.createFixed("aDecimal", null, null, 4);
    assertNotSame(schema1, schema2);
    assertNotSame(schema1, schema3);
    assertEqualsTrue("No logical types", schema1, schema2);
    assertEqualsTrue("No logical types", schema1, schema3);

    LogicalTypes.decimal(9).addToSchema(schema1);
    assertEqualsFalse("Two has no logical type", schema1, schema2);

    LogicalTypes.decimal(9).addToSchema(schema2);
    assertEqualsTrue("Same logical types", schema1, schema2);

    LogicalTypes.decimal(9, 2).addToSchema(schema3);
    assertEqualsFalse("Different logical type", schema1, schema3);
  }

  @Test
  void registerLogicalTypeThrowsIfTypeNameNotProvided() {
    assertThrows("Should error if type name was not provided", UnsupportedOperationException.class,
        "LogicalTypeFactory TypeName has not been provided", () -> {
          LogicalTypes.register(schema -> LogicalTypes.date());
          return null;
        });
  }

  @Test
  void registerLogicalTypeWithName() {
    final LogicalTypes.LogicalTypeFactory factory = new LogicalTypes.LogicalTypeFactory() {
      @Override
      public LogicalType fromSchema(Schema schema) {
        return LogicalTypes.date();
      }

      @Override
      public String getTypeName() {
        return "typename";
      }
    };

    LogicalTypes.register("registered", factory);

    assertThat(LogicalTypes.getCustomRegisteredTypes(), IsMapContaining.hasEntry("registered", factory));
  }

  @Test
  void registerLogicalTypeWithFactoryName() {
    final LogicalTypes.LogicalTypeFactory factory = new LogicalTypes.LogicalTypeFactory() {
      @Override
      public LogicalType fromSchema(Schema schema) {
        return LogicalTypes.date();
      }

      @Override
      public String getTypeName() {
        return "factory";
      }
    };

    LogicalTypes.register(factory);

    assertThat(LogicalTypes.getCustomRegisteredTypes(), IsMapContaining.hasEntry("factory", factory));
  }

  @Test
  void registerLogicalTypeWithFactoryNameNotProvided() {
    final LogicalTypes.LogicalTypeFactory factory = schema -> LogicalTypes.date();

    LogicalTypes.register("logicalTypeName", factory);

    assertThat(LogicalTypes.getCustomRegisteredTypes(), IsMapContaining.hasEntry("logicalTypeName", factory));
  }

  @Test
  public void testRegisterLogicalTypeFactoryByServiceLoader() {
    assertThat(LogicalTypes.getCustomRegisteredTypes(),
        IsMapContaining.hasEntry(equalTo("custom"), instanceOf(LogicalTypes.LogicalTypeFactory.class)));
  }

  public static void assertEqualsTrue(String message, Object o1, Object o2) {
    assertEquals(o1, o2, "Should be equal (forward): " + message);
    assertEquals(o2, o1, "Should be equal (reverse): " + message);
  }

  public static void assertEqualsFalse(String message, Object o1, Object o2) {
    assertNotEquals(o1, o2, "Should be equal (forward): " + message);
    assertNotEquals(o2, o1, "Should be equal (reverse): " + message);
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   *
   * @param message            A String message to describe this assertion
   * @param expected           An Exception class that the Runnable should throw
   * @param containedInMessage A String that should be contained by the thrown
   *                           exception's message
   * @param callable           A Callable that is expected to throw the exception
   */
  public static void assertThrows(String message, Class<? extends Exception> expected, String containedInMessage,
      Callable<?> callable) {
    try {
      callable.call();
      fail("No exception was thrown (" + message + "), expected: " + expected.getName());
    } catch (Exception actual) {
      assertEquals(expected, actual.getClass(), message);
      assertTrue(actual.getMessage().contains(containedInMessage),
          "Expected exception message (" + containedInMessage + ") missing: " + actual.getMessage());
    }
  }
}
