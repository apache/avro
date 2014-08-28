package org.apache.avro;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;
import org.junit.Assert;
import org.junit.Test;

public class TestLogicalType {

  @Test
  public void testDecimalFromJsonNode() {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put("logicalType", TextNode.valueOf("decimal"));
    node.put("precision", IntNode.valueOf(9));
    node.put("scale", IntNode.valueOf(2));
    LogicalType decimal = LogicalType.fromJsonNode("decimal", node);
    Assert.assertTrue("Should be a Decimal",
        decimal instanceof LogicalType.Decimal);
    Assert.assertEquals("Should have correct precision",
        IntNode.valueOf(9), decimal.getJsonProp("precision"));
    Assert.assertEquals("Should have correct scale",
        IntNode.valueOf(2), decimal.getJsonProp("scale"));
  }

  @Test
  public void testDecimalWithNonByteArrayTypes() {
    final LogicalType decimal = LogicalType.decimal(5, 2);
    // test simple types
    Schema[] nonBytes = new Schema[] {
        Schema.createRecord("Record", null, null, false),
        Schema.createArray(Schema.create(Schema.Type.BYTES)),
        Schema.createMap(Schema.create(Schema.Type.BYTES)),
        Schema.createEnum("Enum", null, null, Arrays.asList("a", "b")),
        Schema.createUnion(Arrays.asList(
            Schema.create(Schema.Type.BYTES),
            Schema.createFixed("fixed", null, null, 4))),
        Schema.create(Schema.Type.BOOLEAN), Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.FLOAT),
        Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.STRING) };
    for (final Schema schema : nonBytes) {
      assertThrows("Should reject type: " + schema.getType(),
          IllegalArgumentException.class,
          "DECIMAL must be backed by fixed or bytes", new Callable() {
            @Override
            public Object call() throws Exception {
              schema.setLogicalType(decimal);
              return null;
            }
          });
    }
  }

  @Test
  public void testUnknownFromJsonNode() {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put("logicalType", TextNode.valueOf("unknown"));
    node.put("someProperty", IntNode.valueOf(34));
    LogicalType logicalType = LogicalType.fromJsonNode("unknown", node);
    Assert.assertNull("Should not return a LogicalType instance", logicalType);
  }

  @Test
  public void testDecimalBytesHasNoPrecisionLimit() {
    Schema schema = Schema.create(Schema.Type.BYTES);
    // precision is not limited for bytes
    schema.setLogicalType(LogicalType.decimal(Integer.MAX_VALUE));
    Assert.assertEquals("Precision should be an IntNode(Integer.MAX_VALUE)",
        new IntNode(Integer.MAX_VALUE),
        schema.getLogicalType().getJsonProp("precision"));
  }

  @Test
  public void testDecimalFixedPrecisionLimit() {
    // 4 bytes can hold up to 9 digits of precision
    final Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    assertThrows("Should reject precision", IllegalArgumentException.class,
        "fixed(4) cannot store 10 digits (max 9)", new Callable() {
          @Override
          public Object call() throws Exception {
            schema.setLogicalType(LogicalType.decimal(10));
            return null;
          }
        }
    );
    Assert.assertNull("Invalid logical type should not be set on schema",
        schema.getLogicalType());
  }

  @Test
  public void testDecimalFailsWithZeroPrecision() {
    final Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    assertThrows("Should reject precision", IllegalArgumentException.class,
        "Invalid DECIMAL precision: 0 (must be positive)", new Callable() {
          @Override
          public Object call() throws Exception {
            schema.setLogicalType(LogicalType.decimal(0));
            return null;
          }
        });
    Assert.assertNull("Invalid logical type should not be set on schema",
        schema.getLogicalType());
  }

  @Test
  public void testDecimalFailsWithNegativePrecision() {
    final Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    assertThrows("Should reject precision", IllegalArgumentException.class,
        "Invalid DECIMAL precision: -9 (must be positive)", new Callable() {
          @Override
          public Object call() throws Exception {
            schema.setLogicalType(LogicalType.decimal(-9));
            return null;
          }
        });
    Assert.assertNull("Invalid logical type should not be set on schema",
        schema.getLogicalType());
  }

  @Test
  public void testDecimalScaleBoundedByPrecision() {
    final Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    assertThrows("Should reject precision", IllegalArgumentException.class,
        "Invalid DECIMAL scale: 10 (greater than precision: 9)",
        new Callable() {
          @Override
          public Object call() throws Exception {
            schema.setLogicalType(LogicalType.decimal(9, 10));
            return null;
          }
        });
    Assert.assertNull("Invalid logical type should not be set on schema",
        schema.getLogicalType());
  }

  @Test
  public void testDecimalFailsWithNegativeScale() {
    final Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    assertThrows("Should reject precision", IllegalArgumentException.class,
        "Invalid DECIMAL scale: -2 (must be positive)", new Callable() {
          @Override
          public Object call() throws Exception {
            schema.setLogicalType(LogicalType.decimal(9, -2));
            return null;
          }
        });
    Assert.assertNull("Invalid logical type should not be set on schema",
        schema.getLogicalType());
  }

  @Test
  public void testSchemaRejectsSecondLogicalType() {
    final Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    schema.setLogicalType(LogicalType.decimal(9));
    assertThrows("Should reject second logical type",
        IllegalArgumentException.class,
        "Cannot replace existing logical type", new Callable() {
          @Override
          public Object call() throws Exception {
            schema.setLogicalType(LogicalType.decimal(9, 2));
            return null;
          }
        }
    );
    Assert.assertEquals("First logical type should still be set on schema",
        LogicalType.decimal(9), schema.getLogicalType());
  }

  @Test
  public void testDecimalDefaultScale() {
    Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    // 4 bytes can hold up to 9 digits of precision
    schema.setLogicalType(LogicalType.decimal(9));
    Assert.assertEquals("Scale should be an IntNode(0)",
        IntNode.valueOf(0),
        schema.getLogicalType().getJsonProp("scale"));
  }

  @Test
  public void testFixedDecimalToFromJson() {
    Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    schema.setLogicalType(LogicalType.decimal(9, 2));
    Schema parsed = new Schema.Parser().parse(schema.toString(true));
    Assert.assertEquals("Constructed and parsed schemas should match",
        schema, parsed);
  }

  @Test
  public void testBytesDecimalToFromJson() {
    Schema schema = Schema.create(Schema.Type.BYTES);
    schema.setLogicalType(LogicalType.decimal(9, 2));
    Schema parsed = new Schema.Parser().parse(schema.toString(true));
    Assert.assertEquals("Constructed and parsed schemas should match",
        schema, parsed);
  }

  @Test
  public void testLogicalTypeEquals() {
    LogicalType unknown = new LogicalType(new HashSet<String>(), "unknown") {
      @Override
      public void validate(Schema schema) {}

      @Override
      public Set<String> reserved() { return new HashSet<String>(); }
    };

    LogicalType.Decimal decimal90 = LogicalType.decimal(9);
    LogicalType.Decimal decimal80 = LogicalType.decimal(8);
    LogicalType.Decimal decimal92 = LogicalType.decimal(9, 2);

    assertEqualsTrue("Same decimal", LogicalType.decimal(9, 0), decimal90);
    assertEqualsTrue("Same decimal", LogicalType.decimal(8, 0), decimal80);
    assertEqualsTrue("Same decimal", LogicalType.decimal(9, 2), decimal92);
    assertEqualsFalse("Different logical type", unknown, decimal90);
    assertEqualsFalse("Different precision", decimal90, decimal80);
    assertEqualsFalse("Different scale", decimal90, decimal92);
  }

  @Test
  public void testLogicalTypeInSchemaEquals() {
    Schema schema1 = Schema.createFixed("aDecimal", null, null, 4);
    Schema schema2 = Schema.createFixed("aDecimal", null, null, 4);
    Schema schema3 = Schema.createFixed("aDecimal", null, null, 4);
    Assert.assertNotSame(schema1, schema2);
    Assert.assertNotSame(schema1, schema3);
    assertEqualsTrue("No logical types", schema1, schema2);
    assertEqualsTrue("No logical types", schema1, schema3);

    schema1.setLogicalType(LogicalType.decimal(9));
    assertEqualsFalse("Two has no logical type", schema1, schema2);

    schema2.setLogicalType(LogicalType.decimal(9));
    assertEqualsTrue("Same logical types", schema1, schema2);

    schema3.setLogicalType(LogicalType.decimal(9, 2));
    assertEqualsFalse("Different logical type", schema1, schema3);
  }


  public static void assertEqualsTrue(String message, Object o1, Object o2) {
    Assert.assertTrue("Should be equal (forward): " + message, o1.equals(o2));
    Assert.assertTrue("Should be equal (reverse): " + message, o2.equals(o1));
  }

  public static void assertEqualsFalse(String message, Object o1, Object o2) {
    Assert.assertFalse("Should be equal (forward): " + message, o1.equals(o2));
    Assert.assertFalse("Should be equal (reverse): " + message, o2.equals(o1));
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param containedInMessage A String that should be contained by the thrown
   *                           exception's message
   * @param callable A Callable that is expected to throw the exception
   */
  public static void assertThrows(String message,
                                  Class<? extends Exception> expected,
                                  String containedInMessage,
                                  Callable callable) {
    try {
      callable.call();
      Assert.fail("No exception was thrown (" + message + "), expected: " +
          expected.getName());
    } catch (Exception actual) {
      Assert.assertEquals(message, expected, actual.getClass());
      Assert.assertTrue(
          "Expected exception message (" + containedInMessage + ") missing: " +
              actual.getMessage(),
          actual.getMessage().contains(containedInMessage)
      );
    }
  }
}
