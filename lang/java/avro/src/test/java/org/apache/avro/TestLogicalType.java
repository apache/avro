package org.apache.avro;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.avro.logicalTypes.Decimal;
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
    LogicalType decimal = AbstractLogicalType.fromJsonNode(node, Schema.Type.STRING);
    Assert.assertTrue("Should be a Decimal",
        decimal instanceof Decimal);
    Assert.assertEquals("Should have correct precision",
        9, decimal.getProperty("precision"));
    Assert.assertEquals("Should have correct scale",
        2, decimal.getProperty("scale"));
  }

  @Test
  public void testDecimalWithNonByteArrayTypes() {
    final LogicalType decimal = new Decimal(5, 2, Schema.Type.STRING);
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
        Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL)};
    for (final Schema schema : nonBytes) {
      assertThrows("Should reject type: " + schema.getType(),
          IllegalArgumentException.class,
          "decimal must be backed by fixed or bytes", new Callable() {
            @Override
            public Object call() throws Exception {
              schema.setLogicalType(decimal);
              return null;
            }
          });
    }
  }

  @Test(expected = RuntimeException.class)
  public void testUnknownFromJsonNode() {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put("logicalType", TextNode.valueOf("unknown"));
    node.put("someProperty", IntNode.valueOf(34));
    LogicalType logicalType = AbstractLogicalType.fromJsonNode(node, Schema.Type.STRING);
    Assert.assertNull("Should not return a LogicalType instance", logicalType);
  }

  @Test
  public void testDecimalBytesHasNoPrecisionLimit() {
    Schema schema = Schema.create(Schema.Type.BYTES);
    // precision is not limited for bytes
    schema.setLogicalType(new Decimal(Integer.MAX_VALUE, 0, Schema.Type.STRING));
    Assert.assertEquals("Precision should be an IntNode(Integer.MAX_VALUE)",
        Integer.MAX_VALUE,
        schema.getLogicalType().getProperty("precision"));
  }

  @Test
  public void testDecimalFixedPrecisionLimit() {
    // 4 bytes can hold up to 9 digits of precision
    final Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    assertThrows("Should reject precision", IllegalArgumentException.class,
        "fixed(4) cannot store 10 digits (max 9)", new Callable() {
          @Override
          public Object call() throws Exception {
            schema.setLogicalType(new Decimal(10, 0, Schema.Type.STRING));
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
        "Invalid decimal precision: 0 (must be positive)", new Callable() {
          @Override
          public Object call() throws Exception {
            schema.setLogicalType(new Decimal(0, 0, Schema.Type.STRING));
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
        "Invalid decimal precision: -9 (must be positive)", new Callable() {
          @Override
          public Object call() throws Exception {
            schema.setLogicalType(new Decimal(-9, 0, Schema.Type.STRING));
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
        "Invalid decimal scale: 10 (greater than precision: 9)",
        new Callable() {
          @Override
          public Object call() throws Exception {
            schema.setLogicalType(new Decimal(9, 10, Schema.Type.STRING));
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
        "Invalid decimal scale: -2 (must be positive)", new Callable() {
          @Override
          public Object call() throws Exception {
            schema.setLogicalType(new Decimal(9, -2, Schema.Type.STRING));
            return null;
          }
        });
    Assert.assertNull("Invalid logical type should not be set on schema",
        schema.getLogicalType());
  }

  @Test
  public void testSchemaRejectsSecondLogicalType() {
    final Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    schema.setLogicalType(new Decimal(9, 0, Schema.Type.STRING));
    assertThrows("Should reject second logical type",
        IllegalArgumentException.class,
        "Cannot replace existing logical type", new Callable() {
          @Override
          public Object call() throws Exception {
            schema.setLogicalType(new Decimal(9, 2, Schema.Type.STRING));
            return null;
          }
        }
    );
    Assert.assertEquals("First logical type should still be set on schema",
        new Decimal(9, 0, Schema.Type.STRING), schema.getLogicalType());
  }

  @Test
  public void testDecimalDefaultScale() {
    Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    // 4 bytes can hold up to 9 digits of precision
    schema.setLogicalType(new Decimal(9, 0, Schema.Type.STRING));
    Assert.assertEquals("Scale should be an IntNode(0)",
        0,
        schema.getLogicalType().getProperty("scale"));
  }

  @Test
  public void testFixedDecimalToFromJson() {
    Schema schema = Schema.createFixed("aDecimal", null, null, 4);
    schema.setLogicalType(new Decimal(9, 2, Schema.Type.STRING));
    Schema parsed = new Schema.Parser().parse(schema.toString(true));
    Assert.assertEquals("Constructed and parsed schemas should match",
        schema, parsed);
  }

  @Test
  public void testBytesDecimalToFromJson() {
    Schema schema = Schema.create(Schema.Type.BYTES);
    schema.setLogicalType(new Decimal(9, 2, Schema.Type.STRING));
    Schema parsed = new Schema.Parser().parse(schema.toString(true));
    Assert.assertEquals("Constructed and parsed schemas should match",
        schema, parsed);
  }

  @Test
  public void testLogicalTypeEquals() {
    LogicalType unknown = new AbstractLogicalType(Schema.Type.STRING,
            new HashSet<String>(), "unknown", Collections.EMPTY_MAP) {
      @Override
      public void validate(Schema schema) {}

      @Override
      public Set<String> reserved() { return new HashSet<String>(); }

        @Override
        public Class<?> getLogicalJavaType() {
            throw new UnsupportedOperationException("Not supported yet."); 
        }

        @Override
        public Object deserialize(Object object) {
            throw new UnsupportedOperationException("Not supported yet."); 
        }

        @Override
        public Object serialize(Object object) {
            throw new UnsupportedOperationException("Not supported yet."); 
        }
    };

    Decimal decimal90 = new Decimal(9, 0, Schema.Type.STRING);
    Decimal decimal80 = new Decimal(8, 0, Schema.Type.STRING);
    Decimal decimal92 = new Decimal(9, 2, Schema.Type.STRING);

    assertEqualsTrue("Same decimal", new Decimal(9, 0, Schema.Type.STRING), decimal90);
    assertEqualsTrue("Same decimal", new Decimal(8, 0, Schema.Type.STRING), decimal80);
    assertEqualsTrue("Same decimal", new Decimal(9, 2, Schema.Type.STRING), decimal92);
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

    schema1.setLogicalType(new Decimal(9, 0, Schema.Type.STRING));
    assertEqualsFalse("Two has no logical type", schema1, schema2);

    schema2.setLogicalType(new Decimal(9, 0, Schema.Type.STRING));
    assertEqualsTrue("Same logical types", schema1, schema2);

    schema3.setLogicalType(new Decimal(9, 2, Schema.Type.STRING));
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
