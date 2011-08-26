package org.apache.avro.data;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for RecordBuilderBase.
 */
public class RecordBuilderBaseTest {
  private static Set<Type> primitives;
  private static Set<Type> nonNullPrimitives;
  
  @BeforeClass()
  public static void setUpBeforeClass() {
    primitives = new HashSet<Type>(Arrays.asList(Type.values()));
    primitives.removeAll(Arrays.asList(new Type[] { 
        Type.RECORD, Type.ENUM, Type.ARRAY, Type.MAP, Type.UNION, Type.FIXED 
    }));
    
    nonNullPrimitives = new HashSet<Type>(primitives);
    nonNullPrimitives.remove(Type.NULL);
  }
  
  @Test
  public void testIsValidValueWithPrimitives() { 
    // Verify that a non-null value is valid for all primitives:
    for (Type type : primitives) {
      Field f = new Field("f", Schema.create(type), null, null);
      Assert.assertTrue(RecordBuilderBase.isValidValue(f, new Object()));
    }
    
    // Verify that null is not valid for all non-null primitives:
    for (Type type : nonNullPrimitives) {
      Field f = new Field("f", Schema.create(type), null, null);
      Assert.assertFalse(RecordBuilderBase.isValidValue(f, null));
    }
  }
  
  @Test
  public void testIsValidValueWithNullField() {
    // Verify that null is a valid value for null fields:
    Assert.assertTrue(RecordBuilderBase.isValidValue(
        new Field("f", Schema.create(Type.NULL), null, null), null));
  }
  
  @Test
  public void testIsValidValueWithUnion() {
    // Verify that null values are not valid for a union with no null type:
    Schema unionWithoutNull = Schema.createUnion(Arrays.asList(new Schema[] { 
        Schema.create(Type.STRING), Schema.create(Type.BOOLEAN)
    }));
    
    Assert.assertTrue(RecordBuilderBase.isValidValue(
        new Field("f", unionWithoutNull, null, null), new Object()));
    Assert.assertFalse(RecordBuilderBase.isValidValue(
        new Field("f", unionWithoutNull, null, null), null));
    
    // Verify that null values are valid for a union with a null type:
    Schema unionWithNull = Schema.createUnion(Arrays.asList(new Schema[] { 
        Schema.create(Type.STRING), Schema.create(Type.NULL)
    }));
    
    Assert.assertTrue(RecordBuilderBase.isValidValue(
        new Field("f", unionWithNull, null, null), new Object()));
    Assert.assertTrue(RecordBuilderBase.isValidValue(
        new Field("f", unionWithNull, null, null), null));
  }
}
