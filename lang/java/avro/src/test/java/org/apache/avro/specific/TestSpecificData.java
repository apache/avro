package org.apache.avro.specific;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.Before;
import org.junit.Test;

/*
 * If integerClass is primitive, reflection to find method will
 * result in a NoSuchMethodException in the case of a UNION schema
 */
public class TestSpecificData {

  private Class<?> intClass;
  private Class<?> integerClass;

  @Before
  public void setUp() {
    Schema intSchema = Schema.create(Type.INT);
    intClass = SpecificData.get().getClass(intSchema);
    Schema nullSchema = Schema.create(Type.NULL);
    Schema nullIntUnionSchema = Schema.createUnion(Arrays.asList(nullSchema, intSchema));
    integerClass = SpecificData.get().getClass(nullIntUnionSchema);
  }

  @Test
  public void testClassTypes() {
    assertTrue(intClass.isPrimitive());
    assertFalse(integerClass.isPrimitive());
  }

  @Test
  public void testPrimitiveParam() throws Exception {
    assertNotNull(Reflection.class.getMethod("primitive", intClass));
  }

  @Test(expected = NoSuchMethodException.class)
  public void testPrimitiveParamError() throws Exception {
    Reflection.class.getMethod("primitiveWrapper", intClass);
  }

  @Test
  public void testPrimitiveWrapperParam() throws Exception {
    assertNotNull(Reflection.class.getMethod("primitiveWrapper", integerClass));
  }

  @Test(expected = NoSuchMethodException.class)
  public void testPrimitiveWrapperParamError() throws Exception {
    Reflection.class.getMethod("primitive", integerClass);
  }

  static class Reflection {
    public void primitive(int i) {}
    public void primitiveWrapper(Integer i) {}
  }
}
