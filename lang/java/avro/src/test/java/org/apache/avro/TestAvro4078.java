package org.apache.avro;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import org.apache.avro.specific.SpecificData;
import org.junit.Test;

/**
 * Unit test for demonstrating specific data potential issue.
 */
public class TestAvro4078 {

  public static final Schema FULLNAME_SCHEMA = (new Schema.Parser()).parse("{\n" + "     \"type\": \"record\",\n"
      + "     \"namespace\": \"org.apache.avro\",\n" + "     \"name\": \"FullName\",\n" + "     \"fields\": [\n"
      + "       { \"name\": \"first\", \"type\": \"string\" },\n"
      + "       { \"name\": \"last\", \"type\": \"string\" }\n" + "     ]\n" + "}");

  @Test
  public void testClassLoad() {
    System.err.println(FULLNAME_SCHEMA);
//    assertNotNull(SpecificData.get().getClass(FULLNAME_SCHEMA));
    assertSame(FullName.class, SpecificData.get().getClass(FULLNAME_SCHEMA));
  }

  public static void main(String[] args) {
    assertSame(FullName.class, SpecificData.get().getClass(FULLNAME_SCHEMA));
  }
}
