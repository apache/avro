package org.apache.avro;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * This test demonstrates the fix for a complex nested schema type.
 */
public class TestNestedRecords {


  @Test
  public void testSubSchema() throws IOException {

    final Schema child = SchemaBuilder.record("Child")
      .namespace("org.apache.avro.nested")
      .fields()
      .requiredString("childField").endRecord();


    final Schema parent = SchemaBuilder.record("Parent")
      .namespace("org.apache.avro.nested")
      .fields()
      .requiredString("parentField1")
      .name("child1").type(child).noDefault()
      .name("child2").type(child).noDefault()
      .requiredString("parentField2").endRecord();


    final String inputAsExpected = "{\n" +
      " \"parentField1\": \"parentValue\",\n" +
      " \"child1\":{\n" +
      "    \"childField\":\"childValue\",\n" +

      //this field should be safely ignored
      "    \"extraField\":\"extraValue\"\n" +
      " },\n" +
      " \"child2\":{\n" +
      "    \"childField\":\"childValue\",\n" +

      //This field should be safely ignored
      "    \"extraField\":\"extraValue\"\n" +
      " },\n" +
      " \"parentField2\":\"parentValue2\"\n" +
      "}";


    final ByteArrayInputStream inputStream = new ByteArrayInputStream(inputAsExpected.getBytes());

    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(parent, inputStream);
    final DatumReader<Object> reader = new GenericDatumReader<Object>(parent);

    final Object decoded = reader.read(null, decoder);

//    assertThat(parent.getParentField(), equalTo("parentValue"));
//    assertThat(parent.getChild().getChildField(), equalTo("childValue"));

    System.out.println(decoded);

  }


}
