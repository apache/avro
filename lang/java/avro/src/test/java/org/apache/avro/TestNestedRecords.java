package org.apache.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * This test demonstrates the fix for a complex nested schema type.
 */
public class TestNestedRecords {


  @Test
  public void testSingleSubRecord() throws IOException {

    final Schema child = SchemaBuilder.record("Child")
            .namespace("org.apache.avro.nested")
            .fields()
            .requiredString("childField").endRecord();


    final Schema parent = SchemaBuilder.record("Parent")
            .namespace("org.apache.avro.nested")
            .fields()
            .requiredString("parentField1")
            .name("child1").type(child).noDefault()
            .requiredString("parentField2").endRecord();



    final String inputAsExpected = "{\n" +
            " \"parentField1\": \"parentValue1\",\n" +
            " \"child1\":{\n" +
            "    \"childField\":\"childValue1\"\n" +
            " },\n" +
            " \"parentField2\":\"parentValue2\"\n" +
            "}";


    final ByteArrayInputStream inputStream = new ByteArrayInputStream(inputAsExpected.getBytes());

    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(parent, inputStream);
    final DatumReader<Object> reader = new GenericDatumReader<Object>(parent);

    final GenericData.Record  decoded = (GenericData.Record) reader.read(null, decoder);


    assertThat(decoded.get("parentField1").toString(), equalTo("parentValue1"));
    assertThat(decoded.get("parentField2").toString(), equalTo("parentValue2"));

    assertThat(((GenericData.Record)decoded.get("child1")).get("childField").toString(), equalTo("childValue1"));

  }



  @Test
  public void testSingleSubRecordExtraField() throws IOException {

    final Schema child = SchemaBuilder.record("Child")
            .namespace("org.apache.avro.nested")
            .fields()
            .requiredString("childField").endRecord();


    final Schema parent = SchemaBuilder.record("Parent")
            .namespace("org.apache.avro.nested")
            .fields()
            .requiredString("parentField1")
            .name("child1").type(child).noDefault()
            .requiredString("parentField2").endRecord();


    final String inputAsExpected = "{\n" +
            " \"parentField1\": \"parentValue1\",\n" +
            " \"child1\":{\n" +
            "    \"childField\":\"childValue1\",\n" +

            //this field should be safely ignored
            "    \"extraField\":\"extraValue\"\n" +
            " },\n" +
            " \"parentField2\":\"parentValue2\"\n" +
            "}";


    final ByteArrayInputStream inputStream = new ByteArrayInputStream(inputAsExpected.getBytes());

    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(parent, inputStream);
    final DatumReader<Object> reader = new GenericDatumReader<Object>(parent);

    final GenericData.Record decoded = (GenericData.Record) reader.read(null, decoder);

    assertThat(decoded.get("parentField1").toString(), equalTo("parentValue1"));
    assertThat(decoded.get("parentField2").toString(), equalTo("parentValue2"));

    assertThat(((GenericData.Record)decoded.get("child1")).get("childField").toString(), equalTo("childValue1"));


  }

}
