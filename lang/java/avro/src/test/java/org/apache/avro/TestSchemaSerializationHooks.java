package org.apache.avro;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaSerializationHooks {

  @Test
  public void testSchemaRefResolver() throws IOException {
    Schema recSchema = SchemaBuilder.record("TestRecord")
            .prop("id", "testId")
            .fields()
            .nullableLong("number", 0L)
            .endRecord();
    Schema schema = SchemaBuilder.array()
            .items(recSchema);
    StringWriter stringWriter = new StringWriter();
    JsonGenerator jgen = Schema.FACTORY.createGenerator(stringWriter);
    schema.toJson(new TestResolver(recSchema), jgen);
    jgen.flush();
    System.out.println(stringWriter.toString());
    Schema result = new Schema.Parser(new TestResolver(recSchema)).parse(stringWriter.toString());
    System.out.println(result);
    Assert.assertEquals(schema, result);

  }

  private static class TestResolver extends Schema.Names {

    private final Schema recSchema;

    public TestResolver(Schema recSchema) {
      this.recSchema = recSchema;
    }

    public TestResolver(Schema recSchema, String space) {
      super(space);
      this.recSchema = recSchema;
    }



    public Schema customRead(Function<String, JsonNode> object) {
      JsonNode node = object.apply("$ref");
      if (node != null && "testId".equals(node.asText())) {
        return recSchema;
      }
      return null;
    }

    public boolean customWrite(Schema schema, JsonGenerator gen) throws IOException {
      String ref = schema.getProp("id");
      if (ref != null) {
        gen.writeStartObject();
        gen.writeFieldName("$ref");
        gen.writeString(ref);
        gen.writeEndObject();
        return true;
      }
      return false;
    }
  }

}
