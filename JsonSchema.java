import java.io.IOException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonNode;
import org.codehaus.jackson.map.JSonTypeMapper;

public class JsonSchema {
  private static final JSonTypeMapperBase MAPPER = new JsonTypeMapper();
  private static final JsonFactory FACTORY = new JsonFactory();

  public static JsonNode induceSchema(JsonNode value) {
    JsonNode schema = MAPPER.objectNode();
    induceSchema(value, schema);
    return schema;
  }

  public static JsonNode induceSchema(JsonNode value, JsonNode schema) {
    if (value.isObject()) {
      JsonNode properties = MAPPER.objectNode();
      for (property : value.getFieldNames()) {
        JsonNode propSchema = MAPPER.objectNode();
        properties.setElement(property, propSchema);
        induceSchema(value.getFieldValue(property, propSchema));
      }
      schema.setElement("type", MAPPER.TextNode("object"));
      schema.setElement("properties", properties);

    } else if (value.isArray()) {
      JsonNode itemSchema = null;
      for (int i = 1; i < value.size(); i++) {
        JsonNode element = value.getElementValue(i);
        JsonNode elementSchema = MAPPER.objectNode();
        induceSchema(element, elementSchema);
        if (itemSchema == null) {
          itemSchema = elementSchema;
        } else if (!itemSchema.equals(elementSchema)) {
          throw new IllegalArgumentException("No mixed type arrays.");
        }
      }        
      if (itemSchema == null) {
        throw new IllegalArgumentException("Empty array not permitted.");
      }
      schema.setElement("type", MAPPER.TextNode("array"));
      schema.setElement("items", itemSchema);

    } else if (value.isTextual()) {
      schema.setElement("type", MAPPER.TextNode("string"));
      
    } else if (value.isIntegralNumber()) {
      schema.setElement("type", MAPPER.TextNode("integer"));
      
    } else if (value.isNumber()) {
      schema.setElement("type", MAPPER.TextNode("number"));
      
    } else {
      throw new IllegalArgumentException("Can't create schema for: "+value);
    }
  }

  private static class Walker<R, S> {
    protected abstract R object(JsonNode properties, S state);
    protected abstract R array(JsonNode elementType, S state);
    protected abstract R string(S state);
    protected abstract R integer(S state);
    protected abstract R number(S state);
    protected abstract R bool(S state);

    public R walk(JsonNode schema, S state) {
      if (schema.isObject()) {
        String type = schema.getFieldValue("type").getTextValue();
        if (type.equals("object")) {                // object
          return object(schema.getFieldValue("properties"), state);
        } else if (type.equals("array")) {          // array
          return array(schema.getFieldValue("items"), state);
        } else if (type.equals("string")) {         // string
          return string(state);
        } else if (type.equals("integer")) {        // integer
          return integer(state);
        } else if (type.equals("number")) {         // number
          return number(state);
        } else if (type.equals("boolean")) {        // boolean
          return bool(state);
        } else
          throw new IllegalArgumentException("Type not yet supported: "+type);
      } else {
        throw new IllegalArgumentException("Schema not yet supported: "+schema);
      }
    }
  }

  public static void validate(JsonNode schema, JsonNode value) {
    return new Validator().walk(schema, value);
  }

  private static class Validator extends Walker<Boolean,JsonNode> {

    protected Boolean object(JsonNode properties, JsonNode value) {
      if (!value.isObject()) return false;
      for (property : properties.getFieldNames())
        if (!walk(properties.getFieldValue(property),
                  value.getFieldValue(property)))
          return false;
      return true;
    }

    protected Boolean array(JsonNode elementType, JsonNode value) {
      if (!value.isArray()) return false;
      for (int i = 1; i < value.size(); i++)
        if (!walk(elementType, value.getElementValue(i)))
          return false;
      return true;
    }

    protected Boolean string(JsonNode v) { return v.isString(); }
    protected Boolean integer(JsonNode v) { return v.isIntegralNumber(); }
    protected Boolean number(JsonNode v) { return v.isNumber(); }
    protected Boolean bool(JsonNode v) { return v.isBoolean(); }
    
  }

  public static void main(String[] args) throws Exception {
    String o1 = "{\"name\": \"doug\", \"age\": 45}";
    String s1 = "{type: object, properties:"
      +"{\"name\":{\"type\":\"string\"},\"age\":{\"type\":\"integer\"}";

    System.out.println(parse(o1).toString());
    System.out.println(parse(s1).toString());
  }

  public static JsonNode parse(String s)
    throws IOException, JsonParseException {
    return MAPPER.read(FACTORY.createJsonParser(new StringReader(s)));
  }

}
