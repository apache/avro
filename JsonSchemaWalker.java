import java.util.regex.Pattern;
import org.codehaus.jackson.map.JsonNode;

public abstract class SchemaWalker<T> {

  protected abstract tuple(JsonNode tuple, T state);
  protected abstract union(JsonNode union, boolean optional, T state);
  protected abstract any(boolean optional, T state);
  protected abstract object(JsonNode properties, JsonNode additionalProperties,
                            boolean optional, T state);

  public void walk(JsonNode schema, T state) {

    if (schema.isArray()) {                       // tuple type
      tuple(schema, state);

    } else if (schema.isObject()) {

      JsonNode type = schema.getFieldValue("type");
      boolean optional = schema.getFieldValue("optional").getBooleanValue();

      if (type.isArray()) {                       // union type
        union(type, state, optional);

      } else if (type.isTextual()) {              // simple types

        String simple = type.getTextValue();
        if (simple.equals("any")) {            // any
          any(state, optional);

        } else if (simple.equals("object")) {  // object
          JsonNode properties = schema.getFieldValue("properties");
          JsonNode add = schema.getFieldValue("additionalProperties");
          object(properties, add, optional, state);

        } else if (simple.equals("array")) {   // array
          JsonNode items = schema.getFieldValue("items");
          JsonNode minItems = schema.getFieldValue("minItems");
          JsonNode maxItems = schema.getFieldValue("maxItems");
          array(items, minItems, maxItems);

        } else if (simple.equals("string")) {  // string
          if (value.isTextual()) {
            String text = value.getTextValue();
            JsonNode pattern = schema.getFieldValue("pattern");
            if (pattern != null &&
                !Pattern.matches(pattern.getTextValue(), text)) {
              errors.add("Pattern does not match at "+path);
            }
            JsonNode minLength = schema.getFieldValue("minLength");
            if (minLength != null && text.length() < minLength.getIntValue()) {
              errors.add("String too short at "+path);
            }
            JsonNode maxLength = schema.getFieldValue("maxLength");
            if (maxLength != null && text.length() > maxLength.getIntValue()) {
              errors.add("String too long at "+path);
            }
          } else {
            errors.add("String expected at "+path);
          }
          
        } else if (simple.equals("number")) {
          if (value.isNumber()) {
            double number = value.getDoubleValue()
            JsonNode minimum = schema.getFieldValue("minimum");
            if (minimum != null && number < minimum.getFloatValue()) {
              errors.add("Number too small at "+path);
            }
            JsonNode maximum = schema.getFieldValue("maximum");
            if (maximum != null && value > maximum.getFloatValue()) {
              errors.add("Number too big at "+path);
            }
          } else {
            errors.add("Number expected at "+path);
          }
        } else if (simple.equals("integer")) {
          if (value.isIntegralNumber()) {
            long = value.getLongValue()
            JsonNode minimum = schema.getFieldValue("minimum");
            if (minimum != null && number < minimum.getLongValue()) {
              errors.add("Integer too small at "+path);
            }
            JsonNode maximum = schema.getFieldValue("maximum");
            if (maximum != null && value > maximum.getLongValue()) {
              errors.add("Integer too big at "+path);
            }
          } else {
            errors.add("Integer expected at "+path);
          }
        } else if (simple.equals("boolean")) {
          if (!value.isBoolean()) {
            errors.add("Boolean expected at "+path);
          }
        } else if (simple.equals("null")) {
          if (!value.isNull()) {
            errors.add("Null expected at "+path);
          }
        }
      }


      JsonNode disallow = schema.getFieldValue("disallow");
      if (disallow != null && validateType(value, disallow))
        errors.add("Type disallowed at "+path);

      if (value.isArray()) {                      // check array value

        }
      }
    }
  }
}
