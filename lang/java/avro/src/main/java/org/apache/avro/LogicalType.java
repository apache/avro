package org.apache.avro;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.TextNode;

public abstract class LogicalType extends JsonProperties {

  protected LogicalType(Set<String> reserved, String logicalTypeName) {
    super(reserved);
    props.put("logicalType", TextNode.valueOf(logicalTypeName.toLowerCase()));
  }

  /** Validate this logical type for the given Schema */
  public abstract void validate(Schema schema);

  /** Return the set of properties that a reserved for this type */
  public abstract Set<String> reserved();

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    LogicalType other = (LogicalType) obj;
    // equal if properties are the same
    return this.props.equals(other.props);
  }

  @Override
  public int hashCode() {
    return props.hashCode();
  }

  public static LogicalType fromJsonNode(String typeName, JsonNode node) {
    if (typeName.equals("decimal")) {
      return new Decimal(node);
    }
    return null;
  }

  /** Create a Decimal LogicalType with the given precision and scale 0 */
  public static Decimal decimal(int precision) {
    return decimal(precision, 0);
  }

  /** Create a Decimal LogicalType with the given precision and scale */
  public static Decimal decimal(int precision, int scale) {
    return new Decimal(precision, scale);
  }

  /** Decimal represents arbitrary-precision fixed-scale decimal numbers  */
  public static class Decimal extends LogicalType {
    private static final Set<String> RESERVED = reservedSet("precision", "scale");

    private Decimal(int precision, int scale) {
      super(RESERVED, "decimal");
      props.put("precision", IntNode.valueOf(precision));
      props.put("scale", IntNode.valueOf(scale));
    }

    private Decimal(JsonNode node) {
      super(RESERVED, "decimal");
      copyJsonProperties(node, "precision", "scale");
    }

    @Override
    public void validate(Schema schema) {
      // validate the type
      if (schema.getType() != Schema.Type.FIXED &&
          schema.getType() != Schema.Type.BYTES) {
        throw new IllegalArgumentException(
            "Logical type DECIMAL must be backed by fixed or bytes");
      }
      if (getJsonProp("precision") == null) {
        throw new IllegalArgumentException(
            "Precision is required for logical type DECIMAL.");
      }
      int precision = getJsonProp("precision").asInt();
      if (precision <= 0) {
        throw new IllegalArgumentException("Invalid DECIMAL precision: " +
            precision + " (must be positive)");
      } else if (precision > maxPrecision(schema)) {
        throw new IllegalArgumentException(
            "fixed(" + schema.getFixedSize() + ") cannot store " +
                precision + " digits (max " + maxPrecision(schema) + ")");
      }
      int scale = getJsonProp("scale").asInt(0);
      if (scale < 0) {
        throw new IllegalArgumentException("Invalid DECIMAL scale: " +
            scale + " (must be positive)");
      } else if (scale > precision) {
        throw new IllegalArgumentException("Invalid DECIMAL scale: " +
            scale + " (greater than precision: " + precision + ")");
      }
    }

    @Override
    public Set<String> reserved() {
      return RESERVED;
    }

    private long maxPrecision(Schema schema) {
      if (schema.getType() == Schema.Type.BYTES) {
        // not bounded
        return Integer.MAX_VALUE;
      } else if (schema.getType() == Schema.Type.FIXED) {
        int size = schema.getFixedSize();
        return Math.round(          // convert double to long
            Math.floor(Math.log10(  // number of base-10 digits
                Math.pow(2, 8 * size - 1) - 1)  // max value stored
            ));
      } else {
        // not valid for any other type
        return 0;
      }
    }
  }

  /** Helper method to copy properties from an incoming node */
  protected void copyJsonProperties(JsonNode node, String... properties) {
    for (String prop : properties) {
      if (node.has(prop)) {
        props.put(prop, node.get(prop));
      }
    }
  }

  /** Helper method to build reserved property sets */
  private static Set<String> reservedSet(String... properties) {
    Set<String> reserved = new HashSet<String>();
    reserved.add("logicalType");
    Collections.addAll(reserved, properties);
    return reserved;
  }

}
