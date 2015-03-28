package org.apache.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;

public class LogicalType {

  public static final String LOGICAL_TYPE_PROP = "logicalType";

  private static final String[] INCOMPATIBLE_PROPS = new String[] {
      GenericData.STRING_PROP, SpecificData.CLASS_PROP,
      SpecificData.KEY_CLASS_PROP, SpecificData.ELEMENT_PROP
  };

  public static LogicalType fromSchema(Schema schema) {
    String typeName = schema.getProp(LOGICAL_TYPE_PROP);

    LogicalType logicalType;
    if ("decimal".equals(typeName)) {
      logicalType = new Decimal(schema);
    } else if ("uuid".equals(typeName)) {
      logicalType = UUID_TYPE;
    } else {
      return null;
    }

    // make sure the type is valid before returning it
    try {
      logicalType.validate(schema);
    } catch (RuntimeException e) {
      // ignore invalid types
      return null;
    }

    return logicalType;
  }

  /** Create a Decimal LogicalType with the given precision and scale 0 */
  public static Decimal decimal(int precision) {
    return decimal(precision, 0);
  }

  /** Create a Decimal LogicalType with the given precision and scale */
  public static Decimal decimal(int precision, int scale) {
    return new Decimal(precision, scale);
  }

  public static final LogicalType UUID_TYPE = new LogicalType("uuid");

  public static LogicalType uuid() {
    return UUID_TYPE;
  }

  private final String name;

  protected LogicalType(String logicalTypeName) {
    this.name = logicalTypeName;
  }

  public String getName() {
    return name;
  }

  /** Validate this logical type for the given Schema and add its props */
  public Schema addToSchema(Schema schema) {
    validate(schema);
    schema.addProp(LOGICAL_TYPE_PROP, name);
    return schema;
  }

  public void validate(Schema schema) {
    for (String incompatible : INCOMPATIBLE_PROPS) {
      if (schema.getProp(incompatible) != null) {
        throw new IllegalArgumentException(
            LOGICAL_TYPE_PROP + " cannot be used with " + incompatible);
      }
    }
  }

  /** Decimal represents arbitrary-precision fixed-scale decimal numbers  */
  public static class Decimal extends LogicalType {
    private static final String PRECISION_PROP = "precision";
    private static final String SCALE_PROP = "scale";

    private final int precision;
    private final int scale;

    private Decimal(int precision, int scale) {
      super("decimal");
      this.precision = precision;
      this.scale = scale;
    }

    private Decimal(Schema schema) {
      super("decimal");
      this.precision = getInt(schema, PRECISION_PROP);
      this.scale = getInt(schema, SCALE_PROP);
    }

    @Override
    public Schema addToSchema(Schema schema) {
      super.addToSchema(schema);
      schema.addProp(PRECISION_PROP, precision);
      schema.addProp(SCALE_PROP, scale);
      return schema;
    }

    public int getPrecision() {
      return precision;
    }

    public int getScale() {
      return scale;
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      // validate the type
      if (schema.getType() != Schema.Type.FIXED &&
          schema.getType() != Schema.Type.BYTES) {
        throw new IllegalArgumentException(
            "Logical type DECIMAL must be backed by fixed or bytes");
      }
      if (precision <= 0) {
        throw new IllegalArgumentException("Invalid DECIMAL precision: " +
            precision + " (must be positive)");
      } else if (precision > maxPrecision(schema)) {
        throw new IllegalArgumentException(
            "fixed(" + schema.getFixedSize() + ") cannot store " +
                precision + " digits (max " + maxPrecision(schema) + ")");
      }
      if (scale < 0) {
        throw new IllegalArgumentException("Invalid DECIMAL scale: " +
            scale + " (must be positive)");
      } else if (scale > precision) {
        throw new IllegalArgumentException("Invalid DECIMAL scale: " +
            scale + " (greater than precision: " + precision + ")");
      }
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

    private int getInt(Schema schema, String name) {
      Object obj = schema.getObjectProp(name);
      if (obj instanceof Integer) {
        return (Integer) obj;
      }
      throw new IllegalArgumentException("Expected int " + name + ": " +
          (obj == null ? "null" : obj + ":" + obj.getClass().getSimpleName()));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Decimal decimal = (Decimal) o;

      if (precision != decimal.precision) return false;
      if (scale != decimal.scale) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = precision;
      result = 31 * result + scale;
      return result;
    }
  }
}
