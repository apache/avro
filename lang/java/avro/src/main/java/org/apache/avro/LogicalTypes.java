package org.apache.avro;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.util.WeakIdentityHashMap;

public class LogicalTypes {

  private static final Map<Schema, LogicalType> CACHE =
      new WeakIdentityHashMap<Schema, LogicalType>();

  public interface LogicalTypeFactory {
    LogicalType fromSchema(Schema schema);
  }

  private static final Map<String, LogicalTypeFactory> REGISTERED_TYPES =
      new ConcurrentHashMap<String, LogicalTypeFactory>();

  public static void register(String logicalTypeName, LogicalTypeFactory factory) {
    if (logicalTypeName == null) {
      throw new NullPointerException("Invalid logical type name: null");
    }
    if (factory == null) {
      throw new NullPointerException("Invalid logical type factory: null");
    }
    REGISTERED_TYPES.put(logicalTypeName, factory);
  }

  /**
   * Returns the {@link LogicalType} from the schema, if one is present.
   * @param schema
   * @return
   */
  public static LogicalType fromSchema(Schema schema) {
    return fromSchemaImpl(schema, true);
  }

  public static LogicalType fromSchemaIgnoreInvalid(Schema schema) {
    if (CACHE.containsKey(schema)) {
      return CACHE.get(schema);
    }

    LogicalType logicalType = fromSchemaImpl(schema, false);

    // add to the cache, even if it is null
    CACHE.put(schema, logicalType);

    return logicalType;
  }

  private static LogicalType fromSchemaImpl(Schema schema, boolean throwErrors) {
    String typeName = schema.getProp(LogicalType.LOGICAL_TYPE_PROP);

    LogicalType logicalType;
    try {
      if ("decimal".equals(typeName)) {
        logicalType = new Decimal(schema);
      } else if ("uuid".equals(typeName)) {
        logicalType = UUID_TYPE;
      } else if (REGISTERED_TYPES.containsKey(typeName)) {
        logicalType = REGISTERED_TYPES.get(typeName).fromSchema(schema);
      } else {
        logicalType = null;
      }

      // make sure the type is valid before returning it
      if (logicalType != null) {
        logicalType.validate(schema);
      }
    } catch (RuntimeException e) {
      if (throwErrors) {
        throw e;
      }
      // ignore invalid types
      logicalType = null;
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

  private static final LogicalType UUID_TYPE = new LogicalType("uuid");

  public static LogicalType uuid() {
    return UUID_TYPE;
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
      if (!hasProperty(schema, PRECISION_PROP)) {
        throw new IllegalArgumentException(
            "Invalid decimal: missing precision");
      }

      this.precision = getInt(schema, PRECISION_PROP);

      if (hasProperty(schema, SCALE_PROP)) {
        this.scale = getInt(schema, SCALE_PROP);
      } else {
        this.scale = 0;
      }
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
            "Logical type decimal must be backed by fixed or bytes");
      }
      if (precision <= 0) {
        throw new IllegalArgumentException("Invalid decimal precision: " +
            precision + " (must be positive)");
      } else if (precision > maxPrecision(schema)) {
        throw new IllegalArgumentException(
            "fixed(" + schema.getFixedSize() + ") cannot store " +
                precision + " digits (max " + maxPrecision(schema) + ")");
      }
      if (scale < 0) {
        throw new IllegalArgumentException("Invalid decimal scale: " +
            scale + " (must be positive)");
      } else if (scale > precision) {
        throw new IllegalArgumentException("Invalid decimal scale: " +
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

    private boolean hasProperty(Schema schema, String name) {
      return (schema.getObjectProp(name) != null);
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
