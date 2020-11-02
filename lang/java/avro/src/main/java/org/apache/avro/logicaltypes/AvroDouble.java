package org.apache.avro.logicaltypes;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper of the Avro Type.DOUBLE, a 64 bit IEEE 754 floating-point number.
 *
 */
public class AvroDouble implements AvroPrimitive {
  public static final String NAME = "DOUBLE";
  private static AvroDouble element = new AvroDouble();
  private static Schema schema = Schema.create(Type.DOUBLE);

  public AvroDouble() {
    super();
  }

  public static AvroDouble create() {
    return element;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    return 1;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Double convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Double) {
      return (Double) value;
    } else if (value instanceof String) {
      try {
        return Double.valueOf((String) value);
      } catch (NumberFormatException e) {
        throw new AvroTypeException("Cannot convert the string \"" + value + "\" into a Double");
      }
    } else if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Double");
  }

  @Override
  public Double convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Double) {
      return (Double) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Double");
  }

  @Override
  public void toString(StringBuffer b, Object value) {
    if (value != null) {
      b.append(value.toString());
    }
  }

  @Override
  public Type getBackingType() {
    return Type.DOUBLE;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVRODOUBLE;
  }

  @Override
  public Class<?> getConvertedType() {
    return Double.class;
  }

}
