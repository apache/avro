package org.apache.avro.logicaltypes;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper of the Avro Type.FLOAT, a 32 bit IEEE 754 floating-point number.
 *
 */
public class AvroFloat implements AvroPrimitive {
  public static final String NAME = "FLOAT";
  private static AvroFloat element = new AvroFloat();
  private static Schema schema = Schema.create(Type.FLOAT);

  public AvroFloat() {
    super();
  }

  public static AvroFloat create() {
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
  public Float convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Float) {
      return (Float) value;
    } else if (value instanceof String) {
      try {
        return Float.valueOf((String) value);
      } catch (NumberFormatException e) {
        throw new AvroTypeException("Cannot convert the string \"" + value + "\" into a Float");
      }
    } else if (value instanceof Number) {
      return ((Number) value).floatValue();
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Float");
  }

  @Override
  public Float convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Float) {
      return (Float) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Float");
  }

  @Override
  public void toString(StringBuffer b, Object value) {
    if (value != null) {
      b.append(value.toString());
    }
  }

  @Override
  public Type getBackingType() {
    return Type.FLOAT;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROFLOAT;
  }

  @Override
  public Class<?> getConvertedType() {
    return Float.class;
  }

}
