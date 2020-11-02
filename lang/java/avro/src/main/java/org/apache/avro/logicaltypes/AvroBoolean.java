package org.apache.avro.logicaltypes;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper for the Avro BOOLEAN type.
 */
public class AvroBoolean implements AvroPrimitive {
  public static final String NAME = "BOOLEAN";
  private static AvroBoolean element = new AvroBoolean();
  private static Schema schema = Schema.create(Type.BOOLEAN);

  public AvroBoolean() {
    super();
  }

  public static AvroBoolean create() {
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
  public Boolean convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof String) {
      if ("TRUE".equalsIgnoreCase((String) value)) {
        return Boolean.TRUE;
      } else if ("FALSE".equalsIgnoreCase((String) value)) {
        return Boolean.FALSE;
      }
    } else if (value instanceof Number) {
      int v = ((Number) value).intValue();
      if (v == 1) {
        return Boolean.TRUE;
      } else if (v == 0) {
        return Boolean.FALSE;
      }
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Boolean");
  }

  @Override
  public Boolean convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Boolean) {
      return (Boolean) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Boolean");
  }

  @Override
  public void toString(StringBuffer b, Object value) {
    if (value != null) {
      b.append(value.toString());
    }
  }

  @Override
  public Type getBackingType() {
    return Type.BOOLEAN;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROBOOLEAN;
  }

  @Override
  public Class<?> getConvertedType() {
    return Boolean.class;
  }

}
