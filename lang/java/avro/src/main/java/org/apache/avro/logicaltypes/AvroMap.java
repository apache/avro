package org.apache.avro.logicaltypes;

import java.util.Map;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper around the Avro Type.MAP data type
 *
 */
public class AvroMap implements AvroPrimitive {
  public static final String NAME = "MAP";
  private static AvroMap element = new AvroMap();

  public static AvroMap create() {
    return element;
  }

  public AvroMap() {
    super();
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
  public Map<?, ?> convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Map) {
      return (Map<?, ?>) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a GenericFixed");
  }

  @Override
  public Map<?, ?> convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Map) {
      return (Map<?, ?>) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a GenericFixed");
  }

  @Override
  public void toString(StringBuffer b, Object value) {
    if (value != null) {
      b.append('\"');
      b.append(value.toString());
      b.append('\"');
    }
  }

  @Override
  public Type getBackingType() {
    return Type.MAP;
  }

  @Override
  public Schema getRecommendedSchema() {
    return null;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROMAP;
  }

  @Override
  public Class<?> getConvertedType() {
    return Map.class;
  }

}
