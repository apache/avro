package org.apache.avro.logicaltypes;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Based on an INT but is supposed to hold data from -127 to +128 only. A single
 * signed byte.
 *
 */

public class AvroByte extends LogicalType implements AvroPrimitive {
  public static final String NAME = "BYTE";
  public static final String TYPENAME = NAME;
  private static AvroByte element = new AvroByte();
  private static Schema schema;

  static {
    schema = element.addToSchema(Schema.create(Type.INT));
  }

  private AvroByte() {
    super(TYPENAME);
  }

  public static AvroByte create() {
    return element;
  }

  @Override
  public Schema addToSchema(Schema schema) {
    return super.addToSchema(schema);
  }

  @Override
  public void validate(Schema schema) {
    super.validate(schema);
    // validate the type
    if (schema.getType() != Schema.Type.INT) {
      throw new IllegalArgumentException("Logical type must be backed by an integer");
    }
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
  public void toString(StringBuffer b, Object value) {
    if (value != null) {
      b.append(value.toString());
    }
  }

  @Override
  public Integer convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Byte");
  }

  @Override
  public Type getBackingType() {
    return Type.INT;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROBYTE;
  }

  @Override
  public Byte convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Byte) {
      return (Byte) value;
    } else if (value instanceof Integer) {
      return ((Integer) value).byteValue();
    } else if (value instanceof Number) {
      return ((Number) value).byteValue();
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Byte");
  }

  @Override
  public Class<?> getConvertedType() {
    return Integer.class;
  }

}
