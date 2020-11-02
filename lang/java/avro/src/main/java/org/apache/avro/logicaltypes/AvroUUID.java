package org.apache.avro.logicaltypes;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Stand-in for LogicalTypes.uuid()
 *
 */
public class AvroUUID extends LogicalType implements AvroPrimitive {
  private static Schema schema;
  private static AvroUUID element = new AvroUUID();
  public static final String NAME = "UUID";
  public static final String TYPENAME = LogicalTypes.UUID;

  static {
    schema = element.addToSchema(Schema.create(Type.STRING));
  }

  public AvroUUID() {
    super("uuid");
  }

  public static AvroUUID create() {
    return element;
  }

  @Override
  public Schema addToSchema(Schema schema) {
    super.addToSchema(schema);
    return schema;
  }

  @Override
  public void validate(Schema schema) {
    super.validate(schema);
    // validate the type
    if (schema.getType() != Schema.Type.STRING) {
      throw new IllegalArgumentException("Logical type must be backed by a string");
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
      b.append('\"');
      b.append(value.toString());
      b.append('\"');
    }
  }

  @Override
  public CharSequence convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof CharSequence) {
      return (CharSequence) value;
    } else {
      return value.toString();
    }
  }

  @Override
  public CharSequence convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof CharSequence) {
      return (CharSequence) value;
    } else {
      return value.toString();
    }
  }

  @Override
  public Type getBackingType() {
    return Type.STRING;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROUUID;
  }

  @Override
  public Class<?> getConvertedType() {
    return CharSequence.class;
  }

}
