package org.apache.avro.logicaltypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Type.STRING backed representation of geospatial data in WKT format.
 *
 */
public class AvroSTPoint extends LogicalType implements AvroPrimitive {
  public static final String NAME = "ST_POINT";
  public static final String TYPENAME = NAME;
  private static AvroSTPoint element = new AvroSTPoint();
  private static Schema schema;

  static {
    schema = element.addToSchema(Schema.create(Type.STRING));
  }

  public AvroSTPoint() {
    super(TYPENAME);
  }

  public static AvroSTPoint create() {
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
    return AvroType.AVROSTPOINT;
  }

  @Override
  public Class<?> getConvertedType() {
    return CharSequence.class;
  }

}
