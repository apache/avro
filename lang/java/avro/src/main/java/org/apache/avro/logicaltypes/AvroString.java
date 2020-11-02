package org.apache.avro.logicaltypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper around the Avro Type.STRING data type
 *
 */
public class AvroString implements AvroPrimitive {
  private static Schema schema = Schema.create(Type.STRING);
  public static final String NAME = "STRING";
  private static AvroString element = new AvroString();

  public AvroString() {
    super();
  }

  public static AvroString create() {
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
  public void toString(StringBuffer b, Object value) {
    if (value != null) {
      b.append('\"');
      b.append(AvroType.encodeJson(value.toString()));
      b.append('\"');
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
    return AvroType.AVROSTRING;
  }

  @Override
  public Class<?> getConvertedType() {
    return CharSequence.class;
  }

}
