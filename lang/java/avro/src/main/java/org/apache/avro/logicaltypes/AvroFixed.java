package org.apache.avro.logicaltypes;

import java.nio.ByteBuffer;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;

/**
 * Wrapper around the Avro Type.FIXED data type
 *
 */
public class AvroFixed implements AvroPrimitive {
  public static final String NAME = "FIXED";
  private int length;
  private Schema schema;

  public AvroFixed(int length) {
    super();
    this.length = length;
  }

  public AvroFixed(String text) {
    this(LogicalTypeWithLength.getLength(text));
  }

  public AvroFixed(Schema schema) {
    this(schema.getFixedSize());
    this.schema = schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    return ((AvroFixed) o).length == length;
  }

  @Override
  public int hashCode() {
    return 1;
  }

  @Override
  public String toString() {
    return NAME + "(" + length + ")";
  }

  @Override
  public GenericFixed convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof GenericFixed) {
      return (GenericFixed) value;
    } else if (value instanceof byte[]) {
      return new GenericData.Fixed(schema, (byte[]) value);
    } else if (value instanceof ByteBuffer) {
      return new GenericData.Fixed(schema, ((ByteBuffer) value).array());
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a GenericFixed");
  }

  @Override
  public GenericFixed convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof GenericFixed) {
      return (GenericFixed) value;
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
    return Type.FIXED;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROFIXED;
  }

  @Override
  public Class<?> getConvertedType() {
    return GenericFixed.class;
  }

}
