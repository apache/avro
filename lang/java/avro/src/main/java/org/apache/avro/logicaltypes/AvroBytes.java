package org.apache.avro.logicaltypes;

import java.nio.ByteBuffer;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Is the Avro Type.BYTES datatype, a binary store of any length. A BLOB column.
 *
 */
public class AvroBytes implements AvroPrimitive {
  public static final String NAME = "BYTES";
  private static AvroBytes element = new AvroBytes();
  private static Schema schema = Schema.create(Type.BYTES);

  public AvroBytes() {
    super();
  }

  public static AvroBytes create() {
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
  public Object convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof ByteBuffer) {
      return value;
    } else if (value instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) value);
    } else if (value instanceof Number) {
      byte[] b = new byte[1];
      b[0] = ((Number) value).byteValue();
      return ByteBuffer.wrap(b);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a ByteBuffer");
  }

  @Override
  public ByteBuffer convertToLogicalType(Object value) {
    return (ByteBuffer) value;
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
    return Type.BYTES;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROBYTES;
  }

  @Override
  public Class<?> getConvertedType() {
    return ByteBuffer.class;
  }

}
