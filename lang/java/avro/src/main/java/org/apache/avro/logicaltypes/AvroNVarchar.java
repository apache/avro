package org.apache.avro.logicaltypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * A nvarchar is a string up to a provided max length, holds UTF-8 chars and is
 * sorted and compared binary.
 *
 */
public class AvroNVarchar extends LogicalTypeWithLength {
  public static final String NAME = "NVARCHAR";
  public static final String TYPENAME = NAME;
  private Schema schema;

  public AvroNVarchar(int length) {
    super(TYPENAME, length);
    this.schema = addToSchema(Schema.create(Type.STRING));
  }

  public AvroNVarchar(String text) {
    this(getLength(text));
  }

  public AvroNVarchar(Schema schema) {
    super(TYPENAME, schema);
    this.schema = schema;
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
    return AvroType.AVRONVARCHAR;
  }

  @Override
  public Class<?> getConvertedType() {
    return CharSequence.class;
  }

}
