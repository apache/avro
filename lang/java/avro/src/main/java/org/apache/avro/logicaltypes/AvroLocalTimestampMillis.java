package org.apache.avro.logicaltypes;

import java.time.LocalDateTime;
import java.util.Date;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.LocalTimestampMillis;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.data.TimeConversions.LocalTimestampMillisConversion;

/**
 * Wrapper of LogicalTypes.TimestampMillis.
 *
 */
public class AvroLocalTimestampMillis extends LocalTimestampMillis implements AvroPrimitive {
  private static Schema schema;
  private static AvroLocalTimestampMillis element = new AvroLocalTimestampMillis();
  static {
    schema = element.addToSchema(Schema.create(Type.LONG));
  }
  public static final String NAME = "LOCALTIMESTAMPMILLIS";
  public static final String TYPENAME = LogicalTypes.LOCAL_TIMESTAMP_MILLIS;
  public static final LocalTimestampMillisConversion CONVERTER = new LocalTimestampMillisConversion();

  public AvroLocalTimestampMillis() {
    super();
  }

  public static AvroLocalTimestampMillis create() {
    return element;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Long convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof LocalDateTime) {
      return CONVERTER.toLong((LocalDateTime) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a LocalTimestampMillis");
  }

  @Override
  public LocalDateTime convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Long) {
      return CONVERTER.fromLong((Long) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a LocalTimestampMillis");
  }

  @Override
  public void toString(StringBuffer b, Object value) {
    if (value != null) {
      if (value instanceof Long) {
        Date d = new Date((Long) value);
        b.append('\"');
        b.append(d.toString());
        b.append('\"');
      }
    }
  }

  @Override
  public Type getBackingType() {
    return Type.LONG;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROTIMESTAMPMILLIS;
  }

  @Override
  public Class<?> getConvertedType() {
    return LocalDateTime.class;
  }

}
