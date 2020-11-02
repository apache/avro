package org.apache.avro.logicaltypes;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.TimestampMillis;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.data.TimeConversions.TimestampMillisConversion;

/**
 * Wrapper of LogicalTypes.TimestampMillis. A timestamp is a point in time in
 * UTC timezone, an Instant in Java.
 *
 */
public class AvroTimestampMillis extends TimestampMillis implements AvroPrimitive {
  private static Schema schema;
  private static AvroTimestampMillis element = new AvroTimestampMillis();
  static {
    schema = element.addToSchema(Schema.create(Type.LONG));
  }
  public static final String NAME = "TIMESTAMPMILLIS";
  public static final String TYPENAME = LogicalTypes.TIMESTAMP_MILLIS;
  public static final TimestampMillisConversion CONVERTER = new TimestampMillisConversion();

  public AvroTimestampMillis() {
    super();
  }

  public static AvroTimestampMillis create() {
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
    } else if (value instanceof Date) {
      return convertToRawType(((Date) value).toInstant());
    } else if (value instanceof ZonedDateTime) {
      return convertToRawType(((ZonedDateTime) value).toInstant());
    } else if (value instanceof Instant) {
      return CONVERTER.toLong((Instant) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Timestamp");
  }

  @Override
  public Instant convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Long) {
      return CONVERTER.fromLong((Long) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Timestamp");
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
    return Instant.class;
  }

}
