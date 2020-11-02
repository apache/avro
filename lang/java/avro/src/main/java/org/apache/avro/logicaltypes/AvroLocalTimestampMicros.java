package org.apache.avro.logicaltypes;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.LocalTimestampMicros;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.data.TimeConversions.LocalTimestampMicrosConversion;

/**
 * Wrapper of LogicalTypes.timestampMicros()
 *
 */
public class AvroLocalTimestampMicros extends LocalTimestampMicros implements AvroPrimitive {
  private static Schema schema;
  private static AvroLocalTimestampMicros element = new AvroLocalTimestampMicros();
  static {
    schema = element.addToSchema(Schema.create(Type.LONG));
  }
  public static final String NAME = "LOCALTIMESTAMPMICROS";
  public static final String TYPENAME = LogicalTypes.LOCAL_TIMESTAMP_MICROS;
  public static final LocalTimestampMicrosConversion CONVERTER = new LocalTimestampMicrosConversion();

  public AvroLocalTimestampMicros() {
    super();
  }

  @Override
  public String toString() {
    return NAME;
  }

  public static AvroLocalTimestampMicros create() {
    return element;
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
    } else if (value instanceof LocalDateTime) {
      return CONVERTER.toLong((LocalDateTime) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a TimestampMicros");
  }

  @Override
  public LocalDateTime convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Long) {
      return CONVERTER.fromLong((Long) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a TimestampMicros");
  }

  @Override
  public void toString(StringBuffer b, Object value) {
    if (value != null) {
      if (value instanceof Long) {
        LocalDateTime d = convertToLogicalType(value);
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
    return AvroType.AVROLOCALTIMESTAMPMICROS;
  }

  @Override
  public Class<?> getConvertedType() {
    return LocalDateTime.class;
  }

}
