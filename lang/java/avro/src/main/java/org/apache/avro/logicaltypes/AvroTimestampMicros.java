package org.apache.avro.logicaltypes;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.TimestampMicros;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.data.TimeConversions.TimestampMicrosConversion;

/**
 * Wrapper of LogicalTypes.timestampMicros()
 *
 */
public class AvroTimestampMicros extends TimestampMicros implements AvroPrimitive {
  private static Schema schema;
  private static AvroTimestampMicros element = new AvroTimestampMicros();
  static {
    schema = element.addToSchema(Schema.create(Type.LONG));
  }
  public static final String NAME = "TIMESTAMPMICROS";
  public static final String TYPENAME = LogicalTypes.TIMESTAMP_MICROS;
  private static final TimestampMicrosConversion CONVERTER = new TimestampMicrosConversion();

  public AvroTimestampMicros() {
    super();
  }

  @Override
  public String toString() {
    return NAME;
  }

  public static AvroTimestampMicros create() {
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
    } else if (value instanceof Instant) {
      return CONVERTER.toLong((Instant) value, schema, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a TimestampMicros");
  }

  @Override
  public Instant convertToLogicalType(Object value) {
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
        Instant d = convertToLogicalType(value);
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
    return AvroType.AVROTIMESTAMPMICROS;
  }

  @Override
  public Class<?> getConvertedType() {
    return Instant.class;
  }

}
