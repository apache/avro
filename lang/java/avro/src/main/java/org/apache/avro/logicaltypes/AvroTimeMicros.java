package org.apache.avro.logicaltypes;

import java.time.LocalTime;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.TimeMicros;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.data.TimeConversions.TimeMicrosConversion;

/**
 * Wrapper of LogicalTypes.timeMicros()
 *
 */
public class AvroTimeMicros extends TimeMicros implements AvroPrimitive {
  private static Schema schema;
  private static AvroTimeMicros element = new AvroTimeMicros();
  public static final String NAME = "TIMEMICROS";
  public static final String TYPENAME = LogicalTypes.TIME_MICROS;
  public static final TimeMicrosConversion CONVERTER = new TimeMicrosConversion();

  static {
    schema = element.addToSchema(Schema.create(Type.LONG));
  }

  public AvroTimeMicros() {
    super();
  }

  public static AvroTimeMicros create() {
    return element;
  }

  @Override
  public Schema addToSchema(Schema schema) {
    return super.addToSchema(schema);
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
    } else if (value instanceof LocalTime) {
      return CONVERTER.toLong((LocalTime) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a TimeMicros");
  }

  @Override
  public LocalTime convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Long) {
      return CONVERTER.fromLong((Long) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a TimeMicros");
  }

  @Override
  public void toString(StringBuffer b, Object value) {
    if (value != null) {
      if (value instanceof Integer) {
        LocalTime time = LocalTime.ofNanoOfDay(((Integer) value).longValue() * 1000);
        b.append('\"');
        b.append(time.toString());
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
    return AvroType.AVROTIMEMICROS;
  }

  @Override
  public Class<?> getConvertedType() {
    return LocalTime.class;
  }

}
