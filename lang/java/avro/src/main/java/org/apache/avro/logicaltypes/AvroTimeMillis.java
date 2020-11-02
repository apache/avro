package org.apache.avro.logicaltypes;

import java.time.LocalTime;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.TimeMillis;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.data.TimeConversions.TimeMillisConversion;

/**
 * Wrapper of LogicalTypes.timeMillis()
 *
 */
public class AvroTimeMillis extends TimeMillis implements AvroPrimitive {
  private static Schema schema;
  private static AvroTimeMillis element = new AvroTimeMillis();
  public static final String NAME = "TIME";
  public static final String TYPENAME = LogicalTypes.TIME_MILLIS;
  private static final TimeMillisConversion CONVERTER = new TimeMillisConversion();

  static {
    schema = element.addToSchema(Schema.create(Type.INT));
  }

  public AvroTimeMillis() {
    super();
  }

  public static AvroTimeMillis create() {
    return element;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Integer convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof LocalTime) {
      return CONVERTER.toInt((LocalTime) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Time");
  }

  @Override
  public LocalTime convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Integer) {
      return CONVERTER.fromInt((Integer) value, null, this);
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Time");
  }

  @Override
  public void toString(StringBuffer b, Object value) {
    if (value != null) {
      if (value instanceof Integer) {
        LocalTime time = LocalTime.ofSecondOfDay((Integer) value);
        b.append('\"');
        b.append(time.toString());
        b.append('\"');
      }
    }
  }

  @Override
  public Type getBackingType() {
    return Type.INT;
  }

  @Override
  public Schema getRecommendedSchema() {
    return schema;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROTIMEMILLIS;
  }

  @Override
  public Class<?> getConvertedType() {
    return Integer.class;
  }

}
