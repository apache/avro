package org.apache.avro;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;

public abstract class Conversion<T> {

  public abstract Class<T> getConvertedType();

  public abstract Schema getRecommendedSchema();

  public abstract String getLogicalTypeName();

  public T fromBoolean(Boolean value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "fromBoolean is not supported for " + type.getName());
  }

  public T fromInt(Integer value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "fromInt is not supported for " + type.getName());
  }

  public T fromLong(Long value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "fromLong is not supported for " + type.getName());
  }

  public T fromFloat(Float value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "fromFloat is not supported for " + type.getName());
  }

  public T fromDouble(Double value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "fromDouble is not supported for " + type.getName());
  }

  public T fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "fromCharSequence is not supported for " + type.getName());
  }

  public T fromEnumSymbol(GenericEnumSymbol value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "fromEnumSymbol is not supported for " + type.getName());
  }

  public T fromFixed(GenericFixed value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "fromFixed is not supported for " + type.getName());
  }

  public T fromBytes(ByteBuffer value, Schema schema, LogicalType type)  {
    throw new UnsupportedOperationException(
        "fromBytes is not supported for " + type.getName());
  }

  public T fromArray(Collection<?> value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "fromArray is not supported for " + type.getName());
  }

  public T fromMap(Map<?, ?> value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "fromMap is not supported for " + type.getName());
  }

  public T fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "fromRecord is not supported for " + type.getName());
  }

  public Boolean toBoolean(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "toBoolean is not supported for " + type.getName());
  }

  public Integer toInt(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "toInt is not supported for " + type.getName());
  }

  public Long toLong(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "toLong is not supported for " + type.getName());
  }

  public Float toFloat(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "toFloat is not supported for " + type.getName());
  }

  public Double toDouble(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "toDouble is not supported for " + type.getName());
  }

  public CharSequence toCharSequence(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "toCharSequence is not supported for " + type.getName());
  }

  public GenericEnumSymbol toEnumSymbol(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "toEnumSymbol is not supported for " + type.getName());
  }

  public GenericFixed toFixed(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "toFixed is not supported for " + type.getName());
  }

  public ByteBuffer toBytes(T value, Schema schema, LogicalType type)  {
    throw new UnsupportedOperationException(
        "toBytes is not supported for " + type.getName());
  }

  public Collection<?> toArray(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "toArray is not supported for " + type.getName());
  }

  public Map<?, ?> toMap(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "toMap is not supported for " + type.getName());
  }

  public IndexedRecord toRecord(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException(
        "toRecord is not supported for " + type.getName());
  }

  public static class UUIDConversion extends Conversion<UUID> {
    @Override
    public Class<UUID> getConvertedType() {
      return UUID.class;
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalType.uuid().addToSchema(Schema.create(Schema.Type.STRING));
    }

    @Override
    public String getLogicalTypeName() {
      return "uuid";
    }

    @Override
    public UUID fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
      return UUID.fromString(value.toString());
    }

    @Override
    public CharSequence toCharSequence(UUID value, Schema schema, LogicalType type) {
      return value.toString();
    }
  }

  public static class DecimalConversion extends Conversion<BigDecimal> {
    @Override
    public Class<BigDecimal> getConvertedType() {
      return BigDecimal.class;
    }

    @Override
    public Schema getRecommendedSchema() {
      throw new UnsupportedOperationException(
          "No recommended schema for decimal (scale is required)");
    }

    @Override
    public String getLogicalTypeName() {
      return "decimal";
    }

    @Override
    public BigDecimal fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
      int scale = ((LogicalType.Decimal) type).getScale();
      byte[] bytes;
      if (value.hasArray()) {
        bytes = value.array();
      } else {
        bytes = value.get(new byte[value.remaining()]).array();
      }
      return new BigDecimal(new BigInteger(bytes), scale);
    }

    @Override
    public ByteBuffer toBytes(BigDecimal value, Schema schema, LogicalType type) {
      int scale = ((LogicalType.Decimal) type).getScale();
      if (scale != value.scale()) {
        throw new AvroTypeException("Cannot encode decimal with scale " +
            value.scale() + " as scale " + scale);
      }
      return ByteBuffer.wrap(value.unscaledValue().toByteArray());
    }

    @Override
    public BigDecimal fromFixed(GenericFixed value, Schema schema, LogicalType type) {
      int scale = ((LogicalType.Decimal) type).getScale();
      return new BigDecimal(new BigInteger(value.bytes()), scale);
    }

    @Override
    public GenericFixed toFixed(BigDecimal value, Schema schema, LogicalType type) {
      int scale = ((LogicalType.Decimal) type).getScale();
      if (scale != value.scale()) {
        throw new AvroTypeException("Cannot encode decimal with scale " +
            value.scale() + " as scale " + scale);
      }

      byte fillByte = (byte) (value.signum() < 0 ? 0xFF : 0x00);
      byte[] unscaled = value.unscaledValue().toByteArray();
      byte[] bytes = new byte[schema.getFixedSize()];
      int offset = bytes.length - unscaled.length;

      for (int i = 0; i < bytes.length; i += 1) {
        if (i < offset) {
          bytes[i] = fillByte;
        } else {
          bytes[i] = unscaled[i - offset];
        }
      }

      return new GenericData.Fixed(schema, bytes);
    }
  }

}
