package org.apache.avro;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
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

}
