package org.apache.avro.protobuf;

import com.google.protobuf.Timestamp;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public class ProtoConversions {

  public static class TimestampConversion extends Conversion<Timestamp> {
    @Override
    public Class<Timestamp> getConvertedType() {
      return Timestamp.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp-millis";
    }

    @Override
    public Timestamp fromLong(Long millisFromEpoch, Schema schema, LogicalType type) {
      java.sql.Timestamp ts = new java.sql.Timestamp(millisFromEpoch);

      return Timestamp.newBuilder()
        .setSeconds(ts.getTime() / 1000)
        .setNanos(ts.getNanos())
        .build();
    }

    @Override
    public Long toLong(Timestamp value, Schema schema, LogicalType type) {
      java.sql.Timestamp ts = new java.sql.Timestamp(value.getSeconds() * 1000);
      ts.setNanos(value.getNanos());

      return ts.getTime();
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

  public static class TimestampMicrosConversion extends Conversion<Timestamp> {
    @Override
    public Class<Timestamp> getConvertedType() {
      return Timestamp.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp-micros";
    }

    @Override
    public Timestamp fromLong(Long microsFromEpoch, Schema schema, LogicalType type) {
      java.sql.Timestamp ts = new java.sql.Timestamp(microsFromEpoch / 1000);
      int micros = (int) (microsFromEpoch - (microsFromEpoch / 1000000 * 1000000));
      ts.setNanos(micros * 1000);

      return Timestamp.newBuilder()
        .setSeconds(ts.getTime() / 1000)
        .setNanos(ts.getNanos())
        .build();
    }

    @Override
    public Long toLong(Timestamp value, Schema schema, LogicalType type) {
      if (value.getNanos() - (value.getNanos() / 1000000 * 1000000) > 0) {
        throw new UnsupportedOperationException("micros-precise is supported");
      }

      java.sql.Timestamp ts = new java.sql.Timestamp(value.getSeconds() * 1000);
      ts.setNanos(value.getNanos());

      return ts.getTime();
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

}
