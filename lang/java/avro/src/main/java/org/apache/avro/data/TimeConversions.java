package org.apache.avro.data;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

public class TimeConversions {
  public static class DateConversion extends Conversion<LocalDate> {
    private static final LocalDate EPOCH_DATE = new LocalDate(1970, 1, 1);

    @Override
    public Class<LocalDate> getConvertedType() {
      return LocalDate.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "date";
    }

    @Override
    public LocalDate fromInt(Integer daysFromEpoch, Schema schema, LogicalType type) {
      return EPOCH_DATE.plusDays(daysFromEpoch);
    }

    @Override
    public Integer toInt(LocalDate date, Schema schema, LogicalType type) {
      return Days.daysBetween(EPOCH_DATE, date).getDays();
    }
  }

  public static class TimeConversion extends Conversion<LocalTime> {
    @Override
    public Class<LocalTime> getConvertedType() {
      return LocalTime.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "time-millis";
    }

    @Override
    public LocalTime fromInt(Integer millisFromMidnight, Schema schema, LogicalType type) {
      return LocalTime.fromMillisOfDay(millisFromMidnight);
    }

    @Override
    public Integer toInt(LocalTime time, Schema schema, LogicalType type) {
      return time.millisOfDay().get();
    }
  }

  public static class TimestampConversion extends Conversion<DateTime> {
    @Override
    public Class<DateTime> getConvertedType() {
      return DateTime.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp-millis";
    }

    @Override
    public DateTime fromLong(Long millisFromEpoch, Schema schema, LogicalType type) {
      return new DateTime(millisFromEpoch, DateTimeZone.UTC);
    }

    @Override
    public Long toLong(DateTime timestamp, Schema schema, LogicalType type) {
      return timestamp.getMillis();
    }
  }
}
