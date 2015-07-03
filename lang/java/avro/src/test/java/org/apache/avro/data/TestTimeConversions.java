package org.apache.avro.data;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions.DateConversion;
import org.apache.avro.data.TimeConversions.LossyTimeMicrosConversion;
import org.apache.avro.data.TimeConversions.LossyTimestampMicrosConversion;
import org.apache.avro.data.TimeConversions.TimeMicrosConversion;
import org.apache.avro.data.TimeConversions.TimestampMicrosConversion;
import org.apache.avro.data.TimeConversions.TimeConversion;
import org.apache.avro.data.TimeConversions.TimestampConversion;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.Date;

public class TestTimeConversions {

  public static Schema DATE_SCHEMA;
  public static Schema TIME_MILLIS_SCHEMA;
  public static Schema TIME_MICROS_SCHEMA;
  public static Schema TIMESTAMP_MILLIS_SCHEMA;
  public static Schema TIMESTAMP_MICROS_SCHEMA;

  @BeforeClass
  public static void createSchemas() {
    TestTimeConversions.DATE_SCHEMA = LogicalTypes.date()
        .addToSchema(Schema.create(Schema.Type.INT));
    TestTimeConversions.TIME_MILLIS_SCHEMA = LogicalTypes.timeMillis()
        .addToSchema(Schema.create(Schema.Type.INT));
    TestTimeConversions.TIME_MICROS_SCHEMA = LogicalTypes.timeMicros()
        .addToSchema(Schema.create(Schema.Type.LONG));
    TestTimeConversions.TIMESTAMP_MILLIS_SCHEMA = LogicalTypes.timestampMillis()
        .addToSchema(Schema.create(Schema.Type.LONG));
    TestTimeConversions.TIMESTAMP_MICROS_SCHEMA = LogicalTypes.timestampMicros()
        .addToSchema(Schema.create(Schema.Type.LONG));
  }

  @Test
  public void testDateConversion() throws Exception {
    DateConversion conversion = new DateConversion();
    LocalDate Jan_6_1970 = new LocalDate(1970, 1, 6);    //  5
    LocalDate Jan_1_1970 = new LocalDate(1970, 1, 1);    //  0
    LocalDate Dec_27_1969 = new LocalDate(1969, 12, 27); // -5

    Assert.assertEquals("6 Jan 1970 should be 5", 5,
        (int) conversion.toInt(Jan_6_1970, DATE_SCHEMA, LogicalTypes.date()));
    Assert.assertEquals("1 Jan 1970 should be 0", 0,
        (int) conversion.toInt(Jan_1_1970, DATE_SCHEMA, LogicalTypes.date()));
    Assert.assertEquals("27 Dec 1969 should be -5", -5,
        (int) conversion.toInt(Dec_27_1969, DATE_SCHEMA, LogicalTypes.date()));

    Assert.assertEquals("6 Jan 1970 should be 5",
        conversion.fromInt(5, DATE_SCHEMA, LogicalTypes.date()), Jan_6_1970);
    Assert.assertEquals("1 Jan 1970 should be 0",
        conversion.fromInt(0, DATE_SCHEMA, LogicalTypes.date()), Jan_1_1970);
    Assert.assertEquals("27 Dec 1969 should be -5",
        conversion.fromInt(-5, DATE_SCHEMA, LogicalTypes.date()), Dec_27_1969);
  }

  @Test
  public void testTimeMillisConversion() throws Exception {
    TimeConversion conversion = new TimeConversion();
    LocalTime oneAM = new LocalTime(1, 0);
    LocalTime afternoon = new LocalTime(15, 14, 15, 926);
    int afternoonMillis = ((15 * 60 + 14) * 60 + 15) * 1000 + 926;

    Assert.assertEquals("Midnight should be 0", 0,
        (int) conversion.toInt(
            LocalTime.MIDNIGHT, TIME_MILLIS_SCHEMA, LogicalTypes.timeMillis()));
    Assert.assertEquals("01:00 should be 3,600,000", 3600000,
        (int) conversion.toInt(
            oneAM, TIME_MILLIS_SCHEMA, LogicalTypes.timeMillis()));
    Assert.assertEquals("15:14:15.926 should be " + afternoonMillis,
        afternoonMillis,
        (int) conversion.toInt(
            afternoon, TIME_MILLIS_SCHEMA, LogicalTypes.timeMillis()));

    Assert.assertEquals("Midnight should be 0",
        LocalTime.MIDNIGHT,
        conversion.fromInt(0, TIME_MILLIS_SCHEMA, LogicalTypes.timeMillis()));
    Assert.assertEquals("01:00 should be 3,600,000",
        oneAM,
        conversion.fromInt(
            3600000, TIME_MILLIS_SCHEMA, LogicalTypes.timeMillis()));
    Assert.assertEquals("15:14:15.926 should be " + afternoonMillis,
        afternoon,
        conversion.fromInt(
            afternoonMillis, TIME_MILLIS_SCHEMA, LogicalTypes.timeMillis()));
  }

  @Test
  public void testTimeMicrosConversion() throws Exception {
    TimeMicrosConversion conversion = new TimeMicrosConversion();
    LocalTime oneAM = new LocalTime(1, 0);
    LocalTime afternoon = new LocalTime(15, 14, 15, 926);
    long afternoonMicros = ((long) (15 * 60 + 14) * 60 + 15) * 1000000 + 926551;

    Assert.assertEquals("Midnight should be 0",
        LocalTime.MIDNIGHT,
        conversion.fromLong(0L, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()));
    Assert.assertEquals("01:00 should be 3,600,000,000",
        oneAM,
        conversion.fromLong(
            3600000000L, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()));
    Assert.assertEquals("15:14:15.926000 should be " + afternoonMicros,
        afternoon,
        conversion.fromLong(
            afternoonMicros, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()));

    try {
      conversion.toLong(afternoon,
          TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros());
      Assert.fail("Should not convert LocalTime to long");
    } catch (UnsupportedOperationException e) {
      // expected
    }
  }

  @Test
  public void testLossyTimeMicrosConversion() throws Exception {
    TimeMicrosConversion conversion = new LossyTimeMicrosConversion();
    LocalTime oneAM = new LocalTime(1, 0);
    LocalTime afternoon = new LocalTime(15, 14, 15, 926);
    long afternoonMicros = ((long) (15 * 60 + 14) * 60 + 15) * 1000000 + 926551;

    Assert.assertEquals("Midnight should be 0", 0,
        (long) conversion.toLong(
            LocalTime.MIDNIGHT, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()));
    Assert.assertEquals("01:00 should be 3,600,000,000", 3600000000L,
        (long) conversion.toLong(
            oneAM, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()));
    Assert.assertEquals("15:14:15.926551 should be " + afternoonMicros,
        dropMicros(afternoonMicros), // loses precision!
        (long) conversion.toLong(
            afternoon, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()));

    Assert.assertEquals("Midnight should be 0",
        LocalTime.MIDNIGHT,
        conversion.fromLong(0L, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()));
    Assert.assertEquals("01:00 should be 3,600,000,000",
        oneAM,
        conversion.fromLong(
            3600000000L, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()));
    Assert.assertEquals("15:14:15.926000 should be " + afternoonMicros,
        afternoon,
        conversion.fromLong(
            afternoonMicros, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()));
  }

  @Test
  public void testTimestampMillisConversion() throws Exception {
    TimestampConversion conversion = new TimestampConversion();
    long nowInstant = new Date().getTime();

    DateTime now = conversion.fromLong(
        nowInstant, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis());
    long roundTrip = conversion.toLong(
        now, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis());
    Assert.assertEquals("Round-trip conversion should work",
        nowInstant, roundTrip);

    long May_28_2015_21_46_53_221_instant = 1432849613221L;
    DateTime May_28_2015_21_46_53_221 =
        new DateTime(2015, 5, 28, 21, 46, 53, 221, DateTimeZone.UTC);

    Assert.assertEquals("Known date should be correct",
        May_28_2015_21_46_53_221,
        conversion.fromLong(May_28_2015_21_46_53_221_instant,
            TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()));
    Assert.assertEquals("Known date should be correct",
        May_28_2015_21_46_53_221_instant,
        (long) conversion.toLong(May_28_2015_21_46_53_221,
            TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()));
  }

  @Test
  public void testTimestampMicrosConversion() throws Exception {
    TimestampMicrosConversion conversion = new TimestampMicrosConversion();

    long May_28_2015_21_46_53_221_843_instant = 1432849613221L * 1000 + 843;
    DateTime May_28_2015_21_46_53_221 =
        new DateTime(2015, 5, 28, 21, 46, 53, 221, DateTimeZone.UTC);

    Assert.assertEquals("Known date should be correct",
        May_28_2015_21_46_53_221,
        conversion.fromLong(May_28_2015_21_46_53_221_843_instant,
            TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros()));

    try {
      conversion.toLong(May_28_2015_21_46_53_221,
          TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros());
      Assert.fail("Should not convert DateTime to long");
    } catch (UnsupportedOperationException e) {
      // expected
    }
  }

  @Test
  public void testLossyTimestampMicrosConversion() throws Exception {
    TimestampMicrosConversion conversion = new LossyTimestampMicrosConversion();
    long nowInstant = new Date().getTime() * 1000 + 674; // add fake micros

    DateTime now = conversion.fromLong(
        nowInstant, TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros());
    long roundTrip = conversion.toLong(
        now, TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros());
    Assert.assertEquals("Round-trip conversion should lose microseconds",
        dropMicros(nowInstant), roundTrip);

    long May_28_2015_21_46_53_221_843_instant = 1432849613221L * 1000 + 843;
    DateTime May_28_2015_21_46_53_221 =
        new DateTime(2015, 5, 28, 21, 46, 53, 221, DateTimeZone.UTC);

    Assert.assertEquals("Known date should be correct",
        May_28_2015_21_46_53_221,
        conversion.fromLong(May_28_2015_21_46_53_221_843_instant,
            TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros()));
    Assert.assertEquals("Known date should be correct",
        dropMicros(May_28_2015_21_46_53_221_843_instant),
        (long) conversion.toLong(May_28_2015_21_46_53_221,
            TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros()));
  }

  private long dropMicros(long micros) {
    return micros / 1000 * 1000;
  }
}
