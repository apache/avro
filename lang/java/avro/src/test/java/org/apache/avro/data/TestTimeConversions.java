package org.apache.avro.data;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions.DateConversion;
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
  public static Schema TIMESTAMP_MILLIS_SCHEMA;

  @BeforeClass
  public static void createSchemas() {
    TestTimeConversions.DATE_SCHEMA = LogicalTypes.date()
        .addToSchema(Schema.create(Schema.Type.INT));
    TestTimeConversions.TIME_MILLIS_SCHEMA = LogicalTypes.timeMillis()
        .addToSchema(Schema.create(Schema.Type.INT));
    TestTimeConversions.TIMESTAMP_MILLIS_SCHEMA = LogicalTypes.timestampMillis()
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
  public void testTimestampMillisConversion() throws Exception {
    TimestampConversion conversion = new TimestampConversion();
    long nowInstant = new Date().getTime();

    DateTime now = conversion.fromLong(
        nowInstant, TIME_MILLIS_SCHEMA, LogicalTypes.timestampMillis());
    long roundTrip = conversion.toLong(
        now, TIME_MILLIS_SCHEMA, LogicalTypes.timestampMillis());
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
}
