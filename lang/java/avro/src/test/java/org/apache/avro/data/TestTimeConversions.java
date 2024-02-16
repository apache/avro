/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions.DateConversion;
import org.apache.avro.data.TimeConversions.TimeMicrosConversion;
import org.apache.avro.data.TimeConversions.TimeMillisConversion;
import org.apache.avro.data.TimeConversions.TimestampMicrosConversion;
import org.apache.avro.data.TimeConversions.TimestampMillisConversion;
import org.apache.avro.reflect.ReflectData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestTimeConversions {

  public static Schema DATE_SCHEMA;
  public static Schema TIME_MILLIS_SCHEMA;
  public static Schema TIME_MICROS_SCHEMA;
  public static Schema TIMESTAMP_MILLIS_SCHEMA;
  public static Schema TIMESTAMP_MICROS_SCHEMA;

  @BeforeAll
  public static void createSchemas() {
    TestTimeConversions.DATE_SCHEMA = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    TestTimeConversions.TIME_MILLIS_SCHEMA = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    TestTimeConversions.TIME_MICROS_SCHEMA = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    TestTimeConversions.TIMESTAMP_MILLIS_SCHEMA = LogicalTypes.timestampMillis()
        .addToSchema(Schema.create(Schema.Type.LONG));
    TestTimeConversions.TIMESTAMP_MICROS_SCHEMA = LogicalTypes.timestampMicros()
        .addToSchema(Schema.create(Schema.Type.LONG));
  }

  @Test
  void dateConversion() throws Exception {
    DateConversion conversion = new DateConversion();
    LocalDate Jan_6_1970 = LocalDate.of(1970, 1, 6); // 5
    LocalDate Jan_1_1970 = LocalDate.of(1970, 1, 1); // 0
    LocalDate Dec_27_1969 = LocalDate.of(1969, 12, 27); // -5

    assertEquals(5, (int) conversion.toInt(Jan_6_1970, DATE_SCHEMA, LogicalTypes.date()), "6 Jan 1970 should be 5");
    assertEquals(0, (int) conversion.toInt(Jan_1_1970, DATE_SCHEMA, LogicalTypes.date()), "1 Jan 1970 should be 0");
    assertEquals(-5, (int) conversion.toInt(Dec_27_1969, DATE_SCHEMA, LogicalTypes.date()), "27 Dec 1969 should be -5");

    assertEquals(conversion.fromInt(5, DATE_SCHEMA, LogicalTypes.date()), Jan_6_1970, "6 Jan 1970 should be 5");
    assertEquals(conversion.fromInt(0, DATE_SCHEMA, LogicalTypes.date()), Jan_1_1970, "1 Jan 1970 should be 0");
    assertEquals(conversion.fromInt(-5, DATE_SCHEMA, LogicalTypes.date()), Dec_27_1969, "27 Dec 1969 should be -5");
  }

  @Test
  void timeMillisConversion() {
    TimeMillisConversion conversion = new TimeMillisConversion();
    LocalTime oneAM = LocalTime.of(1, 0);
    LocalTime afternoon = LocalTime.of(15, 14, 15, 926_000_000);
    int afternoonMillis = ((15 * 60 + 14) * 60 + 15) * 1000 + 926;

    assertEquals(0, (int) conversion.toInt(LocalTime.MIDNIGHT, TIME_MILLIS_SCHEMA, LogicalTypes.timeMillis()),
        "Midnight should be 0");
    assertEquals(3_600_000, (int) conversion.toInt(oneAM, TIME_MILLIS_SCHEMA, LogicalTypes.timeMillis()),
        "01:00 should be 3,600,000");
    assertEquals(afternoonMillis, (int) conversion.toInt(afternoon, TIME_MILLIS_SCHEMA, LogicalTypes.timeMillis()),
        "15:14:15.926 should be " + afternoonMillis);

    assertEquals(LocalTime.MIDNIGHT, conversion.fromInt(0, TIME_MILLIS_SCHEMA, LogicalTypes.timeMillis()),
        "Midnight should be 0");
    assertEquals(oneAM, conversion.fromInt(3600000, TIME_MILLIS_SCHEMA, LogicalTypes.timeMillis()),
        "01:00 should be 3,600,000");
    assertEquals(afternoon, conversion.fromInt(afternoonMillis, TIME_MILLIS_SCHEMA, LogicalTypes.timeMillis()),
        "15:14:15.926 should be " + afternoonMillis);
  }

  @Test
  void timeMicrosConversion() throws Exception {
    TimeMicrosConversion conversion = new TimeMicrosConversion();
    LocalTime oneAM = LocalTime.of(1, 0);
    LocalTime afternoon = LocalTime.of(15, 14, 15, 926_551_000);
    long afternoonMicros = ((long) (15 * 60 + 14) * 60 + 15) * 1_000_000 + 926_551;

    assertEquals(LocalTime.MIDNIGHT, conversion.fromLong(0L, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()),
        "Midnight should be 0");
    assertEquals(oneAM, conversion.fromLong(3_600_000_000L, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()),
        "01:00 should be 3,600,000,000");
    assertEquals(afternoon, conversion.fromLong(afternoonMicros, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()),
        "15:14:15.926551 should be " + afternoonMicros);

    assertEquals(0, (long) conversion.toLong(LocalTime.MIDNIGHT, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()),
        "Midnight should be 0");
    assertEquals(3_600_000_000L, (long) conversion.toLong(oneAM, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()),
        "01:00 should be 3,600,000,000");
    assertEquals(afternoonMicros, (long) conversion.toLong(afternoon, TIME_MICROS_SCHEMA, LogicalTypes.timeMicros()),
        "15:14:15.926551 should be " + afternoonMicros);
  }

  @Test
  void timestampMillisConversion() throws Exception {
    TimestampMillisConversion conversion = new TimestampMillisConversion();
    long nowInstant = Instant.now().toEpochMilli(); // ms precision

    // round trip
    Instant now = conversion.fromLong(nowInstant, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis());
    long roundTrip = conversion.toLong(now, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis());
    assertEquals(nowInstant, roundTrip, "Round-trip conversion should work");

    long May_28_2015_21_46_53_221_instant = 1432849613221L;
    Instant May_28_2015_21_46_53_221 = ZonedDateTime.of(2015, 5, 28, 21, 46, 53, 221_000_000, ZoneOffset.UTC)
        .toInstant();

    // known dates from https://www.epochconverter.com/
    // > Epoch
    assertEquals(May_28_2015_21_46_53_221,
        conversion.fromLong(May_28_2015_21_46_53_221_instant, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()),
        "Known date should be correct");
    assertEquals(May_28_2015_21_46_53_221_instant,
        (long) conversion.toLong(May_28_2015_21_46_53_221, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()),
        "Known date should be correct");

    // Epoch
    assertEquals(Instant.EPOCH, conversion.fromLong(0L, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()),
        "1970-01-01 should be 0");
    assertEquals(0L, (long) conversion.toLong(ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC).toInstant(),
        TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()), "1970-01-01 should be 0");

    // < Epoch
    long Jul_01_1969_12_00_00_123_instant = -15854400000L + 123;
    Instant Jul_01_1969_12_00_00_123 = ZonedDateTime.of(1969, 7, 1, 12, 0, 0, 123_000_000, ZoneOffset.UTC).toInstant();

    assertEquals(Jul_01_1969_12_00_00_123,
        conversion.fromLong(Jul_01_1969_12_00_00_123_instant, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()),
        "Pre 1970 date should be correct");
    assertEquals(Jul_01_1969_12_00_00_123_instant,
        (long) conversion.toLong(Jul_01_1969_12_00_00_123, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()),
        "Pre 1970 date should be correct");
  }

  @Test
  void timestampMicrosConversion() throws Exception {
    TimestampMicrosConversion conversion = new TimestampMicrosConversion();

    // known dates from https://www.epochconverter.com/
    // > Epoch
    long May_28_2015_21_46_53_221_843_instant = 1432849613221L * 1000 + 843;
    Instant May_28_2015_21_46_53_221_843 = ZonedDateTime.of(2015, 5, 28, 21, 46, 53, 221_843_000, ZoneOffset.UTC)
        .toInstant();

    assertEquals(May_28_2015_21_46_53_221_843, conversion.fromLong(May_28_2015_21_46_53_221_843_instant,
        TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros()), "Known date should be correct");

    assertEquals(May_28_2015_21_46_53_221_843_instant,
        (long) conversion.toLong(May_28_2015_21_46_53_221_843, TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMillis()),
        "Known date should be correct");

    // Epoch
    assertEquals(Instant.EPOCH, conversion.fromLong(0L, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()),
        "1970-01-01 should be 0");
    assertEquals(0L, (long) conversion.toLong(ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC).toInstant(),
        TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()), "1970-01-01 should be 0");

    // < Epoch
    long Jul_01_1969_12_00_00_000_123_instant = -15854400000L * 1000 + 123;
    Instant Jul_01_1969_12_00_00_000_123 = ZonedDateTime.of(1969, 7, 1, 12, 0, 0, 123_000, ZoneOffset.UTC).toInstant();

    assertEquals(Jul_01_1969_12_00_00_000_123, conversion.fromLong(Jul_01_1969_12_00_00_000_123_instant,
        TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()), "Pre 1970 date should be correct");
    assertEquals(Jul_01_1969_12_00_00_000_123_instant,
        (long) conversion.toLong(Jul_01_1969_12_00_00_000_123, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()),
        "Pre 1970 date should be correct");
  }

  @Test
  void dynamicSchemaWithDateConversion() throws ClassNotFoundException {
    Schema schema = getReflectedSchemaByName("java.time.LocalDate", new TimeConversions.DateConversion());
    assertEquals(DATE_SCHEMA, schema, "Reflected schema should be logicalType date");
  }

  @Test
  void dynamicSchemaWithTimeConversion() throws ClassNotFoundException {
    Schema schema = getReflectedSchemaByName("java.time.LocalTime", new TimeConversions.TimeMillisConversion());
    assertEquals(TIME_MILLIS_SCHEMA, schema, "Reflected schema should be logicalType timeMillis");
  }

  @Test
  void dynamicSchemaWithTimeMicrosConversion() throws ClassNotFoundException {
    Schema schema = getReflectedSchemaByName("java.time.LocalTime", new TimeConversions.TimeMicrosConversion());
    assertEquals(TIME_MICROS_SCHEMA, schema, "Reflected schema should be logicalType timeMicros");
  }

  @Test
  void dynamicSchemaWithDateTimeConversion() throws ClassNotFoundException {
    Schema schema = getReflectedSchemaByName("java.time.Instant", new TimeConversions.TimestampMillisConversion());
    assertEquals(TIMESTAMP_MILLIS_SCHEMA, schema, "Reflected schema should be logicalType timestampMillis");
  }

  @Test
  void dynamicSchemaWithDateTimeMicrosConversion() throws ClassNotFoundException {
    Schema schema = getReflectedSchemaByName("java.time.Instant", new TimeConversions.TimestampMicrosConversion());
    assertEquals(TIMESTAMP_MICROS_SCHEMA, schema, "Reflected schema should be logicalType timestampMicros");
  }

  private Schema getReflectedSchemaByName(String className, Conversion<?> conversion) throws ClassNotFoundException {
    // one argument: a fully qualified class name
    Class<?> cls = Class.forName(className);

    // get the reflected schema for the given class
    ReflectData model = new ReflectData();
    model.addLogicalTypeConversion(conversion);
    return model.getSchema(cls);
  }
}
