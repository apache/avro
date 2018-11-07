/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.specific;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions.DateConversion;
import org.apache.avro.data.TimeConversions.TimeConversion;
import org.apache.avro.data.TimeConversions.TimestampConversion;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * This tests compatibility between classes generated before and after
 * AVRO-1684. TestRecordWithoutLogicalTypes and TestRecordWithLogicalTypes were
 * generated from the same schema, found in
 * src/test/resources/record_with_logical_types.avsc, and
 * TestRecordWithoutLogicalTypes was renamed to avoid the conflict.
 *
 * The classes should not be re-generated because they test compatibility of
 * Avro with existing Avro-generated sources. When using classes generated
 * before AVRO-1684, logical types should not be applied by the read or write
 * paths. Those files should behave as they did before.
 *
 * For AVRO-2079 {@link TestRecordWithJsr310LogicalTypes} was generated from
 * the same schema and tests were added to test compatibility between the
 * two versions.
 */
public class TestSpecificLogicalTypes {

  // Override the default ISO_LOCAL_TIME to make sure that there are
  // trailing zero's in the format:
  // Expected: is "22:07:33.880"
  //     but: was "22:07:33.88"
  private static final DateTimeFormatter ISO_LOCAL_TIME = new DateTimeFormatterBuilder()
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .optionalStart()
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 3, 3, true)
          .toFormatter();

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testRecordWithLogicalTypes() throws IOException {
    TestRecordWithLogicalTypes record = new TestRecordWithLogicalTypes(
        true,
        34,
        35L,
        3.14F,
        3019.34,
        null,
        LocalDate.now(),
        LocalTime.now(),
        DateTime.now().withZone(DateTimeZone.UTC),
        new BigDecimal(123.45f).setScale(2, BigDecimal.ROUND_HALF_DOWN)
    );

    File data = write(TestRecordWithLogicalTypes.getClassSchema(), record);
    List<TestRecordWithLogicalTypes> actual = read(
        TestRecordWithLogicalTypes.getClassSchema(), data);

    Assert.assertEquals("Should match written record", record, actual.get(0));
  }
  @Test
  public void testRecordWithJsr310LogicalTypes() throws IOException {
    TestRecordWithJsr310LogicalTypes record = new TestRecordWithJsr310LogicalTypes(
        true,
        34,
        35L,
        3.14F,
        3019.34,
        null,
        java.time.LocalDate.now(),
        java.time.LocalTime.now().truncatedTo(ChronoUnit.MILLIS),
        java.time.Instant.now().truncatedTo(ChronoUnit.MILLIS),
        new BigDecimal(123.45f).setScale(2, BigDecimal.ROUND_HALF_DOWN)
    );

    File data = write(TestRecordWithJsr310LogicalTypes.getClassSchema(), record);
    List<TestRecordWithJsr310LogicalTypes> actual = read(
        TestRecordWithJsr310LogicalTypes.getClassSchema(), data);

    Assert.assertEquals("Should match written record", record, actual.get(0));
  }

  @Test
  public void testAbilityToReadJsr310RecordWrittenAsJodaRecord() throws IOException {
    TestRecordWithLogicalTypes withJoda = new TestRecordWithLogicalTypes(
            true,
            34,
            35L,
            3.14F,
            3019.34,
            null,
            LocalDate.now(),
            LocalTime.now(),
            DateTime.now().withZone(DateTimeZone.UTC),
            new BigDecimal(123.45f).setScale(2, BigDecimal.ROUND_HALF_DOWN)
    );

    File data = write(TestRecordWithLogicalTypes.getClassSchema(), withJoda);
    List<TestRecordWithJsr310LogicalTypes> actual = read(
        TestRecordWithJsr310LogicalTypes.getClassSchema(), data);

    Assert.assertThat(actual, is(not(empty())));
    TestRecordWithJsr310LogicalTypes withJsr310 = actual.get(0);

    Assert.assertThat(withJsr310.getB(), is(withJoda.getB()));
    Assert.assertThat(withJsr310.getI32(), is(withJoda.getI32()));
    Assert.assertThat(withJsr310.getI64(), is(withJoda.getI64()));
    Assert.assertThat(withJsr310.getF32(), is(withJoda.getF32()));
    Assert.assertThat(withJsr310.getF64(), is(withJoda.getF64()));
    Assert.assertThat(withJsr310.getS(), is(withJoda.getS()));

    Assert.assertThat(ISO_LOCAL_DATE.format(withJsr310.getD()), is(ISODateTimeFormat.date().print(withJoda.getD())));
    Assert.assertThat(ISO_LOCAL_TIME.format(withJsr310.getT()), is(ISODateTimeFormat.time().print(withJoda.getT())));
    Assert.assertThat(ISO_INSTANT.format(withJsr310.getTs()), is(ISODateTimeFormat.dateTime().print(withJoda.getTs())));
    Assert.assertThat(withJsr310.getDec(), comparesEqualTo(withJoda.getDec()));
  }

  @Test
  public void testAbilityToReadJodaRecordWrittenAsJsr310Record() throws IOException {
    TestRecordWithJsr310LogicalTypes withJsr310 = new TestRecordWithJsr310LogicalTypes(
            true,
            34,
            35L,
            3.14F,
            3019.34,
            null,
            java.time.LocalDate.now(),
            java.time.LocalTime.now().truncatedTo(ChronoUnit.MILLIS),
            java.time.Instant.now().truncatedTo(ChronoUnit.MILLIS),
            new BigDecimal(123.45f).setScale(2, BigDecimal.ROUND_HALF_DOWN)
    );

    File data = write(TestRecordWithJsr310LogicalTypes.getClassSchema(), withJsr310);
    List<TestRecordWithLogicalTypes> actual = read(
        TestRecordWithLogicalTypes.getClassSchema(), data);

    Assert.assertThat(actual, is(not(empty())));
    TestRecordWithLogicalTypes withJoda = actual.get(0);

    Assert.assertThat(withJoda.getB(), is(withJsr310.getB()));
    Assert.assertThat(withJoda.getI32(), is(withJsr310.getI32()));
    Assert.assertThat(withJoda.getI64(), is(withJsr310.getI64()));
    Assert.assertThat(withJoda.getF32(), is(withJsr310.getF32()));
    Assert.assertThat(withJoda.getF64(), is(withJsr310.getF64()));
    Assert.assertThat(withJoda.getS(), is(withJsr310.getS()));
    // all of these print in the ISO-8601 format
    Assert.assertThat(withJoda.getD().toString(), is(withJsr310.getD().toString()));
    Assert.assertThat(withJoda.getT().toString(), is(withJsr310.getT().toString()));
    Assert.assertThat(withJoda.getTs().toString(), is(withJsr310.getTs().toString()));
    Assert.assertThat(withJoda.getDec(), comparesEqualTo(withJsr310.getDec()));
  }

  @Test
  public void testRecordWithoutLogicalTypes() throws IOException {
    // the significance of the record without logical types is that it has the
    // same schema (besides record name) as the one with logical types,
    // including the type annotations. this verifies that the type annotations
    // are only applied if the record was compiled to use those types. this
    // ensures compatibility with already-compiled code.

    TestRecordWithoutLogicalTypes record = new TestRecordWithoutLogicalTypes(
        true,
        34,
        35L,
        3.14F,
        3019.34,
        null,
        new DateConversion().toInt(LocalDate.now(), null, null),
        new TimeConversion().toInt(LocalTime.now(), null, null),
        new TimestampConversion().toLong(
            DateTime.now().withZone(DateTimeZone.UTC), null, null),
        new Conversions.DecimalConversion().toBytes(
            new BigDecimal(123.45f).setScale(2, BigDecimal.ROUND_HALF_DOWN), null,
            LogicalTypes.decimal(9, 2))
    );

    File data = write(TestRecordWithoutLogicalTypes.getClassSchema(), record);
    List<TestRecordWithoutLogicalTypes> actual = read(
        TestRecordWithoutLogicalTypes.getClassSchema(), data);

    Assert.assertEquals("Should match written record", record, actual.get(0));
  }

  @Test
  public void testRecordWritePrimitivesReadLogicalTypes() throws IOException {
    LocalDate date = LocalDate.now();
    LocalTime time = LocalTime.now();
    DateTime timestamp = DateTime.now().withZone(DateTimeZone.UTC);
    BigDecimal decimal = new BigDecimal(123.45f).setScale(2, BigDecimal.ROUND_HALF_DOWN);

    TestRecordWithoutLogicalTypes record = new TestRecordWithoutLogicalTypes(
        true,
        34,
        35L,
        3.14F,
        3019.34,
        null,
        new DateConversion().toInt(date, null, null),
        new TimeConversion().toInt(time, null, null),
        new TimestampConversion().toLong(timestamp, null, null),
        new Conversions.DecimalConversion().toBytes(decimal, null, LogicalTypes.decimal(9, 2))
    );

    File data = write(TestRecordWithoutLogicalTypes.getClassSchema(), record);
    // read using the schema with logical types
    List<TestRecordWithLogicalTypes> actual = read(
        TestRecordWithLogicalTypes.getClassSchema(), data);

    TestRecordWithLogicalTypes expected = new TestRecordWithLogicalTypes(
        true,
        34,
        35L,
        3.14F,
        3019.34,
        null,
        date,
        time,
        timestamp,
        decimal
    );

    Assert.assertEquals("Should match written record", expected, actual.get(0));
  }

  @Test
  public void testRecordWriteLogicalTypesReadPrimitives() throws IOException {
    LocalDate date = LocalDate.now();
    LocalTime time = LocalTime.now();
    DateTime timestamp = DateTime.now().withZone(DateTimeZone.UTC);
    BigDecimal decimal = new BigDecimal(123.45f).setScale(2, BigDecimal.ROUND_HALF_DOWN);

    TestRecordWithLogicalTypes record = new TestRecordWithLogicalTypes(
        true,
        34,
        35L,
        3.14F,
        3019.34,
        null,
        date,
        time,
        timestamp,
        decimal
    );

    File data = write(TestRecordWithLogicalTypes.getClassSchema(), record);
    // read using the schema with logical types
    List<TestRecordWithoutLogicalTypes> actual = read(
        TestRecordWithoutLogicalTypes.getClassSchema(), data);

    TestRecordWithoutLogicalTypes expected = new TestRecordWithoutLogicalTypes(
        true,
        34,
        35L,
        3.14F,
        3019.34,
        null,
        new DateConversion().toInt(date, null, null),
        new TimeConversion().toInt(time, null, null),
        new TimestampConversion().toLong(timestamp, null, null),
        new Conversions.DecimalConversion().toBytes(decimal, null, LogicalTypes.decimal(9, 2))
    );

    Assert.assertEquals("Should match written record", expected, actual.get(0));
  }

  private <D> List<D> read(Schema schema, File file)
      throws IOException {
    DatumReader<D> reader = newReader(schema);
    List<D> data = new ArrayList<>();

    try (FileReader<D> fileReader = new DataFileReader<>(file, reader)) {
      for (D datum : fileReader) {
        data.add(datum);
      }
    }

    return data;
  }

  @SuppressWarnings("unchecked")
  private <D> DatumReader<D> newReader(Schema schema) {
    return SpecificData.get().createDatumReader(schema);
  }

  @SuppressWarnings("unchecked")
  private <D extends SpecificRecord> File write(Schema schema, D... data)
      throws IOException {
    File file = temp.newFile();
    DatumWriter<D> writer = SpecificData.get().createDatumWriter(schema);

    try (DataFileWriter<D> fileWriter = new DataFileWriter<>(writer)) {
      fileWriter.create(schema, file);
      for (D datum : data) {
        fileWriter.append(datum);
      }
    }

    return file;
  }
}
