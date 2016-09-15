/**
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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

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
 */
public class TestSpecificLogicalTypes {

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
    List<D> data = new ArrayList<D>();
    FileReader<D> fileReader = null;

    try {
      fileReader = new DataFileReader<D>(file, reader);
      for (D datum : fileReader) {
        data.add(datum);
      }
    } finally {
      if (fileReader != null) {
        fileReader.close();
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
    DataFileWriter<D> fileWriter = new DataFileWriter<D>(writer);

    try {
      fileWriter.create(schema, file);
      for (D datum : data) {
        fileWriter.append(datum);
      }
    } finally {
      fileWriter.close();
    }

    return file;
  }
}
