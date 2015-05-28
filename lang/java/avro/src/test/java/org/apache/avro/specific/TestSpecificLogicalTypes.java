package org.apache.avro.specific;

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
import java.util.ArrayList;
import java.util.List;

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
        DateTime.now().withZone(DateTimeZone.UTC)
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
            DateTime.now().withZone(DateTimeZone.UTC), null, null)
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

    TestRecordWithoutLogicalTypes record = new TestRecordWithoutLogicalTypes(
        true,
        34,
        35L,
        3.14F,
        3019.34,
        null,
        new DateConversion().toInt(date, null, null),
        new TimeConversion().toInt(time, null, null),
        new TimestampConversion().toLong(timestamp, null, null)
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
        timestamp
    );

    Assert.assertEquals("Should match written record", expected, actual.get(0));
  }

  @Test
  public void testRecordWriteLogicalTypesReadPrimitives() throws IOException {
    LocalDate date = LocalDate.now();
    LocalTime time = LocalTime.now();
    DateTime timestamp = DateTime.now().withZone(DateTimeZone.UTC);

    TestRecordWithLogicalTypes record = new TestRecordWithLogicalTypes(
        true,
        34,
        35L,
        3.14F,
        3019.34,
        null,
        date,
        time,
        timestamp
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
        new TimestampConversion().toLong(timestamp, null, null)
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
