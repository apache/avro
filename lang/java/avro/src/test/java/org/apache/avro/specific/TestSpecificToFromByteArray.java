package org.apache.avro.specific;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.message.MissingSchemaException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.junit.Test;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class TestSpecificToFromByteArray {
  @Test
  public void testSpecificToFromByteBufferWithLogicalTypes() throws IOException {
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
        new BigDecimal("123.45")
    );

    ByteBuffer b = record.toByteBuffer();
    TestRecordWithLogicalTypes copy = TestRecordWithLogicalTypes.fromByteBuffer(b);

    assertEquals(record, copy);
  }

  @Test
  public void testSpecificToFromByteBufferWithoutLogicalTypes() throws IOException {
    TestRecordWithoutLogicalTypes record = new TestRecordWithoutLogicalTypes(
        true,
        34,
        35L,
        3.14F,
        3019.34,
        null,
        new TimeConversions.DateConversion().toInt(LocalDate.now(), null, null),
        new TimeConversions.TimeConversion().toInt(LocalTime.now(), null, null),
        new TimeConversions.TimestampConversion().toLong(
            DateTime.now().withZone(DateTimeZone.UTC), null, null),
        new Conversions.DecimalConversion().toBytes(
            new BigDecimal("123.45"), null, LogicalTypes.decimal(9, 2))
    );

    ByteBuffer b = record.toByteBuffer();
    TestRecordWithoutLogicalTypes copy = TestRecordWithoutLogicalTypes.fromByteBuffer(b);

    assertEquals(record, copy);
  }

  @Test(expected = MissingSchemaException.class)
  public void testSpecificByteArrayIncompatibleWithLogicalTypes() throws IOException {
    TestRecordWithoutLogicalTypes withoutLogicalTypes = new TestRecordWithoutLogicalTypes(
        true,
        34,
        35L,
        3.14F,
        3019.34,
        null,
        new TimeConversions.DateConversion().toInt(LocalDate.now(), null, null),
        new TimeConversions.TimeConversion().toInt(LocalTime.now(), null, null),
        new TimeConversions.TimestampConversion().toLong(
            DateTime.now().withZone(DateTimeZone.UTC), null, null),
        new Conversions.DecimalConversion().toBytes(
            new BigDecimal("123.45"), null, LogicalTypes.decimal(9, 2))
    );

    ByteBuffer b = withoutLogicalTypes.toByteBuffer();
    TestRecordWithLogicalTypes.fromByteBuffer(b);
  }

  @Test(expected = MissingSchemaException.class)
  public void testSpecificByteArrayIncompatibleWithoutLogicalTypes() throws IOException {
    TestRecordWithLogicalTypes withLogicalTypes = new TestRecordWithLogicalTypes(
        true,
        34,
        35L,
        3.14F,
        3019.34,
        null,
        LocalDate.now(),
        LocalTime.now(),
        DateTime.now().withZone(DateTimeZone.UTC),
        new BigDecimal("123.45")
    );

    ByteBuffer b = withLogicalTypes.toByteBuffer();
    TestRecordWithoutLogicalTypes.fromByteBuffer(b);
  }
}
