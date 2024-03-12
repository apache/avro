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

package org.apache.avro.generic;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.CustomType;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.TimePeriod;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class TestGenericLogicalTypes {

  @TempDir
  public File temp;

  public static final GenericData GENERIC = new GenericData();

  @BeforeAll
  public static void addLogicalTypes() {
    GENERIC.addLogicalTypeConversion(new Conversions.DecimalConversion());
    GENERIC.addLogicalTypeConversion(new Conversions.UUIDConversion());
    GENERIC.addLogicalTypeConversion(new Conversions.DurationConversion());
    GENERIC.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
    GENERIC.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
  }

  @Test
  public void readUUID() throws IOException {
    Schema uuidSchema = Schema.create(Schema.Type.STRING);
    LogicalTypes.uuid().addToSchema(uuidSchema);

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();
    List<UUID> expected = Arrays.asList(u1, u2);

    File test = write(Schema.create(Schema.Type.STRING), u1.toString(), u2.toString());
    assertEquals(expected, read(GENERIC.createDatumReader(uuidSchema), test), "Should convert Strings to UUIDs");
  }

  @Test
  public void writeUUID() throws IOException {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    stringSchema.addProp(GenericData.STRING_PROP, "String");
    Schema uuidSchema = Schema.create(Schema.Type.STRING);
    LogicalTypes.uuid().addToSchema(uuidSchema);

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();
    List<String> expected = Arrays.asList(u1.toString(), u2.toString());

    File test = write(GENERIC, uuidSchema, u1, u2);
    assertEquals(expected, read(GenericData.get().createDatumReader(stringSchema), test),
        "Should read UUIDs as Strings");
  }

  @Test
  public void writeNullableUUID() throws IOException {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    stringSchema.addProp(GenericData.STRING_PROP, "String");
    Schema nullableStringSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), stringSchema);

    Schema uuidSchema = Schema.create(Schema.Type.STRING);
    LogicalTypes.uuid().addToSchema(uuidSchema);
    Schema nullableUuidSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), uuidSchema);

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();
    List<String> expected = Arrays.asList(u1.toString(), u2.toString());

    File test = write(GENERIC, nullableUuidSchema, u1, u2);
    assertEquals(expected, read(GenericData.get().createDatumReader(nullableStringSchema), test),
        "Should read UUIDs as Strings");
  }

  @Test
  public void readWriteDuration() throws IOException {
    Schema fixedSchema = Schema.createFixed("bare.Fixed", null, null, 12);

    Schema durationSchema = Schema.createFixed("time.Duration", null, null, 12);
    LogicalTypes.duration().addToSchema(durationSchema);

    // These two are necessary for schema evolution!
    fixedSchema.addAlias(durationSchema.getFullName());
    durationSchema.addAlias(fixedSchema.getFullName());

    Random rng = new Random();
    TimePeriod d1 = TimePeriod.of(rng.nextInt(1000), rng.nextInt(1000), rng.nextInt(1000));
    ByteBuffer b1 = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putInt((int) d1.getMonths())
        .putInt((int) d1.getDays()).putInt((int) d1.getMillis());
    GenericFixed f1 = new GenericData.Fixed(fixedSchema, b1.array());

    TimePeriod d2 = TimePeriod.of(rng.nextInt(1000), rng.nextInt(1000), rng.nextInt(1000));
    ByteBuffer b2 = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putInt((int) d2.getMonths())
        .putInt((int) d2.getDays()).putInt((int) d2.getMillis());
    GenericFixed f2 = new GenericData.Fixed(fixedSchema, b2.array());

    File test = write(fixedSchema, f1, f2);
    assertEquals(Arrays.asList(d1, d2), read(GENERIC.createDatumReader(durationSchema), test),
        "Should convert fixed bytes to durations");

    test = write(GENERIC, durationSchema, d2, d1);
    assertEquals(Arrays.asList(f2, f1), read(GenericData.get().createDatumReader(fixedSchema), test),
        "Should convert durations to fixed bytes");
  }

  @Test
  public void readDecimalFixed() throws IOException {
    LogicalType decimal = LogicalTypes.decimal(9, 2);
    Schema fixedSchema = Schema.createFixed("aFixed", null, null, 4);
    Schema decimalSchema = decimal.addToSchema(Schema.createFixed("aFixed", null, null, 4));

    BigDecimal d1 = new BigDecimal("-34.34");
    BigDecimal d2 = new BigDecimal("117230.00");
    List<BigDecimal> expected = Arrays.asList(d1, d2);

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    // use the conversion directly instead of relying on the write side
    GenericFixed d1fixed = conversion.toFixed(d1, fixedSchema, decimal);
    GenericFixed d2fixed = conversion.toFixed(d2, fixedSchema, decimal);

    File test = write(fixedSchema, d1fixed, d2fixed);
    assertEquals(expected, read(GENERIC.createDatumReader(decimalSchema), test), "Should convert fixed to BigDecimals");
  }

  @Test
  public void writeDecimalFixed() throws IOException {
    LogicalType decimal = LogicalTypes.decimal(9, 2);
    Schema fixedSchema = Schema.createFixed("aFixed", null, null, 4);
    Schema decimalSchema = decimal.addToSchema(Schema.createFixed("aFixed", null, null, 4));

    BigDecimal d1 = new BigDecimal("-34.34");
    BigDecimal d2 = new BigDecimal("117230.00");

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    GenericFixed d1fixed = conversion.toFixed(d1, fixedSchema, decimal);
    GenericFixed d2fixed = conversion.toFixed(d2, fixedSchema, decimal);
    List<GenericFixed> expected = Arrays.asList(d1fixed, d2fixed);

    File test = write(GENERIC, decimalSchema, d1, d2);
    assertEquals(expected, read(GenericData.get().createDatumReader(fixedSchema), test),
        "Should read BigDecimals as fixed");
  }

  @Test
  public void decimalToFromBytes() {
    LogicalType decimal = LogicalTypes.decimal(9, 2);
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);

    // Check the round trip to and from bytes
    BigDecimal d1 = new BigDecimal("-34.34");
    BigDecimal d2 = new BigDecimal("117230.00");

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    ByteBuffer d1bytes = conversion.toBytes(d1, bytesSchema, decimal);
    ByteBuffer d2bytes = conversion.toBytes(d2, bytesSchema, decimal);

    assertThat(conversion.fromBytes(d1bytes, bytesSchema, decimal), is(d1));
    assertThat(conversion.fromBytes(d2bytes, bytesSchema, decimal), is(d2));

    assertThat("Ensure ByteBuffer not consumed by conversion", conversion.fromBytes(d1bytes, bytesSchema, decimal),
        is(d1));
  }

  @Test
  public void decimalToFromFixed() {
    LogicalType decimal = LogicalTypes.decimal(9, 2);
    Schema fixedSchema = Schema.createFixed("aFixed", null, null, 4);

    // Check the round trip to and from fixed data.
    BigDecimal d1 = new BigDecimal("-34.34");
    BigDecimal d2 = new BigDecimal("117230.00");

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    GenericFixed d1fixed = conversion.toFixed(d1, fixedSchema, decimal);
    GenericFixed d2fixed = conversion.toFixed(d2, fixedSchema, decimal);
    assertThat(conversion.fromFixed(d1fixed, fixedSchema, decimal), is(d1));
    assertThat(conversion.fromFixed(d2fixed, fixedSchema, decimal), is(d2));
  }

  @Test
  public void readDecimalBytes() throws IOException {
    LogicalType decimal = LogicalTypes.decimal(9, 2);
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);
    Schema decimalSchema = decimal.addToSchema(Schema.create(Schema.Type.BYTES));

    BigDecimal d1 = new BigDecimal("-34.34");
    BigDecimal d2 = new BigDecimal("117230.00");
    List<BigDecimal> expected = Arrays.asList(d1, d2);

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    // use the conversion directly instead of relying on the write side
    ByteBuffer d1bytes = conversion.toBytes(d1, bytesSchema, decimal);
    ByteBuffer d2bytes = conversion.toBytes(d2, bytesSchema, decimal);

    File test = write(bytesSchema, d1bytes, d2bytes);
    assertEquals(expected, read(GENERIC.createDatumReader(decimalSchema), test), "Should convert bytes to BigDecimals");
  }

  @Test
  public void writeDecimalBytes() throws IOException {
    LogicalType decimal = LogicalTypes.decimal(9, 2);
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);
    Schema decimalSchema = decimal.addToSchema(Schema.create(Schema.Type.BYTES));

    BigDecimal d1 = new BigDecimal("-34.34");
    BigDecimal d2 = new BigDecimal("117230.00");

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    // use the conversion directly instead of relying on the write side
    ByteBuffer d1bytes = conversion.toBytes(d1, bytesSchema, decimal);
    ByteBuffer d2bytes = conversion.toBytes(d2, bytesSchema, decimal);
    List<ByteBuffer> expected = Arrays.asList(d1bytes, d2bytes);

    File test = write(GENERIC, decimalSchema, d1bytes, d2bytes);
    assertEquals(expected, read(GenericData.get().createDatumReader(bytesSchema), test),
        "Should read BigDecimals as bytes");
  }

  private <D> List<D> read(DatumReader<D> reader, File file) throws IOException {
    List<D> data = new ArrayList<>();

    try (FileReader<D> fileReader = new DataFileReader<>(file, reader)) {
      for (D datum : fileReader) {
        data.add(datum);
      }
    }

    return data;
  }

  @SafeVarargs
  private final <D> File write(Schema schema, D... data) throws IOException {
    return write(GenericData.get(), schema, data);
  }

  @SuppressWarnings("unchecked")
  private <D> File write(GenericData model, Schema schema, D... data) throws IOException {
    File file = new File(temp, "out.avro");
    DatumWriter<D> writer = model.createDatumWriter(schema);

    try (DataFileWriter<D> fileWriter = new DataFileWriter<>(writer)) {
      fileWriter.create(schema, file);
      for (D datum : data) {
        fileWriter.append(datum);
      }
    }

    return file;
  }

  @Test
  public void copyUuid() {
    testCopy(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)), UUID.randomUUID(), GENERIC);
  }

  @Test
  public void copyUuidRaw() {
    testCopy(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)), UUID.randomUUID().toString(), // use
                                                                                                               // raw
                                                                                                               // type
        GenericData.get()); // with no conversions
  }

  @Test
  public void copyDecimal() {
    testCopy(LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)), new BigDecimal("-34.34"),
        GENERIC);
  }

  @Test
  public void copyDecimalRaw() {
    testCopy(LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)),
        ByteBuffer.wrap(new BigDecimal("-34.34").unscaledValue().toByteArray()), GenericData.get()); // no conversions
  }

  private void testCopy(Schema schema, Object value, GenericData model) {
    // test direct copy of instance
    checkCopy(value, model.deepCopy(schema, value), false);

    // test nested in a record
    Schema recordSchema = Schema.createRecord("X", "", "test", false);
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("x", schema, "", null));
    recordSchema.setFields(fields);

    GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
    builder.set("x", value);
    GenericData.Record record = builder.build();
    checkCopy(record, model.deepCopy(recordSchema, record), true);

    // test nested in array
    Schema arraySchema = Schema.createArray(schema);
    ArrayList<Object> array = new ArrayList<>(Collections.singletonList(value));
    checkCopy(array, model.deepCopy(arraySchema, array), true);

    // test record nested in array
    Schema recordArraySchema = Schema.createArray(recordSchema);
    ArrayList<GenericRecord> recordArray = new ArrayList<>(Collections.singletonList(record));
    checkCopy(recordArray, model.deepCopy(recordArraySchema, recordArray), true);
  }

  private void checkCopy(Object original, Object copy, boolean notSame) {
    if (notSame)
      assertNotSame(original, copy);
    assertEquals(original, copy);
  }

  @Test
  public void readLocalTimestampMillis() throws IOException {
    LogicalType timestamp = LogicalTypes.localTimestampMillis();
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema timestampSchema = timestamp.addToSchema(Schema.create(Schema.Type.LONG));

    LocalDateTime i1 = LocalDateTime.of(1986, 6, 26, 12, 7, 11, 42000000);
    LocalDateTime i2 = LocalDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneOffset.UTC);
    List<LocalDateTime> expected = Arrays.asList(i1, i2);

    Conversion<LocalDateTime> conversion = new TimeConversions.LocalTimestampMillisConversion();

    // use the conversion directly instead of relying on the write side
    Long i1long = conversion.toLong(i1, longSchema, timestamp);
    Long i2long = 0L;

    File test = write(longSchema, i1long, i2long);
    assertEquals(expected, read(GENERIC.createDatumReader(timestampSchema), test),
        "Should convert long to LocalDateTime");
  }

  @Test
  public void writeLocalTimestampMillis() throws IOException {
    LogicalType timestamp = LogicalTypes.localTimestampMillis();
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema timestampSchema = timestamp.addToSchema(Schema.create(Schema.Type.LONG));

    LocalDateTime i1 = LocalDateTime.of(1986, 6, 26, 12, 7, 11, 42000000);
    LocalDateTime i2 = LocalDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneOffset.UTC);

    Conversion<LocalDateTime> conversion = new TimeConversions.LocalTimestampMillisConversion();

    Long d1long = conversion.toLong(i1, longSchema, timestamp);
    Long d2long = 0L;
    List<Long> expected = Arrays.asList(d1long, d2long);

    File test = write(GENERIC, timestampSchema, i1, i2);
    assertEquals(expected, read(GenericData.get().createDatumReader(timestampSchema), test),
        "Should read LocalDateTime as longs");
  }

  @Test
  public void readLocalTimestampMicros() throws IOException {
    LogicalType timestamp = LogicalTypes.localTimestampMicros();
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema timestampSchema = timestamp.addToSchema(Schema.create(Schema.Type.LONG));

    LocalDateTime i1 = LocalDateTime.of(1986, 6, 26, 12, 7, 11, 420000);
    LocalDateTime i2 = LocalDateTime.ofInstant(Instant.ofEpochSecond(0, 4000), ZoneOffset.UTC);
    List<LocalDateTime> expected = Arrays.asList(i1, i2);

    Conversion<LocalDateTime> conversion = new TimeConversions.LocalTimestampMicrosConversion();

    // use the conversion directly instead of relying on the write side
    Long i1long = conversion.toLong(i1, longSchema, timestamp);
    Long i2long = conversion.toLong(i2, longSchema, timestamp);

    File test = write(longSchema, i1long, i2long);
    assertEquals(expected, read(GENERIC.createDatumReader(timestampSchema), test),
        "Should convert long to LocalDateTime");
  }

  @Test
  public void writeLocalTimestampMicros() throws IOException {
    LogicalType timestamp = LogicalTypes.localTimestampMicros();
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema timestampSchema = timestamp.addToSchema(Schema.create(Schema.Type.LONG));

    LocalDateTime i1 = LocalDateTime.of(1986, 6, 26, 12, 7, 11, 420000);
    LocalDateTime i2 = LocalDateTime.ofInstant(Instant.ofEpochSecond(0, 4000), ZoneOffset.UTC);

    Conversion<LocalDateTime> conversion = new TimeConversions.LocalTimestampMicrosConversion();

    Long d1long = conversion.toLong(i1, longSchema, timestamp);
    Long d2long = conversion.toLong(i2, longSchema, timestamp);
    List<Long> expected = Arrays.asList(d1long, d2long);

    File test = write(GENERIC, timestampSchema, i1, i2);
    assertEquals(expected, read(GenericData.get().createDatumReader(timestampSchema), test),
        "Should read LocalDateTime as longs");
  }

  @Test
  public void testReadAutomaticallyRegisteredUri() throws IOException {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    GenericData.setStringType(stringSchema, GenericData.StringType.String);
    LogicalType customType = LogicalTypes.getCustomRegisteredTypes().get("custom").fromSchema(stringSchema);
    Schema customTypeSchema = customType.addToSchema(Schema.create(Schema.Type.STRING));

    CustomType ct1 = new CustomType("foo");
    CustomType ct2 = new CustomType("bar");
    List<CustomType> expected = Arrays.asList(ct1, ct2);

    Conversion<CustomType> conversion = GENERIC.getConversionFor(customType);

    // use the conversion directly instead of relying on the write side
    CharSequence ct1String = conversion.toCharSequence(ct1, stringSchema, customType);
    CharSequence ct2String = conversion.toCharSequence(ct2, stringSchema, customType);

    File test = write(stringSchema, ct1String, ct2String);
    assertEquals(expected, read(GENERIC.createDatumReader(customTypeSchema), test),
        "Should convert string to CustomType");
  }

  @Test
  public void testWriteAutomaticallyRegisteredUri() throws IOException {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    GenericData.setStringType(stringSchema, GenericData.StringType.String);
    LogicalType customType = LogicalTypes.getCustomRegisteredTypes().get("custom").fromSchema(stringSchema);
    Schema customTypeSchema = customType.addToSchema(Schema.create(Schema.Type.STRING));

    CustomType ct1 = new CustomType("foo");
    CustomType ct2 = new CustomType("bar");

    Conversion<CustomType> conversion = GENERIC.getConversionFor(customType);

    // use the conversion directly instead of relying on the write side
    CharSequence ct1String = conversion.toCharSequence(ct1, stringSchema, customType);
    CharSequence ct2String = conversion.toCharSequence(ct2, stringSchema, customType);
    List<CharSequence> expected = Arrays.asList(ct1String, ct2String);

    File test = write(GENERIC, customTypeSchema, ct1, ct2);

    // Note that this test still cannot read strings using the logical type
    // schema, as all GenericData instances have the logical type and the
    // conversions loaded. That's why this final assert is slightly different.

    assertEquals(expected, read(GenericData.get().createDatumReader(stringSchema), test),
        "Should read CustomType as strings");
  }
}
