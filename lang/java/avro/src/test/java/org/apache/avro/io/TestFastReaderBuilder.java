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
package org.apache.avro.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDate;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.data.TimeConversions.DateConversion;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.jupiter.api.Test;

/**
 * AVRO-4215: when the writer schema selects the value through a union, the
 * logical type conversion must be applied exactly once.
 */
public class TestFastReaderBuilder {

  private static final int EPOCH_DAYS = 1000;

  private static final LocalDate EXPECTED_DATE = LocalDate.ofEpochDay(EPOCH_DAYS);

  private static final Schema WRITER_INT = Schema.create(Schema.Type.INT);

  private static final Schema WRITER_UNION_INT = SchemaBuilder.unionOf().intType().endUnion();

  private static final Schema WRITER_NULLABLE_INT = SchemaBuilder.unionOf().nullType().and().intType().endUnion();

  private static final Schema READER_DATE = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));

  private static final Schema READER_NULLABLE_DATE = SchemaBuilder.unionOf().nullType().and().type(READER_DATE)
      .endUnion();

  private static final Schema WRITER_UNION_INT_RECORD = SchemaBuilder.record("DateRecord")
      .namespace("org.apache.avro.io.TestFastReaderBuilder").fields().name("date").type(WRITER_UNION_INT).noDefault()
      .endRecord();

  private static final Schema READER_DATE_RECORD = SchemaBuilder.record("DateRecord")
      .namespace("org.apache.avro.io.TestFastReaderBuilder").fields().name("date").type(READER_DATE).noDefault()
      .endRecord();

  private static GenericData newDataModel(boolean fastReaderEnabled) {
    GenericData model = new GenericData();
    model.addLogicalTypeConversion(new DateConversion());
    model.setFastReaderEnabled(fastReaderEnabled);
    return model;
  }

  private static byte[] write(Schema writerSchema, GenericData model, Object datum) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<>(writerSchema, model).write(datum, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  private static Object read(byte[] serialized, Schema writerSchema, Schema readerSchema, GenericData model)
      throws IOException {
    try (ByteArrayInputStream in = new ByteArrayInputStream(serialized)) {
      Decoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
      GenericDatumReader<Object> reader = new GenericDatumReader<>(writerSchema, readerSchema, model);
      return reader.read(null, decoder);
    }
  }

  @Test
  void writerUnionIntToDateWithFastReader() throws IOException {
    GenericData model = newDataModel(true);
    byte[] serialized = write(WRITER_UNION_INT, model, EPOCH_DAYS);

    Object datum = read(serialized, WRITER_UNION_INT, READER_DATE, model);

    assertInstanceOf(LocalDate.class, datum);
    assertEquals(EXPECTED_DATE, datum);
  }

  @Test
  void writerUnionIntToDateWithResolvingDecoder() throws IOException {
    GenericData model = newDataModel(false);
    byte[] serialized = write(WRITER_UNION_INT, model, EPOCH_DAYS);

    Object datum = read(serialized, WRITER_UNION_INT, READER_DATE, model);

    assertInstanceOf(LocalDate.class, datum);
    assertEquals(EXPECTED_DATE, datum);
  }

  @Test
  void writerUnionIntToDateWithoutRegisteredConversion() throws IOException {
    GenericData model = new GenericData();
    model.setFastReaderEnabled(true);
    byte[] serialized = write(WRITER_UNION_INT, model, EPOCH_DAYS);

    assertEquals(EPOCH_DAYS, read(serialized, WRITER_UNION_INT, READER_DATE, model));
  }

  @Test
  void readerUnionDateWithFastReader() throws IOException {
    GenericData model = newDataModel(true);
    byte[] serialized = write(WRITER_INT, model, EPOCH_DAYS);

    Object datum = read(serialized, WRITER_INT, READER_NULLABLE_DATE, model);

    assertInstanceOf(LocalDate.class, datum);
    assertEquals(EXPECTED_DATE, datum);
  }

  @Test
  void nullableWriterUnionIntToDateWithFastReader() throws IOException {
    GenericData model = newDataModel(true);

    Object date = read(write(WRITER_NULLABLE_INT, model, EPOCH_DAYS), WRITER_NULLABLE_INT, READER_NULLABLE_DATE, model);
    assertInstanceOf(LocalDate.class, date);
    assertEquals(EXPECTED_DATE, date);

    Object nullDatum = read(write(WRITER_NULLABLE_INT, model, null), WRITER_NULLABLE_INT, READER_NULLABLE_DATE, model);
    assertNull(nullDatum);
  }

  @Test
  void nullableWriterUnionIntToDateWithResolvingDecoder() throws IOException {
    GenericData model = newDataModel(false);

    Object date = read(write(WRITER_NULLABLE_INT, model, EPOCH_DAYS), WRITER_NULLABLE_INT, READER_NULLABLE_DATE, model);
    assertInstanceOf(LocalDate.class, date);
    assertEquals(EXPECTED_DATE, date);

    Object nullDatum = read(write(WRITER_NULLABLE_INT, model, null), WRITER_NULLABLE_INT, READER_NULLABLE_DATE, model);
    assertNull(nullDatum);
  }

  @Test
  void specificRecordUsesFieldConversionOnceForWriterUnion() throws IOException {
    CountingDateConversion registeredConversion = new CountingDateConversion();
    CountingDateConversion fieldConversion = new CountingDateConversion();
    IsolatedSpecificData model = new IsolatedSpecificData(registeredConversion, fieldConversion);
    model.setFastReaderEnabled(true);

    GenericData.Record writerDatum = new GenericData.Record(WRITER_UNION_INT_RECORD);
    writerDatum.put("date", EPOCH_DAYS);
    byte[] serialized = write(WRITER_UNION_INT_RECORD, new GenericData(), writerDatum);

    Object datum;
    try (ByteArrayInputStream in = new ByteArrayInputStream(serialized)) {
      Decoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
      DatumReader<Object> reader = new FastReaderBuilder(model).createDatumReader(WRITER_UNION_INT_RECORD,
          READER_DATE_RECORD);
      datum = reader.read(null, decoder);
    }

    assertInstanceOf(DateRecord.class, datum);
    assertEquals(EXPECTED_DATE, ((DateRecord) datum).get(0));
    assertEquals(1, fieldConversion.calls());
    assertEquals(0, registeredConversion.calls());
  }

  private static class CountingDateConversion extends DateConversion {
    private int calls;

    @Override
    public LocalDate fromInt(Integer value, Schema schema, LogicalType type) {
      calls++;
      return super.fromInt(value, schema, type);
    }

    int calls() {
      return calls;
    }
  }

  /**
   * Hand-written specific record; the schema namespace is the enclosing class so
   * the class can be resolved as a nested class.
   */
  public static class DateRecord extends SpecificRecordBase {
    public static final Schema SCHEMA$ = READER_DATE_RECORD;

    private final Conversion<?> fieldConversion;

    private LocalDate date;

    public DateRecord() {
      this(null);
    }

    DateRecord(Conversion<?> fieldConversion) {
      this.fieldConversion = fieldConversion;
    }

    public static Schema getClassSchema() {
      return SCHEMA$;
    }

    @Override
    public Schema getSchema() {
      return SCHEMA$;
    }

    @Override
    public Object get(int field) {
      if (field != 0) {
        throw new IndexOutOfBoundsException("Invalid field index: " + field);
      }
      return date;
    }

    @Override
    public void put(int field, Object value) {
      if (field != 0) {
        throw new IndexOutOfBoundsException("Invalid field index: " + field);
      }
      this.date = (LocalDate) value;
    }

    @Override
    public Conversion<?> getConversion(int field) {
      return field == 0 ? fieldConversion : null;
    }
  }

  /** Isolated data model: never mutates the global SpecificData singleton. */
  private static final class IsolatedSpecificData extends SpecificData {
    private final Conversion<?> fieldConversion;

    IsolatedSpecificData(CountingDateConversion registeredConversion, Conversion<?> fieldConversion) {
      this.fieldConversion = fieldConversion;
      if (registeredConversion != null) {
        addLogicalTypeConversion(registeredConversion);
      }
    }

    @Override
    public GenericData.InstanceSupplier getNewRecordSupplier(Schema schema) {
      if (DateRecord.SCHEMA$.getFullName().equals(schema.getFullName())) {
        final Conversion<?> conversion = fieldConversion;
        return (old, targetSchema) -> new DateRecord(conversion);
      }
      return super.getNewRecordSupplier(schema);
    }
  }
}
