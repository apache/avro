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
package org.apache.avro;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestReadingWritingDataInEvolvedSchemas {

  private static final String RECORD_A = "RecordA";
  private static final String FIELD_A = "fieldA";
  private static final char LATIN_SMALL_LETTER_O_WITH_DIARESIS = '\u00F6';

  private static final Schema DOUBLE_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().doubleType().noDefault() //
      .endRecord();
  private static final Schema FLOAT_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().floatType().noDefault() //
      .endRecord();
  private static final Schema LONG_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().longType().noDefault() //
      .endRecord();
  private static final Schema INT_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().intType().noDefault() //
      .endRecord();
  private static final Schema UNION_INT_LONG_FLOAT_DOUBLE_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().unionOf().doubleType().and().floatType().and().longType().and().intType().endUnion()
      .noDefault() //
      .endRecord();
  private static final Schema STRING_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().stringType().noDefault() //
      .endRecord();
  private static final Schema BYTES_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().bytesType().noDefault() //
      .endRecord();
  private static final Schema UNION_STRING_BYTES_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().unionOf().stringType().and().bytesType().endUnion().noDefault() //
      .endRecord();

  private static final Schema ENUM_AB = SchemaBuilder.enumeration("Enum1").symbols("A", "B");

  private static final Schema ENUM_AB_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type(ENUM_AB).noDefault() //
      .endRecord();

  private static final Schema ENUM_ABC = SchemaBuilder.enumeration("Enum1").symbols("A", "B", "C");
  private static final Schema ENUM_ABC_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type(ENUM_ABC).noDefault() //
      .endRecord();
  private static final Schema UNION_INT_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().unionOf().intType().endUnion().noDefault() //
      .endRecord();
  private static final Schema UNION_LONG_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().unionOf().longType().endUnion().noDefault() //
      .endRecord();
  private static final Schema UNION_FLOAT_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().unionOf().floatType().endUnion().noDefault() //
      .endRecord();
  private static final Schema UNION_DOUBLE_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().unionOf().doubleType().endUnion().noDefault() //
      .endRecord();
  private static final Schema UNION_LONG_FLOAT_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().unionOf().floatType().and().longType().endUnion().noDefault() //
      .endRecord();
  private static final Schema UNION_FLOAT_DOUBLE_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().unionOf().floatType().and().doubleType().endUnion().noDefault() //
      .endRecord();

  enum EncoderType {
    BINARY, JSON
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void doubleWrittenWithUnionSchemaIsConvertedToDoubleSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_INT_LONG_FLOAT_DOUBLE_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42.0);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    Record decoded = decodeGenericBlob(DOUBLE_RECORD, writer, encoded, encoderType);
    assertEquals(42.0, decoded.get(FIELD_A));
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void longWrittenWithUnionSchemaIsConvertedToUnionLongFloatSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_LONG_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42L);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    Record decoded = decodeGenericBlob(UNION_LONG_FLOAT_RECORD, writer, encoded, encoderType);
    assertEquals(42L, decoded.get(FIELD_A));
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void longWrittenWithUnionSchemaIsConvertedToDoubleSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_LONG_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42L);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    Record decoded = decodeGenericBlob(UNION_DOUBLE_RECORD, writer, encoded, encoderType);
    assertEquals(42.0, decoded.get(FIELD_A));
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void intWrittenWithUnionSchemaIsConvertedToDoubleSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_INT_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    Record decoded = decodeGenericBlob(UNION_DOUBLE_RECORD, writer, encoded, encoderType);
    assertEquals(42.0, decoded.get(FIELD_A));
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void intWrittenWithUnionSchemaIsReadableByFloatSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_INT_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    Record decoded = decodeGenericBlob(FLOAT_RECORD, writer, encoded, encoderType);
    assertEquals(42.0f, decoded.get(FIELD_A));
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void intWrittenWithUnionSchemaIsReadableByFloatUnionSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_INT_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    Record decoded = decodeGenericBlob(UNION_FLOAT_RECORD, writer, encoded, encoderType);
    assertEquals(42.0f, decoded.get(FIELD_A));
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void longWrittenWithUnionSchemaIsReadableByFloatSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_LONG_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42L);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    Record decoded = decodeGenericBlob(FLOAT_RECORD, writer, encoded, encoderType);
    assertEquals(42.0f, decoded.get(FIELD_A));
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void longWrittenWithUnionSchemaIsReadableByFloatUnionSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_LONG_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42L);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    Record decoded = decodeGenericBlob(UNION_FLOAT_RECORD, writer, encoded, encoderType);
    assertEquals(42.0f, decoded.get(FIELD_A));
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void longWrittenWithUnionSchemaIsConvertedToLongFloatUnionSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_LONG_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42L);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    Record decoded = decodeGenericBlob(UNION_LONG_FLOAT_RECORD, writer, encoded, encoderType);
    assertEquals(42L, decoded.get(FIELD_A));
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void longWrittenWithUnionSchemaIsConvertedToFloatDoubleUnionSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_LONG_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42L);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    Record decoded = decodeGenericBlob(UNION_FLOAT_DOUBLE_RECORD, writer, encoded, encoderType);
    assertEquals(42.0F, decoded.get(FIELD_A));
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void doubleWrittenWithUnionSchemaIsNotConvertedToFloatSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_INT_LONG_FLOAT_DOUBLE_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42.0);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    AvroTypeException exception = Assertions.assertThrows(AvroTypeException.class,
        () -> decodeGenericBlob(FLOAT_RECORD, writer, encoded, encoderType));
    Assertions.assertEquals("Field \"fieldA\" content mismatch: Found double, expecting float", exception.getMessage());
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void floatWrittenWithUnionSchemaIsNotConvertedToLongSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_INT_LONG_FLOAT_DOUBLE_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42.0f);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    AvroTypeException exception = Assertions.assertThrows(AvroTypeException.class,
        () -> decodeGenericBlob(LONG_RECORD, writer, encoded, encoderType));
    Assertions.assertEquals("Field \"fieldA\" content mismatch: Found float, expecting long", exception.getMessage());
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void longWrittenWithUnionSchemaIsNotConvertedToIntSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_INT_LONG_FLOAT_DOUBLE_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42L);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    AvroTypeException exception = Assertions.assertThrows(AvroTypeException.class,
        () -> decodeGenericBlob(INT_RECORD, writer, encoded, encoderType));
    Assertions.assertEquals("Field \"fieldA\" content mismatch: Found long, expecting int", exception.getMessage());
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void intWrittenWithUnionSchemaIsConvertedToAllNumberSchemas(EncoderType encoderType) throws Exception {
    Schema writer = UNION_INT_LONG_FLOAT_DOUBLE_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    assertEquals(42.0, decodeGenericBlob(DOUBLE_RECORD, writer, encoded, encoderType).get(FIELD_A));
    assertEquals(42.0f, decodeGenericBlob(FLOAT_RECORD, writer, encoded, encoderType).get(FIELD_A));
    assertEquals(42L, decodeGenericBlob(LONG_RECORD, writer, encoded, encoderType).get(FIELD_A));
    assertEquals(42, decodeGenericBlob(INT_RECORD, writer, encoded, encoderType).get(FIELD_A));
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void asciiStringWrittenWithUnionSchemaIsConvertedToBytesSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_STRING_BYTES_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, "42");
    byte[] encoded = encodeGenericBlob(record, encoderType);
    ByteBuffer actual = (ByteBuffer) decodeGenericBlob(BYTES_RECORD, writer, encoded, encoderType).get(FIELD_A);
    assertArrayEquals("42".getBytes(StandardCharsets.UTF_8), actual.array());
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void utf8StringWrittenWithUnionSchemaIsConvertedToBytesSchema(EncoderType encoderType) throws Exception {
    String goeran = String.format("G%sran", LATIN_SMALL_LETTER_O_WITH_DIARESIS);
    Schema writer = UNION_STRING_BYTES_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, goeran);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    ByteBuffer actual = (ByteBuffer) decodeGenericBlob(BYTES_RECORD, writer, encoded, encoderType).get(FIELD_A);
    assertArrayEquals(goeran.getBytes(StandardCharsets.UTF_8), actual.array());
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void asciiBytesWrittenWithUnionSchemaIsConvertedToStringSchema(EncoderType encoderType) throws Exception {
    Schema writer = UNION_STRING_BYTES_RECORD;
    ByteBuffer buf = ByteBuffer.wrap("42".getBytes(StandardCharsets.UTF_8));
    Record record = defaultRecordWithSchema(writer, FIELD_A, buf);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    CharSequence read = (CharSequence) decodeGenericBlob(STRING_RECORD, writer, encoded, encoderType).get(FIELD_A);
    assertEquals("42", read.toString());
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void utf8BytesWrittenWithUnionSchemaIsConvertedToStringSchema(EncoderType encoderType) throws Exception {
    String goeran = String.format("G%sran", LATIN_SMALL_LETTER_O_WITH_DIARESIS);
    Schema writer = UNION_STRING_BYTES_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, goeran);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    CharSequence read = (CharSequence) decodeGenericBlob(STRING_RECORD, writer, encoded, encoderType).get(FIELD_A);
    assertEquals(goeran, read.toString());
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void enumRecordCanBeReadWithExtendedEnumSchema(EncoderType encoderType) throws Exception {
    Schema writer = ENUM_AB_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, new EnumSymbol(ENUM_AB, "A"));
    byte[] encoded = encodeGenericBlob(record, encoderType);
    Record decoded = decodeGenericBlob(ENUM_ABC_RECORD, writer, encoded, encoderType);
    assertEquals("A", decoded.get(FIELD_A).toString());
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void enumRecordWithExtendedSchemaCanBeReadWithOriginalEnumSchemaIfOnlyOldValues(EncoderType encoderType)
      throws Exception {
    Schema writer = ENUM_ABC_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, new EnumSymbol(ENUM_ABC, "A"));
    byte[] encoded = encodeGenericBlob(record, encoderType);
    Record decoded = decodeGenericBlob(ENUM_AB_RECORD, writer, encoded, encoderType);
    assertEquals("A", decoded.get(FIELD_A).toString());
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void enumRecordWithExtendedSchemaCanNotBeReadIfNewValuesAreUsed(EncoderType encoderType) throws Exception {
    Schema writer = ENUM_ABC_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, new EnumSymbol(ENUM_ABC, "C"));
    byte[] encoded = encodeGenericBlob(record, encoderType);

    AvroTypeException exception = Assertions.assertThrows(AvroTypeException.class,
        () -> decodeGenericBlob(ENUM_AB_RECORD, writer, encoded, encoderType));
    Assertions.assertEquals("Field \"fieldA\" content mismatch: No match for C", exception.getMessage());
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void recordWrittenWithExtendedSchemaCanBeReadWithOriginalSchemaButLossOfData(EncoderType encoderType)
      throws Exception {
    Schema writer = SchemaBuilder.record(RECORD_A) //
        .fields() //
        .name("newTopField").type().stringType().noDefault() //
        .name(FIELD_A).type().intType().noDefault() //
        .endRecord();
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42);
    record.put("newTopField", "not decoded");
    byte[] encoded = encodeGenericBlob(record, encoderType);
    Record decoded = decodeGenericBlob(INT_RECORD, writer, encoded, encoderType);
    assertEquals(42, decoded.get(FIELD_A));
    try {
      decoded.get("newTopField");
      Assertions.fail("get should throw a exception");
    } catch (AvroRuntimeException ex) {
      Assertions.assertEquals("Not a valid schema field: newTopField", ex.getMessage());
    }
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void readerWithoutDefaultValueThrowsException(EncoderType encoderType) throws Exception {
    Schema reader = SchemaBuilder.record(RECORD_A) //
        .fields() //
        .name("newField").type().intType().noDefault() //
        .name(FIELD_A).type().intType().noDefault() //
        .endRecord();
    Record record = defaultRecordWithSchema(INT_RECORD, FIELD_A, 42);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    AvroTypeException exception = Assertions.assertThrows(AvroTypeException.class,
        () -> decodeGenericBlob(reader, INT_RECORD, encoded, encoderType));
    Assertions.assertTrue(exception.getMessage().contains("missing required field newField"), exception.getMessage());
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void readerWithDefaultValueIsApplied(EncoderType encoderType) throws Exception {
    Schema reader = SchemaBuilder.record(RECORD_A) //
        .fields() //
        .name("newFieldWithDefault").type().intType().intDefault(314) //
        .name(FIELD_A).type().intType().noDefault() //
        .endRecord();
    Record record = defaultRecordWithSchema(INT_RECORD, FIELD_A, 42);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    Record decoded = decodeGenericBlob(reader, INT_RECORD, encoded, encoderType);
    assertEquals(42, decoded.get(FIELD_A));
    assertEquals(314, decoded.get("newFieldWithDefault"));
  }

  @ParameterizedTest
  @EnumSource(EncoderType.class)
  void aliasesInSchema(EncoderType encoderType) throws Exception {
    Schema writer = new Schema.Parser()
        .parse("{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"User\", \"fields\": ["
            + "{\"name\": \"name\", \"type\": \"int\"}\n" + "]}\n");
    Schema reader = new Schema.Parser()
        .parse("{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"User\", \"fields\": ["
            + "{\"name\": \"fname\", \"type\": \"int\", \"aliases\" : [ \"name\" ]}\n" + "]}\n");

    GenericData.Record record = defaultRecordWithSchema(writer, "name", 1);
    byte[] encoded = encodeGenericBlob(record, encoderType);
    GenericData.Record decoded = decodeGenericBlob(reader, reader, encoded, encoderType);

    assertEquals(1, decoded.get("fname"));
  }

  private <T> Record defaultRecordWithSchema(Schema schema, String key, T value) {
    Record data = new GenericData.Record(schema);
    data.put(key, value);
    return data;
  }

  private byte[] encodeGenericBlob(GenericRecord data, EncoderType encoderType) throws IOException {
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(data.getSchema());
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    Encoder encoder = encoderType == EncoderType.BINARY ? EncoderFactory.get().binaryEncoder(outStream, null)
        : EncoderFactory.get().jsonEncoder(data.getSchema(), outStream);
    writer.write(data, encoder);
    encoder.flush();
    outStream.close();
    return outStream.toByteArray();
  }

  private Record decodeGenericBlob(Schema expectedSchema, Schema schemaOfBlob, byte[] blob, EncoderType encoderType)
      throws IOException {
    if (blob == null) {
      return null;
    }
    GenericDatumReader<Record> reader = new GenericDatumReader<>();
    reader.setExpected(expectedSchema);
    reader.setSchema(schemaOfBlob);
    Decoder decoder = encoderType == EncoderType.BINARY ? DecoderFactory.get().binaryDecoder(blob, null)
        : DecoderFactory.get().jsonDecoder(schemaOfBlob, new ByteArrayInputStream(blob));
    return reader.read(null, decoder);
  }
}
