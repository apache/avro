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
package org.apache.avro;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestReadingWritingDataInEvolvedSchemas {

  private static final String RECORD_A = "RecordA";
  private static final String FIELD_A = "fieldA";
  private static final char LATIN_SMALL_LETTER_O_WITH_DIARESIS = '\u00F6';

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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
      .name(FIELD_A).type().unionOf().stringType().and().bytesType().endUnion()
      .noDefault() //
      .endRecord();
  private static final Schema ENUM_AB_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().enumeration("Enum1").symbols("A", "B").noDefault() //
      .endRecord();
  private static final Schema ENUM_ABC_RECORD = SchemaBuilder.record(RECORD_A) //
      .fields() //
      .name(FIELD_A).type().enumeration("Enum1").symbols("A", "B", "C").noDefault() //
      .endRecord();

  @Test
  public void doubleWrittenWithUnionSchemaIsConvertedToDoubleSchema() throws Exception {
    Schema writer = UNION_INT_LONG_FLOAT_DOUBLE_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42.0);
    byte[] encoded = encodeGenericBlob(record);
    Record decoded = decodeGenericBlob(DOUBLE_RECORD, writer, encoded);
    assertEquals(42.0, decoded.get(FIELD_A));
  }

  @Test
  public void doubleWrittenWithUnionSchemaIsNotConvertedToFloatSchema() throws Exception {
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("Found double, expecting float");
    Schema writer = UNION_INT_LONG_FLOAT_DOUBLE_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42.0);
    byte[] encoded = encodeGenericBlob(record);
    decodeGenericBlob(FLOAT_RECORD, writer, encoded);
  }

  @Test
  public void floatWrittenWithUnionSchemaIsNotConvertedToLongSchema() throws Exception {
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("Found float, expecting long");
    Schema writer = UNION_INT_LONG_FLOAT_DOUBLE_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42.0f);
    byte[] encoded = encodeGenericBlob(record);
    decodeGenericBlob(LONG_RECORD, writer, encoded);
  }

  @Test
  public void longWrittenWithUnionSchemaIsNotConvertedToIntSchema() throws Exception {
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("Found long, expecting int");
    Schema writer = UNION_INT_LONG_FLOAT_DOUBLE_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42L);
    byte[] encoded = encodeGenericBlob(record);
    decodeGenericBlob(INT_RECORD, writer, encoded);
  }

  @Test
  public void intWrittenWithUnionSchemaIsConvertedToAllNumberSchemas() throws Exception {
    Schema writer = UNION_INT_LONG_FLOAT_DOUBLE_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42);
    byte[] encoded = encodeGenericBlob(record);
    assertEquals(42.0, decodeGenericBlob(DOUBLE_RECORD, writer, encoded).get(FIELD_A));
    assertEquals(42.0f, decodeGenericBlob(FLOAT_RECORD, writer, encoded).get(FIELD_A));
    assertEquals(42L, decodeGenericBlob(LONG_RECORD, writer, encoded).get(FIELD_A));
    assertEquals(42, decodeGenericBlob(INT_RECORD, writer, encoded).get(FIELD_A));
  }

  @Test
  public void asciiStringWrittenWithUnionSchemaIsConvertedToBytesSchema() throws Exception {
    Schema writer = UNION_STRING_BYTES_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, "42");
    byte[] encoded = encodeGenericBlob(record);
    ByteBuffer actual = (ByteBuffer) decodeGenericBlob(BYTES_RECORD, writer, encoded).get(FIELD_A);
    assertArrayEquals("42".getBytes("UTF-8"), actual.array());
  }

  @Test
  public void utf8StringWrittenWithUnionSchemaIsConvertedToBytesSchema() throws Exception {
    String goeran = String.format("G%sran", LATIN_SMALL_LETTER_O_WITH_DIARESIS);
    Schema writer = UNION_STRING_BYTES_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, goeran);
    byte[] encoded = encodeGenericBlob(record);
    ByteBuffer actual = (ByteBuffer) decodeGenericBlob(BYTES_RECORD, writer, encoded).get(FIELD_A);
    assertArrayEquals(goeran.getBytes("UTF-8"), actual.array());
  }

  @Test
  public void asciiBytesWrittenWithUnionSchemaIsConvertedToStringSchema() throws Exception {
    Schema writer = UNION_STRING_BYTES_RECORD;
    ByteBuffer buf = ByteBuffer.wrap("42".getBytes("UTF-8"));
    Record record = defaultRecordWithSchema(writer, FIELD_A, buf);
    byte[] encoded = encodeGenericBlob(record);
    CharSequence read =  (CharSequence) decodeGenericBlob(STRING_RECORD, writer, encoded).get(FIELD_A);
    assertEquals("42", read.toString());
  }

  @Test
  public void utf8BytesWrittenWithUnionSchemaIsConvertedToStringSchema() throws Exception {
    String goeran = String.format("G%sran", LATIN_SMALL_LETTER_O_WITH_DIARESIS);
    Schema writer = UNION_STRING_BYTES_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, goeran);
    byte[] encoded = encodeGenericBlob(record);
    CharSequence read =  (CharSequence) decodeGenericBlob(STRING_RECORD, writer, encoded).get(FIELD_A);
    assertEquals(goeran, read.toString());
  }

  @Test
  public void enumRecordCanBeReadWithExtendedEnumSchema() throws Exception {
    Schema writer = ENUM_AB_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, new EnumSymbol(writer, "A"));
    byte[] encoded = encodeGenericBlob(record);
    Record decoded = decodeGenericBlob(ENUM_ABC_RECORD, writer, encoded);
    assertEquals("A", decoded.get(FIELD_A).toString());
  }

  @Test
  public void enumRecordWithExtendedSchemaCanBeReadWithOriginalEnumSchemaIfOnlyOldValues() throws Exception {
    Schema writer = ENUM_ABC_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, new EnumSymbol(writer, "A"));
    byte[] encoded = encodeGenericBlob(record);
    Record decoded = decodeGenericBlob(ENUM_AB_RECORD, writer, encoded);
    assertEquals("A", decoded.get(FIELD_A).toString());
  }

  @Test
  public void enumRecordWithExtendedSchemaCanNotBeReadIfNewValuesAreUsed() throws Exception {
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("No match for C");
    Schema writer = ENUM_ABC_RECORD;
    Record record = defaultRecordWithSchema(writer, FIELD_A, new EnumSymbol(writer, "C"));
    byte[] encoded = encodeGenericBlob(record);
    decodeGenericBlob(ENUM_AB_RECORD, writer, encoded);
  }

  @Test
  public void recordWrittenWithExtendedSchemaCanBeReadWithOriginalSchemaButLossOfData() throws Exception {
    Schema writer = SchemaBuilder.record(RECORD_A) //
        .fields() //
        .name("newTopField").type().stringType().noDefault() //
        .name(FIELD_A).type().intType().noDefault() //
        .endRecord();
    Record record = defaultRecordWithSchema(writer, FIELD_A, 42);
    record.put("newTopField", "not decoded");
    byte[] encoded = encodeGenericBlob(record);
    Record decoded = decodeGenericBlob(INT_RECORD, writer, encoded);
    assertEquals(42, decoded.get(FIELD_A));
    assertNull(decoded.get("newTopField"));
  }

  @Test
  public void readerWithoutDefaultValueThrowsException() throws Exception {
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("missing required field newField");
    Schema reader = SchemaBuilder.record(RECORD_A) //
        .fields() //
        .name("newField").type().intType().noDefault() //
        .name(FIELD_A).type().intType().noDefault() //
        .endRecord();
    Record record = defaultRecordWithSchema(INT_RECORD, FIELD_A, 42);
    byte[] encoded = encodeGenericBlob(record);
    decodeGenericBlob(reader, INT_RECORD, encoded);
  }

  @Test
  public void readerWithDefaultValueIsApplied() throws Exception {
    Schema reader = SchemaBuilder.record(RECORD_A) //
        .fields() //
        .name("newFieldWithDefault").type().intType().intDefault(314) //
        .name(FIELD_A).type().intType().noDefault() //
        .endRecord();
    Record record = defaultRecordWithSchema(INT_RECORD, FIELD_A, 42);
    byte[] encoded = encodeGenericBlob(record);
    Record decoded = decodeGenericBlob(reader, INT_RECORD, encoded);
    assertEquals(42, decoded.get(FIELD_A));
    assertEquals(314, decoded.get("newFieldWithDefault"));
  }

  private <T> Record defaultRecordWithSchema(Schema schema, String key, T value) {
    Record data = new GenericData.Record(schema);
    data.put(key, value);
    return data;
  }

  private static byte[] encodeGenericBlob(GenericRecord data)
      throws IOException {
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(data.getSchema());
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
    writer.write(data, encoder);
    encoder.flush();
    outStream.close();
    return outStream.toByteArray();
  }

  private static Record decodeGenericBlob(Schema expectedSchema, Schema schemaOfBlob, byte[] blob) throws IOException {
    if (blob == null) {
      return null;
    }
    GenericDatumReader<Record> reader = new GenericDatumReader<Record>();
    reader.setExpected(expectedSchema);
    reader.setSchema(schemaOfBlob);
    Decoder decoder = DecoderFactory.get().binaryDecoder(blob, null);
    Record data = null;
    data = reader.read(null, decoder);
    return data;
  }
}
