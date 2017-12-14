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
package org.apache.avro;

import static java.util.Arrays.asList;
import static org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility;
import static org.apache.avro.TestSchemas.A_DINT_B_DINT_RECORD1;
import static org.apache.avro.TestSchemas.A_DINT_RECORD1;
import static org.apache.avro.TestSchemas.A_INT_B_DINT_RECORD1;
import static org.apache.avro.TestSchemas.A_INT_B_INT_RECORD1;
import static org.apache.avro.TestSchemas.A_INT_RECORD1;
import static org.apache.avro.TestSchemas.A_LONG_RECORD1;
import static org.apache.avro.TestSchemas.BOOLEAN_SCHEMA;
import static org.apache.avro.TestSchemas.BYTES_SCHEMA;
import static org.apache.avro.TestSchemas.BYTES_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.DOUBLE_SCHEMA;
import static org.apache.avro.TestSchemas.DOUBLE_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.EMPTY_RECORD1;
import static org.apache.avro.TestSchemas.EMPTY_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.ENUM1_ABC_SCHEMA;
import static org.apache.avro.TestSchemas.ENUM1_AB_SCHEMA;
import static org.apache.avro.TestSchemas.ENUM1_BC_SCHEMA;
import static org.apache.avro.TestSchemas.FIXED_4_BYTES;
import static org.apache.avro.TestSchemas.FLOAT_SCHEMA;
import static org.apache.avro.TestSchemas.FLOAT_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.INT_ARRAY_SCHEMA;
import static org.apache.avro.TestSchemas.INT_FLOAT_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.INT_LIST_RECORD;
import static org.apache.avro.TestSchemas.INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.INT_LONG_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.INT_MAP_SCHEMA;
import static org.apache.avro.TestSchemas.INT_SCHEMA;
import static org.apache.avro.TestSchemas.INT_STRING_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.INT_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.LONG_ARRAY_SCHEMA;
import static org.apache.avro.TestSchemas.LONG_LIST_RECORD;
import static org.apache.avro.TestSchemas.LONG_MAP_SCHEMA;
import static org.apache.avro.TestSchemas.LONG_SCHEMA;
import static org.apache.avro.TestSchemas.LONG_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.NULL_SCHEMA;
import static org.apache.avro.TestSchemas.STRING_ARRAY_SCHEMA;
import static org.apache.avro.TestSchemas.STRING_INT_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.STRING_SCHEMA;
import static org.apache.avro.TestSchemas.STRING_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.assertSchemaContains;
import static org.apache.avro.TestSchemas.list;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import org.apache.avro.SchemaCompatibility.Incompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityResult;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;
import org.apache.avro.TestSchemas.ReaderWriter;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit-tests for SchemaCompatibility. */
public class TestSchemaCompatibility {
  private static final Logger LOG = LoggerFactory.getLogger(TestSchemaCompatibility.class);
  // -----------------------------------------------------------------------------------------------

  private static final Schema WRITER_SCHEMA = Schema.createRecord(list(
      new Schema.Field("oldfield1", INT_SCHEMA, null, null),
      new Schema.Field("oldfield2", STRING_SCHEMA, null, null)));

  @Test
  public void testValidateSchemaPairMissingField() throws Exception {
    final List<Schema.Field> readerFields = list(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null));
    final Schema reader = Schema.createRecord(readerFields);
    final SchemaCompatibility.SchemaPairCompatibility expectedResult =
        new SchemaCompatibility.SchemaPairCompatibility(
            SchemaCompatibility.SchemaCompatibilityResult.compatible(),
            reader,
            WRITER_SCHEMA,
            SchemaCompatibility.READER_WRITER_COMPATIBLE_MESSAGE);

    // Test omitting a field.
    assertEquals(expectedResult, checkReaderWriterCompatibility(reader, WRITER_SCHEMA));
  }

  @Test
  public void testValidateSchemaPairMissingSecondField() throws Exception {
    final List<Schema.Field> readerFields = list(
        new Schema.Field("oldfield2", STRING_SCHEMA, null, null));
    final Schema reader = Schema.createRecord(readerFields);
    final SchemaCompatibility.SchemaPairCompatibility expectedResult =
        new SchemaCompatibility.SchemaPairCompatibility(
            SchemaCompatibility.SchemaCompatibilityResult.compatible(),
            reader,
            WRITER_SCHEMA,
            SchemaCompatibility.READER_WRITER_COMPATIBLE_MESSAGE);

    // Test omitting other field.
    assertEquals(expectedResult, checkReaderWriterCompatibility(reader, WRITER_SCHEMA));
  }

  @Test
  public void testValidateSchemaPairAllFields() throws Exception {
    final List<Schema.Field> readerFields = list(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("oldfield2", STRING_SCHEMA, null, null));
    final Schema reader = Schema.createRecord(readerFields);
    final SchemaCompatibility.SchemaPairCompatibility expectedResult =
        new SchemaCompatibility.SchemaPairCompatibility(
            SchemaCompatibility.SchemaCompatibilityResult.compatible(),
            reader,
            WRITER_SCHEMA,
            SchemaCompatibility.READER_WRITER_COMPATIBLE_MESSAGE);

    // Test with all fields.
    assertEquals(expectedResult, checkReaderWriterCompatibility(reader, WRITER_SCHEMA));
  }

  @Test
  public void testValidateSchemaNewFieldWithDefault() throws Exception {
    final List<Schema.Field> readerFields = list(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("newfield1", INT_SCHEMA, null, 42));
    final Schema reader = Schema.createRecord(readerFields);
    final SchemaCompatibility.SchemaPairCompatibility expectedResult =
        new SchemaCompatibility.SchemaPairCompatibility(
            SchemaCompatibility.SchemaCompatibilityResult.compatible(),
            reader,
            WRITER_SCHEMA,
            SchemaCompatibility.READER_WRITER_COMPATIBLE_MESSAGE);

    // Test new field with default value.
    assertEquals(expectedResult, checkReaderWriterCompatibility(reader, WRITER_SCHEMA));
  }

  @Test
  public void testValidateSchemaNewField() throws Exception {
    final List<Schema.Field> readerFields = list(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("newfield1", INT_SCHEMA, null, null));
    final Schema reader = Schema.createRecord(readerFields);
    SchemaPairCompatibility compatibility = checkReaderWriterCompatibility(reader, WRITER_SCHEMA);

    // Test new field without default value.
    assertEquals(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE, compatibility.getType());
    assertEquals(SchemaCompatibility.SchemaCompatibilityResult.incompatible(
        SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE, reader, WRITER_SCHEMA,
        "newfield1", asList("", "fields", "1")), compatibility.getResult());
    assertEquals(
        String.format(
            "Data encoded using writer schema:%n%s%n"
                + "will or may fail to decode using reader schema:%n%s%n",
            WRITER_SCHEMA.toString(true), reader.toString(true)),
        compatibility.getDescription());
    assertEquals(reader, compatibility.getReader());
    assertEquals(WRITER_SCHEMA, compatibility.getWriter());
  }

  @Test
  public void testValidateArrayWriterSchema() throws Exception {
    final Schema validReader = Schema.createArray(STRING_SCHEMA);
    final Schema invalidReader = Schema.createMap(STRING_SCHEMA);
    final SchemaCompatibility.SchemaPairCompatibility validResult =
        new SchemaCompatibility.SchemaPairCompatibility(
            SchemaCompatibility.SchemaCompatibilityResult.compatible(),
            validReader,
            STRING_ARRAY_SCHEMA,
            SchemaCompatibility.READER_WRITER_COMPATIBLE_MESSAGE);
    final SchemaCompatibility.SchemaPairCompatibility invalidResult =
        new SchemaCompatibility.SchemaPairCompatibility(
            SchemaCompatibility.SchemaCompatibilityResult.incompatible(
            SchemaIncompatibilityType.TYPE_MISMATCH,
            invalidReader,
            STRING_ARRAY_SCHEMA,
            "reader type: MAP not compatible with writer type: ARRAY",
            asList("")),
            invalidReader,
            STRING_ARRAY_SCHEMA,
            String.format(
                "Data encoded using writer schema:%n%s%n"
                + "will or may fail to decode using reader schema:%n%s%n",
                STRING_ARRAY_SCHEMA.toString(true),
                invalidReader.toString(true)));

    assertEquals(
        validResult,
        checkReaderWriterCompatibility(validReader, STRING_ARRAY_SCHEMA));
    assertEquals(
        invalidResult,
        checkReaderWriterCompatibility(invalidReader, STRING_ARRAY_SCHEMA));
  }

  @Test
  public void testValidatePrimitiveWriterSchema() throws Exception {
    final Schema validReader = Schema.create(Schema.Type.STRING);
    final SchemaCompatibility.SchemaPairCompatibility validResult =
        new SchemaCompatibility.SchemaPairCompatibility(
            SchemaCompatibility.SchemaCompatibilityResult.compatible(),
            validReader,
            STRING_SCHEMA,
            SchemaCompatibility.READER_WRITER_COMPATIBLE_MESSAGE);
    final SchemaCompatibility.SchemaPairCompatibility invalidResult =
        new SchemaCompatibility.SchemaPairCompatibility(
            SchemaCompatibility.SchemaCompatibilityResult.incompatible(
            SchemaIncompatibilityType.TYPE_MISMATCH,
            INT_SCHEMA,
            STRING_SCHEMA,
            "reader type: INT not compatible with writer type: STRING",
            asList("")),
            INT_SCHEMA,
            STRING_SCHEMA,
            String.format(
                "Data encoded using writer schema:%n%s%n"
                + "will or may fail to decode using reader schema:%n%s%n",
                STRING_SCHEMA.toString(true),
                INT_SCHEMA.toString(true)));

    assertEquals(
        validResult,
        checkReaderWriterCompatibility(validReader, STRING_SCHEMA));
    assertEquals(
        invalidResult,
        checkReaderWriterCompatibility(INT_SCHEMA, STRING_SCHEMA));
  }

  /** Reader union schema must contain all writer union branches. */
  @Test
  public void testUnionReaderWriterSubsetIncompatibility() {
    final Schema unionWriter = Schema.createUnion(list(INT_SCHEMA, STRING_SCHEMA, LONG_SCHEMA));
    final Schema unionReader = Schema.createUnion(list(INT_SCHEMA, STRING_SCHEMA));
    final SchemaPairCompatibility result =
        checkReaderWriterCompatibility(unionReader, unionWriter);
    assertEquals(SchemaCompatibilityType.INCOMPATIBLE, result.getType());
  }

  // -----------------------------------------------------------------------------------------------

  /** Collection of reader/writer schema pair that are compatible. */
  public static final List<ReaderWriter> COMPATIBLE_READER_WRITER_TEST_CASES = list(
      new ReaderWriter(BOOLEAN_SCHEMA, BOOLEAN_SCHEMA),

      new ReaderWriter(INT_SCHEMA, INT_SCHEMA),

      new ReaderWriter(LONG_SCHEMA, INT_SCHEMA),
      new ReaderWriter(LONG_SCHEMA, LONG_SCHEMA),

      // Avro spec says INT/LONG can be promoted to FLOAT/DOUBLE.
      // This is arguable as this causes a loss of precision.
      new ReaderWriter(FLOAT_SCHEMA, INT_SCHEMA),
      new ReaderWriter(FLOAT_SCHEMA, LONG_SCHEMA),
      new ReaderWriter(DOUBLE_SCHEMA, LONG_SCHEMA),

      new ReaderWriter(DOUBLE_SCHEMA, INT_SCHEMA),
      new ReaderWriter(DOUBLE_SCHEMA, FLOAT_SCHEMA),

      new ReaderWriter(STRING_SCHEMA, STRING_SCHEMA),

      new ReaderWriter(BYTES_SCHEMA, BYTES_SCHEMA),

      new ReaderWriter(INT_ARRAY_SCHEMA, INT_ARRAY_SCHEMA),
      new ReaderWriter(LONG_ARRAY_SCHEMA, INT_ARRAY_SCHEMA),
      new ReaderWriter(INT_MAP_SCHEMA, INT_MAP_SCHEMA),
      new ReaderWriter(LONG_MAP_SCHEMA, INT_MAP_SCHEMA),

      new ReaderWriter(ENUM1_AB_SCHEMA, ENUM1_AB_SCHEMA),
      new ReaderWriter(ENUM1_ABC_SCHEMA, ENUM1_AB_SCHEMA),

      // String-to/from-bytes, introduced in Avro 1.7.7
      new ReaderWriter(STRING_SCHEMA, BYTES_SCHEMA),
      new ReaderWriter(BYTES_SCHEMA, STRING_SCHEMA),

      // Tests involving unions:
      new ReaderWriter(EMPTY_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
      new ReaderWriter(FLOAT_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
      new ReaderWriter(FLOAT_UNION_SCHEMA, INT_UNION_SCHEMA),
      new ReaderWriter(FLOAT_UNION_SCHEMA, LONG_UNION_SCHEMA),
      new ReaderWriter(FLOAT_UNION_SCHEMA, INT_LONG_UNION_SCHEMA),
      new ReaderWriter(INT_UNION_SCHEMA, INT_UNION_SCHEMA),
      new ReaderWriter(INT_STRING_UNION_SCHEMA, STRING_INT_UNION_SCHEMA),
      new ReaderWriter(INT_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
      new ReaderWriter(LONG_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
      new ReaderWriter(LONG_UNION_SCHEMA, INT_UNION_SCHEMA),
      new ReaderWriter(FLOAT_UNION_SCHEMA, INT_UNION_SCHEMA),
      new ReaderWriter(DOUBLE_UNION_SCHEMA, INT_UNION_SCHEMA),
      new ReaderWriter(FLOAT_UNION_SCHEMA, LONG_UNION_SCHEMA),
      new ReaderWriter(DOUBLE_UNION_SCHEMA, LONG_UNION_SCHEMA),
      new ReaderWriter(FLOAT_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
      new ReaderWriter(DOUBLE_UNION_SCHEMA, FLOAT_UNION_SCHEMA),
      new ReaderWriter(STRING_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
      new ReaderWriter(STRING_UNION_SCHEMA, BYTES_UNION_SCHEMA),
      new ReaderWriter(BYTES_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
      new ReaderWriter(BYTES_UNION_SCHEMA, STRING_UNION_SCHEMA),
      new ReaderWriter(DOUBLE_UNION_SCHEMA, INT_FLOAT_UNION_SCHEMA),

      // Readers capable of reading all branches of a union are compatible
      new ReaderWriter(FLOAT_SCHEMA, INT_FLOAT_UNION_SCHEMA),
      new ReaderWriter(LONG_SCHEMA, INT_LONG_UNION_SCHEMA),
      new ReaderWriter(DOUBLE_SCHEMA, INT_FLOAT_UNION_SCHEMA),
      new ReaderWriter(DOUBLE_SCHEMA, INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA),

      // Special case of singleton unions:
      new ReaderWriter(FLOAT_SCHEMA, FLOAT_UNION_SCHEMA),
      new ReaderWriter(INT_UNION_SCHEMA, INT_SCHEMA),
      new ReaderWriter(INT_SCHEMA, INT_UNION_SCHEMA),
      // Fixed types
      new ReaderWriter(FIXED_4_BYTES, FIXED_4_BYTES),

      // Tests involving records:
      new ReaderWriter(EMPTY_RECORD1, EMPTY_RECORD1),
      new ReaderWriter(EMPTY_RECORD1, A_INT_RECORD1),

      new ReaderWriter(A_INT_RECORD1, A_INT_RECORD1),
      new ReaderWriter(A_DINT_RECORD1, A_INT_RECORD1),
      new ReaderWriter(A_DINT_RECORD1, A_DINT_RECORD1),
      new ReaderWriter(A_INT_RECORD1, A_DINT_RECORD1),

      new ReaderWriter(A_LONG_RECORD1, A_INT_RECORD1),

      new ReaderWriter(A_INT_RECORD1, A_INT_B_INT_RECORD1),
      new ReaderWriter(A_DINT_RECORD1, A_INT_B_INT_RECORD1),

      new ReaderWriter(A_INT_B_DINT_RECORD1, A_INT_RECORD1),
      new ReaderWriter(A_DINT_B_DINT_RECORD1, EMPTY_RECORD1),
      new ReaderWriter(A_DINT_B_DINT_RECORD1, A_INT_RECORD1),
      new ReaderWriter(A_INT_B_INT_RECORD1, A_DINT_B_DINT_RECORD1),

      new ReaderWriter(INT_LIST_RECORD, INT_LIST_RECORD),
      new ReaderWriter(LONG_LIST_RECORD, LONG_LIST_RECORD),
      new ReaderWriter(LONG_LIST_RECORD, INT_LIST_RECORD),

      new ReaderWriter(NULL_SCHEMA, NULL_SCHEMA)
  );

  // -----------------------------------------------------------------------------------------------

  /**
   * The reader/writer pairs that are incompatible are now moved to specific test classes, one class
   * per error case (for easier pinpointing of errors). The method to validate incompatibility is
   * still here.
   */
  public static void validateIncompatibleSchemas(
      Schema reader,
      Schema writer,
      SchemaIncompatibilityType incompatibility,
      String message,
      String location
    ) {
    validateIncompatibleSchemas(
        reader,
        writer,
        asList(incompatibility),
        asList(message),
        asList(location)
    );
  }

  // -----------------------------------------------------------------------------------------------

  public static void validateIncompatibleSchemas(Schema reader, Schema writer,
      List<SchemaIncompatibilityType> incompatibilityTypes, List<String> messages, List<String> locations) {
    SchemaPairCompatibility compatibility = checkReaderWriterCompatibility(reader, writer);
    SchemaCompatibilityResult compatibilityResult = compatibility.getResult();
    assertEquals(reader, compatibility.getReader());
    assertEquals(writer, compatibility.getWriter());
    assertEquals(SchemaCompatibilityType.INCOMPATIBLE, compatibilityResult.getCompatibility());

    assertEquals(incompatibilityTypes.size(), compatibilityResult.getIncompatibilities().size());
    for (int i = 0 ; i < incompatibilityTypes.size(); i++) {
      Incompatibility incompatibility = compatibilityResult.getIncompatibilities().get(i);
      assertSchemaContains(incompatibility.getReaderFragment(), reader);
      assertSchemaContains(incompatibility.getWriterFragment(), writer);
      assertEquals(incompatibilityTypes.get(i), incompatibility.getType());
      assertEquals(messages.get(i), incompatibility.getMessage());
      assertEquals(locations.get(i), incompatibility.getLocation());
    }

    String description = String.format(
        "Data encoded using writer schema:%n%s%n"
            + "will or may fail to decode using reader schema:%n%s%n",
        writer.toString(true), reader.toString(true));
    assertEquals(description, compatibility.getDescription());
  }

  // -----------------------------------------------------------------------------------------------

  /** Tests reader/writer compatibility validation. */
  @Test
  public void testReaderWriterCompatibility() {
    for (ReaderWriter readerWriter : COMPATIBLE_READER_WRITER_TEST_CASES) {
      final Schema reader = readerWriter.getReader();
      final Schema writer = readerWriter.getWriter();
      LOG.debug("Testing compatibility of reader {} with writer {}.", reader, writer);
      final SchemaPairCompatibility result =
          checkReaderWriterCompatibility(reader, writer);
      assertEquals(String.format(
          "Expecting reader %s to be compatible with writer %s, but tested incompatible.",
          reader, writer),
          SchemaCompatibilityType.COMPATIBLE, result.getType());
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Descriptor for a test case that encodes a datum according to a given writer schema,
   * then decodes it according to reader schema and validates the decoded value.
   */
  private static final class DecodingTestCase {
    /** Writer schema used to encode the datum. */
    private final Schema mWriterSchema;

    /** Datum to encode according to the specified writer schema. */
    private final Object mDatum;

    /** Reader schema used to decode the datum encoded using the writer schema. */
    private final Schema mReaderSchema;

    /** Expected datum value when using the reader schema to decode from the writer schema. */
    private final Object mDecodedDatum;

    public DecodingTestCase(
        final Schema writerSchema,
        final Object datum,
        final Schema readerSchema,
        final Object decoded) {
      mWriterSchema = writerSchema;
      mDatum = datum;
      mReaderSchema = readerSchema;
      mDecodedDatum = decoded;
    }

    public Schema getReaderSchema() {
      return mReaderSchema;
    }

    public Schema getWriterSchema() {
      return mWriterSchema;
    }

    public Object getDatum() {
      return mDatum;
    }

    public Object getDecodedDatum() {
      return mDecodedDatum;
    }
  }

  // -----------------------------------------------------------------------------------------------

  public static final List<DecodingTestCase> DECODING_COMPATIBILITY_TEST_CASES = list(
      new DecodingTestCase(INT_SCHEMA, 1, INT_SCHEMA, 1),
      new DecodingTestCase(INT_SCHEMA, 1, LONG_SCHEMA, 1L),
      new DecodingTestCase(INT_SCHEMA, 1, FLOAT_SCHEMA, 1.0f),
      new DecodingTestCase(INT_SCHEMA, 1, DOUBLE_SCHEMA, 1.0d),

      // This is currently accepted but causes a precision loss:
      // IEEE 754 floats have 24 bits signed mantissa
      new DecodingTestCase(INT_SCHEMA, (1 << 24) + 1, FLOAT_SCHEMA, (float) ((1 << 24) + 1)),

      // new DecodingTestCase(LONG_SCHEMA, 1L, INT_SCHEMA, 1),  // should work in best-effort!

      new DecodingTestCase(
          ENUM1_AB_SCHEMA, new EnumSymbol(ENUM1_AB_SCHEMA, "A"),
          ENUM1_ABC_SCHEMA, new EnumSymbol(ENUM1_ABC_SCHEMA, "A")),

      new DecodingTestCase(
          ENUM1_ABC_SCHEMA, new EnumSymbol(ENUM1_ABC_SCHEMA, "A"),
          ENUM1_AB_SCHEMA, new EnumSymbol(ENUM1_AB_SCHEMA, "A")),

      new DecodingTestCase(
          ENUM1_ABC_SCHEMA, new EnumSymbol(ENUM1_ABC_SCHEMA, "B"),
          ENUM1_BC_SCHEMA, new EnumSymbol(ENUM1_BC_SCHEMA, "B")),

      new DecodingTestCase(
          INT_STRING_UNION_SCHEMA, "the string",
          STRING_SCHEMA, new Utf8("the string")),

      new DecodingTestCase(
          INT_STRING_UNION_SCHEMA, "the string",
          STRING_UNION_SCHEMA, new Utf8("the string"))
);

  /** Tests the reader/writer compatibility at decoding time. */
  @Test
  public void testReaderWriterDecodingCompatibility() throws Exception {
    for (DecodingTestCase testCase : DECODING_COMPATIBILITY_TEST_CASES) {
      final Schema readerSchema = testCase.getReaderSchema();
      final Schema writerSchema = testCase.getWriterSchema();
      final Object datum = testCase.getDatum();
      final Object expectedDecodedDatum = testCase.getDecodedDatum();

      LOG.debug(
          "Testing incompatibility of reader {} with writer {}.",
          readerSchema, writerSchema);

      LOG.debug("Encode datum {} with writer {}.", datum, writerSchema);
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      final DatumWriter<Object> datumWriter = new GenericDatumWriter<>(writerSchema);
      datumWriter.write(datum, encoder);
      encoder.flush();

      LOG.debug(
          "Decode datum {} whose writer is {} with reader {}.",
          new Object[]{datum, writerSchema, readerSchema});
      final byte[] bytes = baos.toByteArray();
      final Decoder decoder = DecoderFactory.get().resolvingDecoder(
          writerSchema, readerSchema,
          DecoderFactory.get().binaryDecoder(bytes, null));
      final DatumReader<Object> datumReader = new GenericDatumReader<>(readerSchema);
      final Object decodedDatum = datumReader.read(null, decoder);

      assertEquals(String.format(
          "Expecting decoded value %s when decoding value %s whose writer schema is %s "
          + "using reader schema %s, but value was %s.",
          expectedDecodedDatum, datum, writerSchema, readerSchema, decodedDatum),
          expectedDecodedDatum, decodedDatum);
    }
  }

  Deque<String> asDeqeue(String... args) {
    Deque<String> dq = new ArrayDeque<>();
    List<String> x = Arrays.asList(args);
    Collections.reverse(x);
    dq.addAll(x);
    return dq;
  }
}
