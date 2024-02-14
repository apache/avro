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
package org.apache.avro.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Objects;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;

import static org.junit.Assert.assertEquals;

public class TestRandomData {
  private long seed;

  private int count;

  private File file;
  private GenericData genericData;
  private SpecificData specificData;
  private Schema specificSchema;
  private ReflectData reflectData;
  private Schema reflectedSchema;

  @Before
  public void setUp() throws Exception {
    file = Files.createTempFile("randomData", ".avro").toFile();
    seed = System.currentTimeMillis();
    count = new Random().nextInt(50) + 75;

    genericData = GenericData.get();
    specificData = SpecificData.get();
    specificSchema = specificData.getSchema(SpecificTestRecord.class);
    reflectData = ReflectData.get();
    reflectedSchema = reflectData.getSchema(ReflectTestRecord.class);
  }

  @Test
  public void testRandomDataFromGenericToGeneric() throws IOException {
    checkWrite(genericData, TEST_SCHEMA);
    checkRead(genericData, TEST_SCHEMA);
  }

  @Test
  public void testRandomDataFromGenericToSpecific() throws IOException {
    checkWrite(genericData, TEST_SCHEMA);
    checkRead(specificData, specificSchema);
  }

  @Test
  public void testRandomDataFromGenericToReflected() throws IOException {
    checkWrite(genericData, TEST_SCHEMA);
    checkRead(reflectData, reflectedSchema);
  }

  @Test
  public void testRandomDataFromSpecificToGeneric() throws IOException {
    checkWrite(specificData, specificSchema);
    checkRead(genericData, TEST_SCHEMA);
  }

  @Test
  public void testRandomDataFromSpecificToSpecific() throws IOException {
    checkWrite(specificData, specificSchema);
    checkRead(specificData, specificSchema);
  }

  @Test
  public void testRandomDataFromSpecificToReflected() throws IOException {
    checkWrite(specificData, specificSchema);
    checkRead(reflectData, reflectedSchema);
  }

  @Test
  public void testRandomDataFromReflectedToGeneric() throws IOException {
    checkWrite(reflectData, reflectedSchema);
    checkRead(genericData, TEST_SCHEMA);
  }

  @Test
  public void testRandomDataFromReflectedToSpecific() throws IOException {
    checkWrite(reflectData, reflectedSchema);
    checkRead(specificData, specificSchema);
  }

  @Test
  public void testRandomDataFromReflectedToReflected() throws IOException {
    checkWrite(reflectData, reflectedSchema);
    checkRead(reflectData, reflectedSchema);
  }

  private void checkWrite(GenericData genericData, Schema schema) throws IOException {
    // noinspection unchecked
    try (DataFileWriter<Object> writer = new DataFileWriter<Object>(genericData.createDatumWriter(schema))) {
      writer.create(schema, file);
      for (Object datum : new RandomData(genericData, schema, this.count, seed)) {
        writer.append(datum);
      }
    }
  }

  private void checkRead(GenericData genericData, Schema schema) throws IOException {
    // noinspection unchecked
    try (DataFileReader<Object> reader = new DataFileReader<Object>(file, genericData.createDatumReader(schema))) {
      for (Object expected : new RandomData(genericData, schema, this.count, seed)) {
        assertEquals(expected, reader.next());
      }
    }
  }

  /*
   * Test classes: they implement the same schema, but one is a SpecificRecord and
   * the other uses a reflected schema.
   */

  public static final String TEST_SCHEMA_JSON = "{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"},{\"name\":\"y\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]}";

  public static final Schema TEST_SCHEMA = new Schema.Parser().parse(TEST_SCHEMA_JSON);

  public static class SpecificTestRecord extends SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {

    public static org.apache.avro.Schema getClassSchema() {
      return CODER.SCHEMA$;
    }

    public static final class InternalCoders {

      private final org.apache.avro.Schema SCHEMA$;// = new
                                                   // org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"NestedLogicalTypesArray\",\"namespace\":\"org.apache.avro.codegentest.testdata\",\"doc\":\"Test
                                                   // nested types with logical types in generated Java
                                                   // classes\",\"fields\":[{\"name\":\"arrayOfRecords\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"RecordInArray\",\"fields\":[{\"name\":\"nullableDateField\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}]}}}]}");

      private final SpecificData MODEL$ = new SpecificData();

      public InternalCoders() {
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        org.apache.avro.Schema currentSchema = parser.parse(TEST_SCHEMA_JSON.replace("\"name\":\"Record\"",
            "\"name\":\"" + SpecificTestRecord.class.getCanonicalName() + "\""));

        this.SCHEMA$ = currentSchema;

        this.MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.DateConversion());
        this.ENCODER = new BinaryMessageEncoder<>(this.MODEL$, this.SCHEMA$);
        this.DECODER = new BinaryMessageDecoder<>(this.MODEL$, this.SCHEMA$);
      }

      private final BinaryMessageEncoder<SpecificTestRecord> ENCODER;

      private final BinaryMessageDecoder<SpecificTestRecord> DECODER;

      /**
       * Return the BinaryMessageEncoder instance used by this class.
       * 
       * @return the message encoder used by this class
       */
      public BinaryMessageEncoder<SpecificTestRecord> getEncoder() {
        return ENCODER;
      }

      /**
       * Return the BinaryMessageDecoder instance used by this class.
       * 
       * @return the message decoder used by this class
       */
      public BinaryMessageDecoder<SpecificTestRecord> getDecoder() {
        return DECODER;
      }

      /**
       * Create a new BinaryMessageDecoder instance for this class that uses the
       * specified {@link SchemaStore}.
       * 
       * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
       * @return a BinaryMessageDecoder instance for this class backed by the given
       *         SchemaStore
       */
      public BinaryMessageDecoder<SpecificTestRecord> createDecoder(SchemaStore resolver) {
        return new BinaryMessageDecoder<>(this.MODEL$, this.SCHEMA$, resolver);
      }

      /**
       * Deserializes a NestedLogicalTypesArray from a ByteBuffer.
       * 
       * @param b a byte buffer holding serialized data for an instance of this class
       * @return a NestedLogicalTypesArray instance decoded from the given buffer
       * @throws java.io.IOException if the given bytes could not be deserialized into
       *                             an instance of this class
       */
      public SpecificTestRecord fromByteBuffer(java.nio.ByteBuffer b) throws java.io.IOException {
        return DECODER.decode(b);
      }
    }

    public static final InternalCoders CODER = new InternalCoders();

    /**
     * Serializes this NestedLogicalTypesArray to a ByteBuffer.
     * 
     * @return a buffer holding the serialized data for this instance
     * @throws java.io.IOException if this instance could not be serialized
     */
    public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
      return CODER.getEncoder().encode(this);
    }

    private int x;
    private String y;

    @Override
    public org.apache.avro.specific.SpecificData getSpecificData() {
      return CODER.MODEL$;
    }

    @Override
    public Schema getSchema() {
      return CODER.SCHEMA$;
    }

    @Override
    public void put(int i, Object v) {
      switch (i) {
      case 0:
        x = (Integer) v;
        break;
      case 1:
        y = (String) v;
        break;
      default:
        throw new RuntimeException();
      }
    }

    @Override
    public Object get(int i) {
      switch (i) {
      case 0:
        return x;
      case 1:
        return y;
      }
      throw new RuntimeException();
    }
  }

  public static class ReflectTestRecord {
    private int x;
    private String y;

    public int getX() {
      return x;
    }

    public void setX(int x) {
      this.x = x;
    }

    public String getY() {
      return y;
    }

    public void setY(String y) {
      this.y = y;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReflectTestRecord that = (ReflectTestRecord) o;
      return x == that.x && Objects.equals(y, that.y);
    }

    @Override
    public int hashCode() {
      return Objects.hash(x, y);
    }

    @Override
    public String toString() {
      return String.format("{\"x\": %d, \"y\": \"%s\"}", x, y);
    }
  }
}
