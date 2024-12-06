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
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Before;
import org.junit.Test;

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

  public static class SpecificTestRecord extends SpecificRecordBase {
    public static final Schema SCHEMA$ = new Schema.Parser().parse(TEST_SCHEMA_JSON.replace("\"name\":\"Record\"",
        "\"name\":\"" + SpecificTestRecord.class.getCanonicalName() + "\""));
    private int x;
    private String y;

    @Override
    public Schema getSchema() {
      return SCHEMA$;
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
