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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream.Header;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.util.RandomData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestDataFileAppend {

  @Rule
  public TemporaryFolder DIR = new TemporaryFolder();

  private static final String SCHEMA_JSON = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": ["
      + "{\"name\":\"stringField\", \"type\":\"string\"}," + "{\"name\":\"longField\", \"type\":\"long\"}]}";
  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);
  private static final long SEED = System.currentTimeMillis();
  private static final int COUNT = Integer.parseInt(System.getProperty("test.count", "200"));
  private static final boolean VALIDATE = !"false".equals(System.getProperty("test.validate", "true"));

  /*
   * 
   */
  @Test
  public void testAppendTo() throws Exception {
    File file = new File(DIR.getRoot().getPath(), "testAppend.avro");
    Iterator<Object> datumGenerator = new RandomData(SCHEMA, COUNT, SEED).iterator();

    // initialize file with COUNT rows
    try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>()).setSyncInterval(100)) {
      writer.setCodec(CodecFactory.nullCodec());
      writer.create(SCHEMA, file);
      for (int i = 0; i < COUNT; i++) {
        Object datum = datumGenerator.next();
        writer.append(datum);
        if (i % (COUNT / 3) == 0)
          writer.sync(); // force some syncs mid-file
      }
    }

    checkGenericRead(file, COUNT);

    // read Header from file
    Header header;
    {
      DataFileReader<Object> reader = new DataFileReader<>(file, new GenericDatumReader<>());
      header = reader.getHeader();
      reader.close();
    }

    int repeatAppend = 2;
    for (int repeat = 0; repeat < repeatAppend; repeat++) {
      // append to in-memory buffer next COUNT rows
      ByteArrayOutputStream bufferOut = new ByteArrayOutputStream();
      try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>()).setSyncInterval(100)) {
        writer.setCodec(CodecFactory.nullCodec());

        writer.appendTo(header, bufferOut);

        for (int i = 0; i < COUNT; i++) {
          Object datum = datumGenerator.next();
          writer.append(datum);
          if (i % (COUNT / 3) == 0)
            writer.sync(); // force some syncs mid-file
        }
      }
      bufferOut.close();
      byte[] serializedBytes = bufferOut.toByteArray();

      // append buffer to file (emulate a remote file append)
      try (OutputStream appendTo = new FileOutputStream(file, true)) {
        appendTo.write(serializedBytes);
      }

    }

    // check file with appended data
    checkGenericRead(file, COUNT + repeatAppend * COUNT);
  }

  private void checkGenericRead(File file, int expectedCount) throws IOException {
    Iterator<Object> datumGenerator = new RandomData(SCHEMA, COUNT, SEED).iterator();
    try (DataFileReader<Object> reader = new DataFileReader<>(file, new GenericDatumReader<>())) {
      Object datum = null;
      for (int i = 0; i < expectedCount; i++) {
        datum = reader.next(datum);
        if (VALIDATE) {
          Object expected = datumGenerator.next();
          assertEquals(expected, datum);
        }
      }
      if (reader.hasNext()) {
        Assert.fail("should have no more rows");
      }
    }
  }

}
