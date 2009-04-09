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

import java.io.*;
import java.util.*;
import junit.framework.TestCase;
import org.codehaus.jackson.map.JsonNode;

import org.apache.avro.io.*;
import org.apache.avro.file.*;
import org.apache.avro.generic.*;
import org.apache.avro.specific.*;
import org.apache.avro.reflect.*;
import org.apache.avro.util.*;

public class TestDataFile extends TestCase {
  private static final int COUNT =
    Integer.parseInt(System.getProperty("test.count", "10"));
  private static final boolean VALIDATE = 
    !"false".equals(System.getProperty("test.validate", "true"));
  private static final File DIR
    = new File(System.getProperty("test.dir", "/tmp"));
  private static final File DATAFILE_DIR
    = new File(System.getProperty("test.dir", "/tmp")+"/data-files/");
  private static final File FILE = new File(DIR, "test.avro");
  private static final long SEED = System.currentTimeMillis();

  private static final String SCHEMA_JSON =
    "{\"type\": \"record\", \"fields\":{"
    +"\"stringField\":\"string\","
    +"\"longField\": \"long\"}}";
  private static final Schema SCHEMA = Schema.parse(SCHEMA_JSON);

  public void testGenericWrite() throws IOException {
    DataFileWriter<Object> writer =
      new DataFileWriter<Object>(SCHEMA,
                                 new FileOutputStream(FILE),
                                 new GenericDatumWriter());
    try {
      for (Object datum : new RandomData(SCHEMA, COUNT, SEED)) {
        writer.append(datum);
      }
    } finally {
      writer.close();
    }
  }

  public void testGenericRead() throws IOException {
    DataFileReader<Object> reader =
      new DataFileReader<Object>(new SeekableFileInput(FILE),
                                 new GenericDatumReader());
    try {
      Object datum = null;
      if (VALIDATE) {
        for (Object expected : new RandomData(SCHEMA, COUNT, SEED)) {
          datum = reader.next(datum);
          assertEquals(expected, datum);
        }
      } else {
        for (int i = 0; i < COUNT; i++) {
          datum = reader.next(datum);
        }
      }
    } finally {
      reader.close();
    }
  }

  public void testGeneratedGeneric() throws IOException {
    readFiles(new GenericDatumReader());
  }

  public void testGeneratedSpecific() throws IOException {
    readFiles(new SpecificDatumReader("org.apache.avro."));
  }

  public void testGeneratedReflect() throws IOException {
    readFiles(new ReflectDatumReader("org.apache.avro."));
  }

  private void readFiles(DatumReader<Object> datumReader) throws IOException {
    for (File f : DATAFILE_DIR.listFiles())
      readFile(f, datumReader, true);
  }

  private void readFile(File f, DatumReader<Object> datumReader, boolean reuse)
    throws IOException {
    System.out.println("Reading "+ f.getName());
    DataFileReader<Object> reader =
      new DataFileReader<Object>(new SeekableFileInput(f), datumReader);
    Object datum = null;
    long count = reader.getMetaLong("count");
    for (int i = 0; i < count; i++) {
      datum = reader.next(reuse ? datum : null);
      assertNotNull(datum);
    }
  }

  public static void main(String[] args) throws Exception {
    File input = new File(args[0]);
    Schema projection = null;
    if (args.length > 1)
      projection = Schema.parse(new File(args[1]));
    TestDataFile tester = new TestDataFile();
    tester.readFile(input, new GenericDatumReader(null, projection), false);
    long start = System.currentTimeMillis();
    for (int i = 0; i < 4; i++)
      tester.readFile(input, new GenericDatumReader(null, projection), false);
    System.out.println("Time: "+(System.currentTimeMillis()-start));
  }

}
