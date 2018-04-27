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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.Syncable;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class TestDataFile {
  private static final Logger LOG =
    LoggerFactory.getLogger(TestDataFile.class);

  CodecFactory codec = null;
  public TestDataFile(CodecFactory codec) {
    this.codec = codec;
    LOG.info("Running with codec: " + codec);
  }

  @Parameters
  public static List<Object[]> codecs() {
    List<Object[]> r = new ArrayList<>();
    r.add(new Object[] { null });
    r.add(new Object[] { CodecFactory.deflateCodec(0) });
    r.add(new Object[] { CodecFactory.deflateCodec(1) });
    r.add(new Object[] { CodecFactory.deflateCodec(9) });
    r.add(new Object[] { CodecFactory.nullCodec() });
    r.add(new Object[] { CodecFactory.snappyCodec() });
    r.add(new Object[] { CodecFactory.xzCodec(0) });
    r.add(new Object[] { CodecFactory.xzCodec(1) });
    r.add(new Object[] { CodecFactory.xzCodec(6) });
    return r;
  }

  private static final int COUNT =
    Integer.parseInt(System.getProperty("test.count", "200"));
  private static final boolean VALIDATE =
    !"false".equals(System.getProperty("test.validate", "true"));
  private static final File DIR
    = new File(System.getProperty("test.dir", "/tmp"));
  private static final long SEED = System.currentTimeMillis();
  private static final String SCHEMA_JSON =
    "{\"type\": \"record\", \"name\": \"Test\", \"fields\": ["
    +"{\"name\":\"stringField\", \"type\":\"string\"},"
    +"{\"name\":\"longField\", \"type\":\"long\"}]}";
  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

  private File makeFile() {
    return new File(DIR, "test-" + codec + ".avro");
  }

  @Test public void runTestsInOrder() throws Exception {
    testGenericWrite();
    testGenericRead();
    testSplits();
    testSyncDiscovery();
    testGenericAppend();
    testReadWithHeader();
    testFSync(false);
    testFSync(true);
  }

  public void testGenericWrite() throws IOException {
    DataFileWriter<Object> writer =
      new DataFileWriter<>(new GenericDatumWriter<>())
      .setSyncInterval(100);
    if (codec != null) {
      writer.setCodec(codec);
    }
    writer.create(SCHEMA, makeFile());
    try {
      int count = 0;
      for (Object datum : new RandomData(SCHEMA, COUNT, SEED)) {
        writer.append(datum);
        if (++count%(COUNT/3) == 0)
          writer.sync();                          // force some syncs mid-file
        if (count == 5) {
          // force a write of an invalid record
          boolean threwProperly = false;
          try {
            GenericData.Record record = (GenericData.Record) datum;
            record.put(1, null);
            threwProperly = true;
            writer.append(record);
            threwProperly = false;
          } catch (DataFileWriter.AppendWriteException e) {
            System.out.println("Ignoring: "+e);
          }
          Assert.assertTrue("failed to throw when expected", threwProperly);
        }
      }
    } finally {
      writer.close();
    }

    // Ensure that a second call to close doesn't raise an exception. (AVRO-1249)
    Exception doubleCloseEx = null;

    try {
      writer.close();
    } catch (Exception e) {
      doubleCloseEx = e;
    }

    Assert.assertNull("Double close() threw an unexpected exception", doubleCloseEx);
  }

  public void testGenericRead() throws IOException {
    DataFileReader<Object> reader =
      new DataFileReader<>(makeFile(), new GenericDatumReader<>());
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

  public void testSplits() throws IOException {
    File file = makeFile();
    DataFileReader<Object> reader =
      new DataFileReader<>(file, new GenericDatumReader<>());
    Random rand = new Random(SEED);
    try {
      int splits = 10;                            // number of splits
      int length = (int)file.length();            // length of file
      int end = length;                           // end of split
      int remaining = end;                        // bytes remaining
      int count = 0;                              // count of entries
      while (remaining > 0) {
        int start = Math.max(0, end - rand.nextInt(2*length/splits));
        reader.sync(start);                       // count entries in split
        while (!reader.pastSync(end)) {
          reader.next();
          count++;
        }
        remaining -= end-start;
        end = start;
      }
      assertEquals(COUNT, count);
    } finally {
      reader.close();
    }
  }

  public void testSyncDiscovery() throws IOException {
    File file = makeFile();
    DataFileReader<Object> reader =
      new DataFileReader<>(file, new GenericDatumReader<>());
    try {
      // discover the sync points
      ArrayList<Long> syncs = new ArrayList<>();
      long previousSync = -1;
      while (reader.hasNext()) {
        if (reader.previousSync() != previousSync) {
          previousSync = reader.previousSync();
          syncs.add(previousSync);
        }
        reader.next();
      }
      // confirm that the first point is the one reached by sync(0)
      reader.sync(0);
      assertEquals((long)reader.previousSync(), (long)syncs.get(0));
      // and confirm that all points are reachable
      for (Long sync : syncs) {
        reader.seek(sync);
        assertNotNull(reader.next());
      }
    } finally {
      reader.close();
    }
  }

  public void testGenericAppend() throws IOException {
    File file = makeFile();
    long start = file.length();
    DataFileWriter<Object> writer =
      new DataFileWriter<>(new GenericDatumWriter<>())
      .appendTo(file);
    try {
      for (Object datum : new RandomData(SCHEMA, COUNT, SEED+1)) {
        writer.append(datum);
      }
    } finally {
      writer.close();
    }
    DataFileReader<Object> reader =
      new DataFileReader<>(file, new GenericDatumReader<>());
    try {
      reader.seek(start);
      Object datum = null;
      if (VALIDATE) {
        for (Object expected : new RandomData(SCHEMA, COUNT, SEED+1)) {
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

  public void testReadWithHeader() throws IOException {
    File file = makeFile();
    DataFileReader<Object> reader =
      new DataFileReader<>(file, new GenericDatumReader<>());
    // get a header for this file
    DataFileStream.Header header = reader.getHeader();
    // re-open to an arbitrary position near the middle, with sync == true
    SeekableFileInput sin = new SeekableFileInput(file);
    sin.seek(sin.length() / 2);
    reader = DataFileReader.openReader(sin, new GenericDatumReader<>(),
        header, true);
    assertNotNull("Should be able to reopen from arbitrary point", reader.next());
    long validPos = reader.previousSync();
    // post sync, we know of a valid sync point: re-open with seek (sync == false)
    sin.seek(validPos);
    reader = DataFileReader.openReader(sin, new GenericDatumReader<>(),
        header, false);
    assertEquals("Should not move from sync point on reopen", validPos, sin.tell());
    assertNotNull("Should be able to reopen at sync point", reader.next());
  }

  @Test public void testSyncInHeader() throws IOException {
    DataFileReader<Object> reader = new DataFileReader<>
      (new File("../../../share/test/data/syncInMeta.avro"),
       new GenericDatumReader<>());
    reader.sync(0);
    for (Object datum : reader)
      assertNotNull(datum);
  }

  @Test public void test12() throws IOException {
    readFile(new File("../../../share/test/data/test.avro12"),
             new GenericDatumReader<>());
  }

  @Test
  public void testFlushCount() throws IOException {
    DataFileWriter<Object> writer =
      new DataFileWriter<>(new GenericDatumWriter<>());
    writer.setFlushOnEveryBlock(false);
    TestingByteArrayOutputStream out = new TestingByteArrayOutputStream();
    writer.create(SCHEMA, out);
    int currentCount = 0;
    int flushCounter = 0;
    try {
      for (Object datum : new RandomData(SCHEMA, COUNT, SEED+1)) {
        currentCount++;
        writer.append(datum);
        writer.sync();
        if (currentCount % 10 == 0) {
          flushCounter++;
          writer.flush();
        }
      }
    } finally {
      writer.close();
    }
    System.out.println("Total number of flushes: " + out.flushCount);
    // Unfortunately, the underlying buffered output stream might flush data
    // to disk when the buffer becomes full, so the only check we can
    // accurately do is that each sync did not lead to a flush and that the
    // file was flushed at least as many times as we called flush. Generally
    // noticed that out.flushCount is almost always 24 though.
    Assert.assertTrue(out.flushCount < currentCount &&
      out.flushCount >= flushCounter);
  }

  private void testFSync(boolean useFile) throws IOException {
    DataFileWriter<Object> writer =
      new DataFileWriter<>(new GenericDatumWriter<>());
    try {
      writer.setFlushOnEveryBlock(false);
      TestingByteArrayOutputStream out = new TestingByteArrayOutputStream();
      if (useFile) {
        File f = makeFile();
        SeekableFileInput in = new SeekableFileInput(f);
        try {
          writer.appendTo(in, out);
        } finally {
          in.close();
        }
      } else {
        writer.create(SCHEMA, out);
      }
      int currentCount = 0;
      int syncCounter = 0;
      for (Object datum : new RandomData(SCHEMA, COUNT, SEED + 1)) {
        currentCount++;
        writer.append(datum);
        if (currentCount % 10 == 0) {
          writer.fSync();
          syncCounter++;
        }
      }
      System.out.println("Total number of syncs: " + out.syncCount);
      Assert.assertEquals(syncCounter, out.syncCount);
    } finally {
      writer.close();
    }
  }


  static void readFile(File f, DatumReader<? extends Object> datumReader)
    throws IOException {
    FileReader<? extends Object> reader = DataFileReader.openReader(f, datumReader);
    for (Object datum : reader) {
      assertNotNull(datum);
    }
  }

  public static void main(String[] args) throws Exception {
    File input = new File(args[0]);
    Schema projection = null;
    if (args.length > 1)
      projection = Schema.parse(new File(args[1]));
    TestDataFile.readFile(input, new GenericDatumReader<>(null, projection));
    long start = System.currentTimeMillis();
    for (int i = 0; i < 4; i++)
      TestDataFile.readFile(input, new GenericDatumReader<>(null, projection));
    System.out.println("Time: "+(System.currentTimeMillis()-start));
  }

  private class TestingByteArrayOutputStream extends ByteArrayOutputStream
    implements Syncable {
    private int flushCount = 0;
    private int syncCount = 0;

    @Override
    public void flush() throws IOException {
      super.flush();
      flushCount++;
    }

    @Override
    public void sync() throws IOException {
      syncCount++;
    }
  }
}
