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

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.Syncable;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.RandomData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDataFile {
  private static final Logger LOG = LoggerFactory.getLogger(TestDataFile.class);

  @TempDir
  public File DIR;

  public static Stream<Arguments> codecs() {
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
    r.add(new Object[] { CodecFactory.zstandardCodec(-5) });
    r.add(new Object[] { CodecFactory.zstandardCodec(0, true) });
    r.add(new Object[] { CodecFactory.zstandardCodec(5, false) });
    r.add(new Object[] { CodecFactory.zstandardCodec(18, true) });
    r.add(new Object[] { CodecFactory.zstandardCodec(0, false, false) });
    r.add(new Object[] { CodecFactory.zstandardCodec(0, false, true) });
    return r.stream().map(Arguments::of);
  }

  private static final int COUNT = Integer.parseInt(System.getProperty("test.count", "200"));
  private static final boolean VALIDATE = !"false".equals(System.getProperty("test.validate", "true"));

  private static final long SEED = System.currentTimeMillis();
  private static final String SCHEMA_JSON = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": ["
      + "{\"name\":\"stringField\", \"type\":\"string\"}," + "{\"name\":\"longField\", \"type\":\"long\"}]}";
  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);
  private static final Object LAST_RECORD;
  static {
    Object lastValue = null;
    for (Object object : new RandomData(SCHEMA, COUNT, SEED)) {
      lastValue = object;
    }
    LAST_RECORD = lastValue;
  }

  private File makeFile(CodecFactory codec) {
    return new File(DIR, "test-" + codec + ".avro");
  }

  @ParameterizedTest
  @MethodSource("codecs")
  public void runTestsInOrder(CodecFactory codec) throws Exception {
    // Run for both encoders, but the MethodSource didn't really like it,
    // so it is just a loop within the test
    List<Function<OutputStream, BinaryEncoder>> encoders = new ArrayList<>();
    encoders.add(b -> new EncoderFactory().directBinaryEncoder(b, null));
    encoders.add(b -> new EncoderFactory().blockingDirectBinaryEncoder(b, null));

    for (Function<OutputStream, BinaryEncoder> encoder : encoders) {
      LOG.info("Running with codec: {}", codec);
      testGenericWrite(codec, encoder);
      testGenericRead(codec);
      testSplits(codec);
      testSyncDiscovery(codec);
      testReadLastRecord(codec);
      testGenericAppend(codec, encoder);
      testReadWithHeader(codec);
      testFSync(codec, encoder, false);
      testFSync(codec, encoder, true);
    }
  }

  private void testGenericWrite(CodecFactory codec, Function<OutputStream, BinaryEncoder> encoderFunc)
      throws IOException {
    DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>()).setSyncInterval(100);
    if (codec != null) {
      writer.setCodec(codec);
    }
    writer.setEncoder(encoderFunc);
    writer.create(SCHEMA, makeFile(codec));
    try {
      int count = 0;
      for (Object datum : new RandomData(SCHEMA, COUNT, SEED)) {
        writer.append(datum);
        if (++count % (COUNT / 3) == 0)
          writer.sync(); // force some syncs mid-file
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
            System.out.println("Ignoring: " + e);
          }
          assertTrue(threwProperly, "failed to throw when expected");
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

    assertNull(doubleCloseEx, "Double close() threw an unexpected exception");
  }

  private void testGenericRead(CodecFactory codec) throws IOException {
    try (DataFileReader<Object> reader = new DataFileReader<>(makeFile(codec), new GenericDatumReader<>())) {
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
    }
  }

  private void testSplits(CodecFactory codec) throws IOException {
    File file = makeFile(codec);
    try (DataFileReader<Object> reader = new DataFileReader<>(file, new GenericDatumReader<>())) {
      Random rand = new Random(SEED);
      int splits = 10; // number of splits
      int length = (int) file.length(); // length of file
      int end = length; // end of split
      int remaining = end; // bytes remaining
      int count = 0; // count of entries
      while (remaining > 0) {
        int start = Math.max(0, end - rand.nextInt(2 * length / splits));
        reader.sync(start); // count entries in split
        while (!reader.pastSync(end)) {
          reader.next();
          count++;
        }
        remaining -= end - start;
        end = start;
      }
      assertEquals(COUNT, count);
    }
  }

  private void testSyncDiscovery(CodecFactory codec) throws IOException {
    File file = makeFile(codec);
    try (DataFileReader<Object> reader = new DataFileReader<>(file, new GenericDatumReader<>())) {
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
      assertEquals(reader.previousSync(), (long) syncs.get(0));
      // and confirm that all points are reachable
      for (Long sync : syncs) {
        reader.seek(sync);
        assertNotNull(reader.next());
      }
      // Lastly, confirm that reading (but not decoding) all blocks results in the
      // same sync points
      reader.sync(0);
      ArrayList<Long> syncs2 = new ArrayList<>();
      while (reader.hasNext()) {
        syncs2.add(reader.previousSync());
        reader.nextBlock();
      }
      assertEquals(syncs, syncs2);
    }
  }

  private void testReadLastRecord(CodecFactory codec) throws IOException {
    File file = makeFile(codec);
    try (DataFileReader<Object> reader = new DataFileReader<>(file, new GenericDatumReader<>())) {
      long lastBlockStart = -1;
      while (reader.hasNext()) {
        // This algorithm can be made more efficient by checking if the underlying
        // SeekableFileInput has been fully read: if so, the last block is in
        // memory, and calls to next() will decode it.
        // NOTE: this depends on the current implementation of DataFileReader.
        lastBlockStart = reader.previousSync();
        reader.nextBlock();
      }
      reader.seek(lastBlockStart);

      Object lastRecord = null;
      while (reader.hasNext()) {
        lastRecord = reader.next(lastRecord);
      }
      assertEquals(LAST_RECORD, lastRecord);
    }
  }

  private void testGenericAppend(CodecFactory codec, Function<OutputStream, BinaryEncoder> encoderFunc)
      throws IOException {
    File file = makeFile(codec);
    long start = file.length();
    try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>()).appendTo(file)) {
      writer.setEncoder(encoderFunc);
      for (Object datum : new RandomData(SCHEMA, COUNT, SEED + 1)) {
        writer.append(datum);
      }
    }
    try (DataFileReader<Object> reader = new DataFileReader<>(file, new GenericDatumReader<>())) {
      reader.seek(start);
      Object datum = null;
      if (VALIDATE) {
        for (Object expected : new RandomData(SCHEMA, COUNT, SEED + 1)) {
          datum = reader.next(datum);
          assertEquals(expected, datum);
        }
      } else {
        for (int i = 0; i < COUNT; i++) {
          datum = reader.next(datum);
        }
      }
    }
  }

  private void testReadWithHeader(CodecFactory codec) throws IOException {
    File file = makeFile(codec);
    try (DataFileReader<Object> reader = new DataFileReader<>(file, new GenericDatumReader<>())) {
      // get a header for this file
      DataFileStream.Header header = reader.getHeader();
      // re-open to an arbitrary position near the middle, with sync == true
      SeekableFileInput sin = new SeekableFileInput(file);
      sin.seek(sin.length() / 2);
      try (DataFileReader<Object> readerTrue = DataFileReader.openReader(sin, new GenericDatumReader<>(), header,
          true);) {

        assertNotNull(readerTrue.next(), "Should be able to reopen from arbitrary point");
        long validPos = readerTrue.previousSync();
        // post sync, we know of a valid sync point: re-open with seek (sync == false)
        sin.seek(validPos);
        try (DataFileReader<Object> readerFalse = DataFileReader.openReader(sin, new GenericDatumReader<>(), header,
            false)) {
          assertEquals(validPos, sin.tell(), "Should not move from sync point on reopen");
          assertNotNull(readerFalse.next(), "Should be able to reopen at sync point");
        }
      }
    }
  }

  @Test
  public void syncInHeader() throws IOException {
    try (DataFileReader<Object> reader = new DataFileReader<>(
        new File("target/test-classes/share/test/data/syncInMeta.avro"), new GenericDatumReader<>())) {
      reader.sync(0);
      for (Object datum : reader)
        assertNotNull(datum);
    }
  }

  @Test
  public void test12() throws IOException {
    readFile(new File("target/test-classes/share/test/data/test.avro12"), new GenericDatumReader<>());
  }

  @Test
  public void flushCount() throws IOException {
    DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
    writer.setFlushOnEveryBlock(false);
    TestingByteArrayOutputStream out = new TestingByteArrayOutputStream();
    writer.create(SCHEMA, out);
    int currentCount = 0;
    int flushCounter = 0;
    try {
      for (Object datum : new RandomData(SCHEMA, COUNT, SEED + 1)) {
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
    assertTrue(out.flushCount < currentCount && out.flushCount >= flushCounter);
  }

  private void testFSync(CodecFactory codec, Function<OutputStream, BinaryEncoder> encoderFunc, boolean useFile)
      throws IOException {
    try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.setEncoder(encoderFunc);
      writer.setFlushOnEveryBlock(false);
      TestingByteArrayOutputStream out = new TestingByteArrayOutputStream();
      if (useFile) {
        File f = makeFile(codec);
        try (SeekableFileInput in = new SeekableFileInput(f)) {
          writer.appendTo(in, out);
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
      assertEquals(syncCounter, out.syncCount);
    }
  }

  static void readFile(File f, DatumReader<? extends Object> datumReader) throws IOException {
    try (FileReader<? extends Object> reader = DataFileReader.openReader(f, datumReader)) {
      for (Object datum : reader) {
        assertNotNull(datum);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    File input = new File(args[0]);
    Schema projection = null;
    if (args.length > 1)
      projection = new Schema.Parser().parse(new File(args[1]));
    TestDataFile.readFile(input, new GenericDatumReader<>(null, projection));
    long start = System.currentTimeMillis();
    for (int i = 0; i < 4; i++)
      TestDataFile.readFile(input, new GenericDatumReader<>(null, projection));
    System.out.println("Time: " + (System.currentTimeMillis() - start));
  }

  private static class TestingByteArrayOutputStream extends ByteArrayOutputStream implements Syncable {
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
