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

package org.apache.avro.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.mapred.AvroContainerFileBlock;
import org.apache.avro.mapred.AvroContainerFileHeader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

public class TestAvroEncodedRecordReader {

  /**
   * A temporary directory for test data.
   */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();

  /**
   * Verifies that avro records can be read and progress is reported correctly.
   */
  @Test
  public void testReadRecords() throws IOException, InterruptedException {

    // Create the test avro file input with three records.
    Schema schema = SchemaBuilder.builder().record("stats").fields()
      .name("counts").type(Schema.create(Schema.Type.INT)).noDefault()
      .name("name").type(Schema.create(Schema.Type.STRING)).noDefault()
      .endRecord();

    GenericRecord firstInputRecord = new GenericRecordBuilder(schema)
      .set("counts", 3)
      .set("name", "apple")
      .build();

    GenericRecord secondInputRecord = new GenericRecordBuilder(schema)
      .set("counts", 2)
      .set("name", "banana")
      .build();

    GenericRecord thirdInputRecord = new GenericRecordBuilder(schema)
      .set("counts", 1)
      .set("name", "carrot")
      .build();

    final SeekableInput avroFileInput = new SeekableFileInput(
      AvroFiles.createFile(
        new File(mTempDir.getRoot(), "myInputFile.avro"),
        schema,
        firstInputRecord,
        secondInputRecord,
        thirdInputRecord));

    // Create the record reader over the avro input file.
    RecordReader<AvroContainerFileHeader, AvroContainerFileBlock> recordReader
      = new AvroEncodedRecordReader() {
      @Override
      protected SeekableInput createSeekableInput(Configuration conf, Path path)
        throws IOException {
        return avroFileInput;
      }
    };

    // Set up the job configuration.
    Configuration conf = new Configuration();

    // Create a mock input split for this record reader.
    FileSplit inputSplit = createMock(FileSplit.class);
    expect(inputSplit.getPath()).andReturn(new Path("/path/to/an/avro/file")).anyTimes();
    expect(inputSplit.getStart()).andReturn(0L).anyTimes();
    expect(inputSplit.getLength()).andReturn(avroFileInput.length()).anyTimes();

    // Create a mock task attempt context for this record reader.
    TaskAttemptContext context = createMock(TaskAttemptContext.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();

    // Initialize the record reader.
    replay(inputSplit);
    replay(context);
    recordReader.initialize(inputSplit, context);

    assertEquals("Progress should be zero before any records are read",
      0.0f, recordReader.getProgress(), 0.0f);

    // Some variables to hold the records.
    AvroContainerFileHeader key;
    AvroContainerFileBlock value;

    // Read the first record.
    assertTrue("Expected at least one record", recordReader.nextKeyValue());
    key = recordReader.getCurrentKey();
    value = recordReader.getCurrentValue();

    assertNotNull("First record had null key", key);
    assertNotNull("First record had null value", value);

    assertEquals(schema, key.getWriterSchema());

    assertEquals(3L, value.getObjectCount());

    ByteBuffer encodedObjects = value.getEncodedObjects();
    assertEquals(23, encodedObjects.remaining());
    assertEquals(0x1422132F, encodedObjects.hashCode());

    assertEquals("Progress should be complete (1 out of 1 records processed)",
      1.0f, recordReader.getProgress(), 0.0f);

    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    InputStream encodedStream = value.getEncodedObjectStream();
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(encodedStream, null);
    GenericRecord record = datumReader.read(null, decoder);
    assertEquals(3, record.get("counts"));
    assertEquals("apple", record.get("name").toString());

    record = datumReader.read(record, decoder);
    assertEquals(2, record.get("counts"));
    assertEquals("banana", record.get("name").toString());

    record = datumReader.read(record, decoder);
    assertEquals(1, record.get("counts"));
    assertEquals("carrot", record.get("name").toString());

    assertEquals(0, encodedStream.available());

    // There should be no more records.
    assertFalse("Expected only 1 record", recordReader.nextKeyValue());

    // Close the record reader.
    recordReader.close();

    // Verify the expected calls on the mocks.
    verify(inputSplit);
    verify(context);
  }
}
