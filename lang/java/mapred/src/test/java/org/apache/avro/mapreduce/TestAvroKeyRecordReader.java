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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro.mapreduce;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestAvroKeyRecordReader {
  /** A temporary directory for test data. */
  @TempDir
  public File mTempDir;

  /**
   * Verifies that avro records can be read and progress is reported correctly.
   */
  @Test
  void readRecords() throws IOException, InterruptedException {
    // Create the test avro file input with two records:
    // 1. "first"
    // 2. "second"
    final SeekableInput avroFileInput = new SeekableFileInput(AvroFiles
        .createFile(new File(mTempDir, "myStringfile.avro"), Schema.create(Schema.Type.STRING), "first", "second"));

    // Create the record reader.
    Schema readerSchema = Schema.create(Schema.Type.STRING);
    RecordReader<AvroKey<CharSequence>, NullWritable> recordReader = new AvroKeyRecordReader<CharSequence>(
        readerSchema) {
      @Override
      protected SeekableInput createSeekableInput(Configuration conf, Path path) throws IOException {
        return avroFileInput;
      }
    };

    // Set up the job configuration.
    Configuration conf = new Configuration();

    // Create a mock input split for this record reader.
    FileSplit inputSplit = mock(FileSplit.class);
    when(inputSplit.getPath()).thenReturn(new Path("/path/to/an/avro/file"));
    when(inputSplit.getStart()).thenReturn(0L);
    when(inputSplit.getLength()).thenReturn(avroFileInput.length());

    // Create a mock task attempt context for this record reader.
    TaskAttemptContext context = mock(TaskAttemptContext.class);
    when(context.getConfiguration()).thenReturn(conf);

    // Initialize the record reader.
    recordReader.initialize(inputSplit, context);

    assertEquals(0.0f, recordReader.getProgress(), 0.0f, "Progress should be zero before any records are read");

    // Some variables to hold the records.
    AvroKey<CharSequence> key;
    NullWritable value;

    // Read the first record.
    assertTrue(recordReader.nextKeyValue(), "Expected at least one record");
    key = recordReader.getCurrentKey();
    value = recordReader.getCurrentValue();

    assertNotNull(key, "First record had null key");
    assertNotNull(value, "First record had null value");

    CharSequence firstString = key.datum();
    assertEquals("first", firstString.toString());

    assertEquals(key, recordReader.getCurrentKey());
    assertEquals(value, recordReader.getCurrentValue());

    // Read the second record.
    assertTrue(recordReader.nextKeyValue(), "Expected to read a second record");
    key = recordReader.getCurrentKey();
    value = recordReader.getCurrentValue();

    assertNotNull(key, "Second record had null key");
    assertNotNull(value, "Second record had null value");

    CharSequence secondString = key.datum();
    assertEquals("second", secondString.toString());

    assertEquals(1.0f, recordReader.getProgress(), 0.0f, "Progress should be complete (2 out of 2 records processed)");

    // There should be no more records.
    assertFalse(recordReader.nextKeyValue(), "Expected only 2 records");

    // Close the record reader.
    recordReader.close();

    // Verify the expected calls on the mocks.
    verify(inputSplit).getPath();
    verify(inputSplit, times(2)).getStart();
    verify(inputSplit).getLength();
    verify(context, atLeastOnce()).getConfiguration();
  }
}
