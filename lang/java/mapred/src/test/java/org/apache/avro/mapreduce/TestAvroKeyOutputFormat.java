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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro.mapreduce;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAvroKeyOutputFormat {
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();

  @Test
  public void testWithNullCodec() throws IOException {
    Configuration conf = new Configuration();
    testGetRecordWriter(conf, CodecFactory.nullCodec());
  }

  @Test
  public void testWithDeflateCodec() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.output.compress", true);
    conf.setInt(org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY, 3);
    testGetRecordWriter(conf, CodecFactory.deflateCodec(3));
  }

  @Test
  public void testWithSnappyCode() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.output.compress", true);
    conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC);
    testGetRecordWriter(conf, CodecFactory.snappyCodec());
  }

  /**
   * Tests that the record writer is contructed and returned correclty from the output format.
   */
  private void testGetRecordWriter(Configuration conf, CodecFactory expectedCodec)
      throws IOException {
    // Configure a mock task attempt context.
    Job job = new Job(conf);
    job.getConfiguration().set("mapred.output.dir", mTempDir.getRoot().getPath());
    Schema writerSchema = Schema.create(Schema.Type.INT);
    AvroJob.setOutputKeySchema(job, writerSchema);
    TaskAttemptContext context = createMock(TaskAttemptContext.class);
    expect(context.getConfiguration())
        .andReturn(job.getConfiguration()).anyTimes();
    expect(context.getTaskAttemptID())
        .andReturn(new TaskAttemptID("id", 1, true, 1, 1))
        .anyTimes();

    // Create a mock record writer.
    @SuppressWarnings("unchecked")
    RecordWriter<AvroKey<Integer>, NullWritable> expectedRecordWriter
        = createMock(RecordWriter.class);
    AvroKeyOutputFormat.RecordWriterFactory recordWriterFactory
        = createMock(AvroKeyOutputFormat.RecordWriterFactory.class);

    // Expect the record writer factory to be called with appropriate parameters.
    Capture<CodecFactory> capturedCodecFactory = new Capture<CodecFactory>();
    expect(recordWriterFactory.create(eq(writerSchema),
        capture(capturedCodecFactory),  // Capture for comparison later.
        anyObject(OutputStream.class))).andReturn(expectedRecordWriter);

    replay(context);
    replay(expectedRecordWriter);
    replay(recordWriterFactory);

    AvroKeyOutputFormat<Integer> outputFormat
        = new AvroKeyOutputFormat<Integer>(recordWriterFactory);
    RecordWriter<AvroKey<Integer>, NullWritable> recordWriter
        = outputFormat.getRecordWriter(context);
    // Make sure the expected codec was used.
    assertTrue(capturedCodecFactory.hasCaptured());
    assertEquals(expectedCodec.toString(), capturedCodecFactory.getValue().toString());

    verify(context);
    verify(expectedRecordWriter);
    verify(recordWriterFactory);

    assertNotNull(recordWriter);
    assertTrue(expectedRecordWriter == recordWriter);
  }
}
