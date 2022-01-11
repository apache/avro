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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

public class TestAvroKeyOutputFormat {
  private static final String SYNC_INTERVAL_KEY = org.apache.avro.mapred.AvroOutputFormat.SYNC_INTERVAL_KEY;
  private static final int TEST_SYNC_INTERVAL = 12345;

  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();

  @Test
  public void testWithNullCodec() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(SYNC_INTERVAL_KEY, TEST_SYNC_INTERVAL);
    testGetRecordWriter(conf, CodecFactory.nullCodec(), TEST_SYNC_INTERVAL);
  }

  @Test
  public void testWithDeflateCodec() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.output.compress", true);
    conf.setInt(org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY, 3);
    testGetRecordWriter(conf, CodecFactory.deflateCodec(3), DataFileConstants.DEFAULT_SYNC_INTERVAL);
  }

  @Test
  public void testWithSnappyCode() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.output.compress", true);
    conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC);
    conf.setInt(SYNC_INTERVAL_KEY, TEST_SYNC_INTERVAL);
    testGetRecordWriter(conf, CodecFactory.snappyCodec(), TEST_SYNC_INTERVAL);
  }

  @Test
  public void testWithBZip2Code() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.output.compress", true);
    conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.BZIP2_CODEC);
    testGetRecordWriter(conf, CodecFactory.bzip2Codec(), DataFileConstants.DEFAULT_SYNC_INTERVAL);
  }

  @Test
  public void testWithZstandardCode() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.output.compress", true);
    conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.ZSTANDARD_CODEC);
    testGetRecordWriter(conf, CodecFactory.zstandardCodec(3), DataFileConstants.DEFAULT_SYNC_INTERVAL);
  }

  @Test
  public void testWithDeflateCodeWithHadoopConfig() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.output.compress", true);
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.DeflateCodec");
    conf.setInt(org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY, -1);
    conf.setInt(SYNC_INTERVAL_KEY, TEST_SYNC_INTERVAL);
    testGetRecordWriter(conf, CodecFactory.deflateCodec(-1), TEST_SYNC_INTERVAL);
  }

  @Test
  public void testWithSnappyCodeWithHadoopConfig() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.output.compress", true);
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    testGetRecordWriter(conf, CodecFactory.snappyCodec(), DataFileConstants.DEFAULT_SYNC_INTERVAL);
  }

  @Test
  public void testWithBZip2CodeWithHadoopConfig() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.output.compress", true);
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
    conf.setInt(SYNC_INTERVAL_KEY, TEST_SYNC_INTERVAL);
    testGetRecordWriter(conf, CodecFactory.bzip2Codec(), TEST_SYNC_INTERVAL);
  }

  /**
   * Tests that the record writer is constructed and returned correctly from the
   * output format.
   */
  private void testGetRecordWriter(Configuration conf, CodecFactory expectedCodec, int expectedSyncInterval)
      throws IOException {
    // Configure a mock task attempt context.
    Job job = Job.getInstance(conf);
    job.getConfiguration().set("mapred.output.dir", mTempDir.getRoot().getPath());
    Schema writerSchema = Schema.create(Schema.Type.INT);
    AvroJob.setOutputKeySchema(job, writerSchema);
    TaskAttemptContext context = mock(TaskAttemptContext.class);
    when(context.getConfiguration()).thenReturn(job.getConfiguration());
    when(context.getTaskAttemptID()).thenReturn(TaskAttemptID.forName("attempt_200707121733_0001_m_000000_0"));
    when(context.getNumReduceTasks()).thenReturn(1);

    // Create a mock record writer.
    @SuppressWarnings("unchecked")
    RecordWriter<AvroKey<Integer>, NullWritable> expectedRecordWriter = mock(RecordWriter.class);
    AvroKeyOutputFormat.RecordWriterFactory recordWriterFactory = mock(AvroKeyOutputFormat.RecordWriterFactory.class);

    // when the record writer factory to be called with appropriate parameters.
    ArgumentCaptor<CodecFactory> capturedCodecFactory = ArgumentCaptor.forClass(CodecFactory.class);
    when(recordWriterFactory.create(eq(writerSchema), any(GenericData.class), capturedCodecFactory.capture(), // Capture
                                                                                                              // for
                                                                                                              // comparison
                                                                                                              // later.
        any(OutputStream.class), eq(expectedSyncInterval))).thenReturn(expectedRecordWriter);

    AvroKeyOutputFormat<Integer> outputFormat = new AvroKeyOutputFormat<>(recordWriterFactory);
    RecordWriter<AvroKey<Integer>, NullWritable> recordWriter = outputFormat.getRecordWriter(context);
    // Make sure the expected codec was used.
    assertNotNull(capturedCodecFactory.getValue());
    assertEquals(expectedCodec.toString(), capturedCodecFactory.getValue().toString());

    verify(context, atLeastOnce()).getConfiguration();
    verify(recordWriterFactory).create(eq(writerSchema), any(ReflectData.class), any(CodecFactory.class),
        any(OutputStream.class), anyInt());

    assertNotNull(recordWriter);
    assertSame(expectedRecordWriter, recordWriter);
  }
}
