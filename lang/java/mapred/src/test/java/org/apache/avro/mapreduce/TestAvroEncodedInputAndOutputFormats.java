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

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.mapred.AvroContainerFileBlock;
import org.apache.avro.mapred.AvroContainerFileHeader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertNotNull;

public class TestAvroEncodedInputAndOutputFormats {

  /** A temporary directory for test data. */
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  /**
   * Verifies that a non-null record reader can be created, and the key/value types are
   * as expected.
   */
  @Test
  public void testCreateRecordReader() throws IOException, InterruptedException {
    // Set up the job configuration.
    Job job = new Job();
    Configuration conf = job.getConfiguration();

    FileSplit inputSplit = createMock(FileSplit.class);
    TaskAttemptContext context = createMock(TaskAttemptContext.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();

    replay(inputSplit);
    replay(context);

    AvroEncodedInputFormat inputFormat = new AvroEncodedInputFormat();
    @SuppressWarnings("unchecked")
    RecordReader<AvroContainerFileHeader, AvroContainerFileBlock> recordReader =
      inputFormat.createRecordReader(inputSplit, context);
    assertNotNull(inputFormat);
    recordReader.close();

    verify(inputSplit);
    verify(context);
  }

  private static class TestMapper extends
    Mapper<AvroContainerFileHeader, AvroContainerFileBlock, AvroKey<TextStats>, NullWritable> {

    private AvroKey<TextStats> mAvroKey = new AvroKey<TextStats>();

    @Override
    protected void map(AvroContainerFileHeader key, AvroContainerFileBlock value, Context context)
      throws IOException, InterruptedException {

      assert TextStats.getClassSchema().equals(key.getWriterSchema());
      assert value.getObjectCount() > 0;

      DatumReader<TextStats> datumReader = new ReflectDatumReader<TextStats>(TextStats.class);
      for (TextStats ts : AvroEncodedInputFormat.stream(datumReader, key, value)) {
        mAvroKey.datum(ts);
        context.write(mAvroKey, NullWritable.get());
      }
    }
  }

  private Job createTestJob(String outputLabel) throws Exception {
    Job job = new Job();
    FileInputFormat.setInputPaths(job, new Path(getClass()
      .getResource("/org/apache/avro/mapreduce/mapreduce-test-input.avro")
      .toURI().toString()));
    job.setInputFormatClass(AvroEncodedInputFormat.class);
    Path outputPath = new Path(tmpFolder.getRoot().getPath() + "/out-" + outputLabel);
    FileOutputFormat.setOutputPath(job, outputPath);
    return job;
  }

  /** Checks that the results from the MapReduce were as expected. */
  private void validateTestJobResults(Job job) throws Exception {
    Path outputPath = FileOutputFormat.getOutputPath(job);
    FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    FileStatus[] outputFiles = fileSystem.globStatus(outputPath.suffix("/part-*"));
    Assert.assertEquals(1, outputFiles.length);

    DataFileReader<TextStats> reader = new DataFileReader<TextStats>(
      new FsInput(outputFiles[0].getPath(), job.getConfiguration()),
      new SpecificDatumReader<TextStats>());
    Map<String, Integer> counts = new HashMap<String, Integer>();
    for (TextStats record : reader) {
      counts.put(record.name.toString(), record.count);
    }
    reader.close();

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());
  }

  /**
   * Verifies that the AvroEncodedInputFormat behaves as expected.
   */
  @Test
  public void testInputFormat() throws Exception {
    Job job = createTestJob("test-avro-encoded-input-format");

    job.setMapperClass(TestMapper.class);
    AvroJob.setMapOutputKeySchema(job, TextStats.getClassSchema());
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(AvroKeyOutputFormat.class);

    Assert.assertTrue(job.waitForCompletion(true));

    validateTestJobResults(job);
  }

  private static class TestReducer
    extends Reducer<AvroContainerFileHeader, AvroContainerFileBlock, BytesWritable, LongWritable> {

    @Override
    protected void reduce(
      AvroContainerFileHeader key, Iterable<AvroContainerFileBlock> values, Context context)
      throws IOException, InterruptedException {

      assert TextStats.getClassSchema().equals(key.getWriterSchema());

      DatumReader<TextStats> datumReader = new ReflectDatumReader<TextStats>(TextStats.class);
      DatumWriter<GenericRecord> datumWriter =
        new GenericDatumWriter<GenericRecord>(TextStats.getClassSchema());
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      BinaryEncoder encoder = null;
      for (TextStats ts : AvroEncodedInputFormat.stream(datumReader, key, values)) {
        outputStream.reset();
        encoder = EncoderFactory.get().directBinaryEncoder(outputStream, encoder);
        datumWriter.write(ts, encoder);
        context.write(new BytesWritable(outputStream.toByteArray()), new LongWritable(1L));
      }
    }
  }

  /**
   * Verifies that the AvroEncodedOutputFormat behaves as expected, assuming that
   * AvroEncodedInputFormat behaves as expected.
   */
  @Test
  public void testOutputFormat() throws Exception {
    Job job = createTestJob("test-avro-encoded-output-format");

    job.setMapOutputKeyClass(AvroContainerFileHeader.class);
    job.setMapOutputValueClass(AvroContainerFileBlock.class);
    job.setReducerClass(TestReducer.class);
    AvroJob.setOutputKeySchema(job, TextStats.getClassSchema());
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(LongWritable.class);
    job.setOutputFormatClass(AvroEncodedOutputFormat.class);

    Assert.assertTrue(job.waitForCompletion(true));

    validateTestJobResults(job);
  }
}
