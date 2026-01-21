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
import org.apache.avro.generic.GenericData;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * FileOutputFormat for writing Avro container files with already-encoded objects.
 *
 * <p>Keys are BytesWritable containing a sequence of binary-encoded objects. Values are
 * LongWritable containing the object count in the key.</p>
 *
 * <p>No validation is performed to verify that the schema used in the encoding matches the writer
 * schema declared in the job configuration.</p>
 */
public class AvroEncodedOutputFormat
  extends AvroOutputFormatBase<BytesWritable, LongWritable> {

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordWriter<BytesWritable, LongWritable> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    GenericData dataModel = AvroSerialization.createDataModel(conf);

    // Get the writer schema.
    Schema writerSchema = AvroJob.getOutputKeySchema(conf);
    boolean isMapOnly = context.getNumReduceTasks() == 0;
    if (isMapOnly) {
      Schema mapOutputSchema = AvroJob.getMapOutputKeySchema(conf);
      if (mapOutputSchema != null) {
        writerSchema = mapOutputSchema;
      }
    }
    if (null == writerSchema) {
      throw new IOException(
        "AvroEncodedOutputFormat requires an output schema. " +
          "Use AvroJob.setOutputKeySchema().");
    }

    return new AvroEncodedRecordWriter(
      writerSchema,
      dataModel,
      getCompressionCodec(context),
      getAvroFileOutputStream(context),
      getSyncInterval(context));
  }
}
