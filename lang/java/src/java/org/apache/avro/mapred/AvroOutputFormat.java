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

package org.apache.avro.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.CodecFactory;

/** An {@link org.apache.hadoop.mapred.OutputFormat} for Avro data files. */
public class AvroOutputFormat <T>
  extends FileOutputFormat<AvroWrapper<T>, NullWritable> {

  final static String EXT = ".avro";

  private static final String DEFLATE_LEVEL_KEY = "avro.mapred.deflate.level";
  private static final int DEFAULT_DEFLATE_LEVEL = 1;

  /** Enable output compression using the deflate codec and specify its level.*/
  public static void setDeflateLevel(JobConf job, int level) {
    FileOutputFormat.setCompressOutput(job, true);
    job.setInt(DEFLATE_LEVEL_KEY, level);
  }

  public RecordWriter<AvroWrapper<T>, NullWritable>
    getRecordWriter(FileSystem ignore, JobConf job,
                    String name, Progressable prog)
    throws IOException {

    Schema schema = Schema.parse(job.get(AvroJob.OUTPUT_SCHEMA));

    DatumWriter<T> datumWriter =
      AvroJob.API_SPECIFIC.equals(job.get(AvroJob.OUTPUT_API))
      ? new SpecificDatumWriter<T>()
      : new GenericDatumWriter<T>();

    final DataFileWriter<T> writer = new DataFileWriter<T>(datumWriter);

    if (FileOutputFormat.getCompressOutput(job)) {
      int level = job.getInt(DEFLATE_LEVEL_KEY, DEFAULT_DEFLATE_LEVEL);
      writer.setCodec(CodecFactory.deflateCodec(level));
    }

    Path path = FileOutputFormat.getTaskOutputPath(job, name+EXT);
    writer.create(schema, path.getFileSystem(job).create(path));

    return new RecordWriter<AvroWrapper<T>, NullWritable>() {
        public void write(AvroWrapper<T> wrapper, NullWritable ignore)
          throws IOException {
          writer.append(wrapper.datum());
        }
        public void close(Reporter reporter) throws IOException {
          writer.close();
        }
      };
  }

}
