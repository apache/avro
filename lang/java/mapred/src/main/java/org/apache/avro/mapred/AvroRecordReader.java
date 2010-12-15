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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.avro.file.FileReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;

/** An {@link RecordReader} for Avro data files. */
public class AvroRecordReader<T>
  implements RecordReader<AvroWrapper<T>, NullWritable> {

  private FileReader<T> reader;
  private long start;
  private long end;

  public AvroRecordReader(JobConf job, FileSplit split)
    throws IOException {
    this(DataFileReader.openReader
         (new FsInput(split.getPath(), job),
          new SpecificDatumReader<T>(AvroJob.getInputSchema(job))),
         split);
  }

  protected AvroRecordReader(FileReader<T> reader, FileSplit split)
    throws IOException {
    this.reader = reader;
    reader.sync(split.getStart());                    // sync to start
    this.start = reader.tell();
    this.end = split.getStart() + split.getLength();
  }

  public AvroWrapper<T> createKey() {
    return new AvroWrapper<T>(null);
  }
  
  public NullWritable createValue() { return NullWritable.get(); }
    
  public boolean next(AvroWrapper<T> wrapper, NullWritable ignore)
    throws IOException {
    if (!reader.hasNext() || reader.pastSync(end))
      return false;
    wrapper.datum(reader.next(wrapper.datum()));
    return true;
  }
  
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getPos() - start) / (float)(end - start));
    }
  }
  
  public long getPos() throws IOException {
    return reader.tell();
  }

  public void close() throws IOException { reader.close(); }
  
}

