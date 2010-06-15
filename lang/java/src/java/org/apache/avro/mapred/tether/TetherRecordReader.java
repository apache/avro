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

package org.apache.avro.mapred.tether;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.FsInput;

class TetherRecordReader
  implements RecordReader<TetherData, NullWritable> {

  private FsInput in;
  private DataFileReader reader;
  private long start;
  private long end;

  public TetherRecordReader(JobConf job, FileSplit split)
    throws IOException {
    this.in = new FsInput(split.getPath(), job);
    this.reader =
      new DataFileReader<Object>(in, new GenericDatumReader<Object>());

    reader.sync(split.getStart());                    // sync to start
    this.start = in.tell();
    this.end = split.getStart() + split.getLength();

    job.set(AvroJob.INPUT_SCHEMA, reader.getSchema().toString());
  }

  public Schema getSchema() { return reader.getSchema(); }

  public TetherData createKey() { return new TetherData(); }
  
  public NullWritable createValue() { return NullWritable.get(); }
    
  public boolean next(TetherData data, NullWritable ignore)
    throws IOException {
    if (!reader.hasNext() || reader.pastSync(end))
      return false;
    data.buffer(reader.nextBlock());
    data.count((int)reader.getBlockCount());
    return true;
  }
  
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.tell() - start) / (float)(end - start));
    }
  }
  
  public long getPos() throws IOException {
    return in.tell();
  }

  public void close() throws IOException { reader.close(); }
  
}
