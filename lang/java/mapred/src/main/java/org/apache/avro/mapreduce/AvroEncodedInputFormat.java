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

import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroContainerFileBlock;
import org.apache.avro.mapred.AvroContainerFileHeader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;

/**
 * A MapReduce InputFormat that can handle Avro container files.
 *
 * <p>Keys are AvroContainerFileHeader objects which contain the Avro container file header.
 * Values are AvroContainerFileBlock objects which contain decompressed Avro container file blocks,
 * which consist of a number of binary-encoded objects.</p>
 */
public class AvroEncodedInputFormat
  extends FileInputFormat<AvroContainerFileHeader, AvroContainerFileBlock> {

  /**
   * Utility static method for Hadoop Map/Reduce jobs.
   * <p>
   * Constructs a DataFileStream given an Avro container file header and one block.
   */
  public static <D> DataFileStream<D> stream(
    DatumReader<D> reader, AvroContainerFileHeader key, AvroContainerFileBlock value)
    throws IOException {
    return new DataFileStream<D>(new AvroBlockInputStream(key, value), reader);
  }

  /**
   * Utility static method for Hadoop Map/Reduce jobs.
   * <p>
   * Constructs a DataFileStream given an Avro container file header and some blocks.
   */
  public static <D> DataFileStream<D> stream(
    DatumReader<D> reader, AvroContainerFileHeader key, Iterable<AvroContainerFileBlock> values)
    throws IOException {
    return new DataFileStream<D>(new AvroBlockInputStream(key, values), reader);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordReader<AvroContainerFileHeader, AvroContainerFileBlock> createRecordReader(
    InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    return new AvroEncodedRecordReader();
  }

  static class AvroBlockInputStream extends InputStream {

    private InputStream stream;
    private Iterator<AvroContainerFileBlock> iterator;

    AvroBlockInputStream(AvroContainerFileHeader header, AvroContainerFileBlock value) {
      this.stream = bytesWritableToStream(header.unwrap());
      this.iterator = Collections.singletonList(value).iterator();
    }

    AvroBlockInputStream(AvroContainerFileHeader header, Iterable<AvroContainerFileBlock> values) {
      this.stream = bytesWritableToStream(header.unwrap());
      this.iterator = values.iterator();
    }

    private void update() throws IOException {
      if (stream.available() == 0 && iterator.hasNext()) {
        stream = bytesWritableToStream(iterator.next().unwrap());
      }
    }

    private InputStream bytesWritableToStream(BytesWritable bw) {
      return new ByteArrayInputStream(bw.getBytes(), 0, bw.getLength());
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      update();
      return stream.read(b, off, len);
    }

    @Override
    public int read() throws IOException {
      update();
      return stream.read();
    }

    @Override
    public int available() throws IOException {
      return stream.available();
    }

    @Override
    public long skip(long n) throws IOException {
      return stream.skip(n);
    }

    @Override
    public void close() throws IOException {
      stream.close();
      stream = new ByteArrayInputStream(new byte[0]);
      iterator = Collections.<AvroContainerFileBlock>emptyList().iterator();
      super.close();
    }
  }
}

