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
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroContainerFileBlock;
import org.apache.avro.mapred.AvroContainerFileHeader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Reads file header and decompressed, binary-encoded object blocks from an input split
 * representing a chunk of an Avro container file.
 */
public class AvroEncodedRecordReader
  extends AvroRecordReaderBase<AvroContainerFileHeader, AvroContainerFileBlock, IndexedRecord> {

  private BlockDataFileReader fileReader;
  private AvroContainerFileHeader fileHeader;

  public AvroEncodedRecordReader() {
    super(null);
  }

  /** {@inheritDoc} */
  @Override
  protected DataFileReader<IndexedRecord> createAvroFileReader(
    SeekableInput input, DatumReader<IndexedRecord> datumReader)
    throws IOException {
    return fileReader = new BlockDataFileReader(input, datumReader);
  }

  /** {@inheritDoc} */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
    throws IOException, InterruptedException {
    super.initialize(inputSplit, context);
    fileHeader = new AvroContainerFileHeader(fileReader.getSchema());
  }

  /** {@inheritDoc} */
  @Override
  public AvroContainerFileHeader getCurrentKey() {
    return fileHeader;
  }

  /** {@inheritDoc} */
  @Override
  public AvroContainerFileBlock getCurrentValue() {
    return fileReader.block;
  }

  static class BlockDataFileReader extends DataFileReader<IndexedRecord> {

    AvroContainerFileBlock block = new AvroContainerFileBlock();

    BlockDataFileReader(SeekableInput sin, DatumReader<IndexedRecord> reader)
      throws IOException {
      super(sin, reader);
    }

    @Override
    public IndexedRecord next(IndexedRecord indexedRecord) throws IOException {
      ByteBuffer bb = nextBlock();
      final long count = getBlockCount();
      final int pos = bb.arrayOffset() + bb.position();
      final int len = bb.remaining();
      block.set(count, bb.array(), pos, len);
      blockFinished();
      return null;
    }
  }
}
