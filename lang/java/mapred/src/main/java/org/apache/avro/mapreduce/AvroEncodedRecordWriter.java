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
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Writes binary-encoded Avro records to an Avro container file output stream.
 */
public class AvroEncodedRecordWriter
  extends RecordWriter<BytesWritable, LongWritable>
  implements Syncable {

  private final DataFileWriter fileWriter;

  @SuppressWarnings("unchecked")
  public AvroEncodedRecordWriter(Schema writerSchema,
                                 GenericData dataModel,
                                 CodecFactory compressionCodec,
                                 OutputStream outputStream,
                                 int syncInterval)
    throws IOException {
    fileWriter = new DataFileWriter(dataModel.createDatumWriter(writerSchema));
    fileWriter.setCodec(compressionCodec);
    fileWriter.setSyncInterval(syncInterval);
    fileWriter.create(writerSchema, outputStream);
  }

  /** {@inheritDoc} */
  @Override
  public long sync() throws IOException {
    return fileWriter.sync();
  }

  /** {@inheritDoc} */
  @Override
  public void write(BytesWritable encodedObjects, LongWritable objectsCount) throws IOException {
    long count = objectsCount.get();
    if (count <= 0) {
      throw new IOException("AvroEncodedRecordWriter requires non-negative object count.");
    }
    while (--count > 0) {
      fileWriter.appendEncoded(ByteBuffer.wrap(new byte[0]));
    }
    fileWriter.appendEncoded(
      ByteBuffer.wrap(encodedObjects.getBytes(), 0, encodedObjects.getLength()));
  }

  /** {@inheritDoc} */
  @Override
  public void close(TaskAttemptContext context) throws IOException {
    fileWriter.close();
  }
}
