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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.easymock.EasyMock;
import org.junit.Test;

public class TestAvroKeyRecordWriter {
  @Test
  public void testWrite() throws IOException {
    Schema writerSchema = Schema.create(Schema.Type.INT);
    CodecFactory compressionCodec = CodecFactory.nullCodec();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    TaskAttemptContext context = createMock(TaskAttemptContext.class);

    replay(context);

    // Write an avro container file with two records: 1 and 2.
    AvroKeyRecordWriter<Integer> recordWriter = new AvroKeyRecordWriter<Integer>(
        writerSchema, compressionCodec, outputStream);
    recordWriter.write(new AvroKey<Integer>(1), NullWritable.get());
    recordWriter.write(new AvroKey<Integer>(2), NullWritable.get());
    recordWriter.close(context);

    verify(context);

    // Verify that the file was written as expected.
    InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    Schema readerSchema = Schema.create(Schema.Type.INT);
    DatumReader<Integer> datumReader = new SpecificDatumReader<Integer>(readerSchema);
    DataFileStream<Integer> dataFileReader = new DataFileStream<Integer>(inputStream, datumReader);

    assertTrue(dataFileReader.hasNext());  // Record 1.
    assertEquals(1, dataFileReader.next().intValue());
    assertTrue(dataFileReader.hasNext());  // Record 2.
    assertEquals(2, dataFileReader.next().intValue());
    assertFalse(dataFileReader.hasNext());  // No more records.

    dataFileReader.close();
  }
}
