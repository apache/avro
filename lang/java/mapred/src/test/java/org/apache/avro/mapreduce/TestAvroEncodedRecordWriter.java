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
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

public class TestAvroEncodedRecordWriter {

  /**
   * A temporary directory for test data.
   */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();

  /**
   * Verifies that AvroEncodedRecordWriter performs as expected.
   */
  @Test
  public void testWrite() throws IOException {
    Schema writerSchema = Schema.create(Schema.Type.INT);
    GenericData dataModel = new ReflectData();
    CodecFactory compressionCodec = CodecFactory.deflateCodec(6);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    TaskAttemptContext context = createMock(TaskAttemptContext.class);

    replay(context);

    // Write an avro container file with two records: 1 and 2.
    AvroEncodedRecordWriter recordWriter = new AvroEncodedRecordWriter(
      writerSchema,
      dataModel,
      compressionCodec,
      outputStream,
      DataFileConstants.DEFAULT_SYNC_INTERVAL);

    BytesWritable encodedObjects = new BytesWritable(new byte[2]);
    encodedObjects.getBytes()[0] = 2;
    encodedObjects.getBytes()[1] = 4;
    LongWritable objectsCount = new LongWritable(2L);
    recordWriter.write(encodedObjects, objectsCount);
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

  /**
   * Verifies that AvroEncodedRecordWriter performs as expected with syncable outputs..
   */
  @Test
  public void testSyncableWrite() throws IOException {
    Schema writerSchema = Schema.create(Schema.Type.INT);
    GenericData dataModel = new ReflectData();
    CodecFactory compressionCodec = CodecFactory.deflateCodec(6);
    FileOutputStream outputStream =
      new FileOutputStream(new File(mTempDir.getRoot(), "temp.avro"));
    TaskAttemptContext context = createMock(TaskAttemptContext.class);

    replay(context);

    // Write an avro container file with two records: 1 and 2.
    AvroEncodedRecordWriter recordWriter = new AvroEncodedRecordWriter(
      writerSchema,
      dataModel,
      compressionCodec,
      outputStream,
      DataFileConstants.DEFAULT_SYNC_INTERVAL);

    BytesWritable encodedObjects = new BytesWritable(new byte[1]);
    LongWritable objectsCount = new LongWritable(1L);
    long positionOne = recordWriter.sync();
    encodedObjects.getBytes()[0] = 2;
    recordWriter.write(encodedObjects, objectsCount);
    long positionTwo = recordWriter.sync();
    encodedObjects.getBytes()[0] = 4;
    recordWriter.write(encodedObjects, objectsCount);
    recordWriter.close(context);

    verify(context);

    // Verify that the file was written as expected.
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    Path avroFile = new Path(new File(mTempDir.getRoot(), "temp.avro").toString());
    DataFileReader<Integer> dataFileReader =
      new DataFileReader<Integer>(new FsInput(avroFile, conf), new SpecificDatumReader<Integer>());

    dataFileReader.seek(positionTwo);
    assertTrue(dataFileReader.hasNext());  // Record 2.
    assertEquals(2, (Object) dataFileReader.next());

    dataFileReader.seek(positionOne);
    assertTrue(dataFileReader.hasNext());  // Record 1.
    assertEquals(1, (Object) dataFileReader.next());

    dataFileReader.close();
  }
}
